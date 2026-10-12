"""Bounded collector. Code is public; roster, ledger and rounds stay private."""
import base64
import copy
from datetime import datetime, timezone
import json
import os
import re
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

from capture_core import call, capture
from remote_collector_probe import docker, wait_ready, IMAGE

INTERVAL = 3 * 3600
MAX_PLAYERS = 150
COLLECTION_SECONDS = 18 * 60


class StorageError(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def slot_at(now):
    return int(now.timestamp()) // INTERVAL * INTERVAL


def roster_names(value):
    if not isinstance(value, dict) or set(value) != {'schema_version', 'nicks'} or value['schema_version'] != 1:
        raise ValueError('invalid_roster')
    names = value['nicks']
    if not isinstance(names, list) or not 1 <= len(names) <= MAX_PLAYERS:
        raise ValueError('invalid_roster')
    if any(not isinstance(n, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,32}', n) for n in names):
        raise ValueError('invalid_roster')
    if len({n.casefold() for n in names}) != len(names):
        raise ValueError('duplicate_nick')
    return names


def project_attempt(nick, entry):
    """Explicit stored projection. No HTML, headers, cookies, TAG or exceptions."""
    result = {'requested_nick': nick, 'valid_profile': False}
    row = entry.get('observation') or {}
    if not entry.get('valid_profile'):
        result['cause'] = 'profile-not-found' if entry.get('player_not_found') else 'capture-failed'
        return result
    expected = 'https://stats.universocraft.com/jugador/' + nick
    keys = ('wins', 'kills', 'deaths')
    if (str(row.get('nick', '')).casefold() != nick.casefold()
            or str(row.get('profile_url', '')).casefold() != expected.casefold()
            or row.get('modality') != 'skywars'
            or any(type(row.get(k)) is not int or not 0 <= row[k] <= 2**53 - 1 for k in keys)):
        result['cause'] = 'invalid-observation'
        return result
    try:
        at = datetime.fromisoformat(row['observed_at'].replace('Z', '+00:00'))
        if at.tzinfo is None:
            raise ValueError()
    except (ValueError, KeyError, TypeError):
        result['cause'] = 'invalid-observation'
        return result
    safe = {k: row[k] for k in keys}
    safe.update(nick=row['nick'], modality='skywars', profile_url=expected,
                observed_at=row['observed_at'])
    skin = row.get('appearance') or {}
    texture = skin.get('texture_url', '')
    if (isinstance(texture, str) and re.fullmatch(r'https://textures\.minecraft\.net/texture/[a-f0-9]{32,64}', texture)
            and skin.get('observed_at') == row['observed_at']
            and str(skin.get('profile_url', '')).casefold() == expected.casefold()
            and skin.get('server_id') == 'universocraft'):
        safe['appearance'] = {'server_id': 'universocraft', 'profile_url': expected,
                              'texture_url': texture, 'observed_at': row['observed_at'],
                              'method': 'public-profile-metadata'}
    result.update(valid_profile=True, observation=safe)
    return result


class PrivateStore:
    def __init__(self, repository, token):
        if not re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+', repository) or not token:
            raise StorageError('storage_configuration')
        self.base = 'https://api.github.com/repos/' + repository
        self.token = token
        metadata = self.request('GET', '')
        if metadata.get('private') is not True:
            raise StorageError('data_repository_must_be_private')
        self.branch = metadata['default_branch']
        # An initialized repository is required; no silent branch/repo creation.
        self.request('GET', '/git/ref/heads/' + quote(self.branch, safe=''))
        self.shas = {}

    def request(self, method, path, payload=None, missing=False):
        request = Request(self.base + path,
                          data=json.dumps(payload).encode() if payload is not None else None,
                          method=method,
                          headers={'Authorization': 'Bearer ' + self.token,
                                   'Accept': 'application/vnd.github+json',
                                   'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'LOG-Collector',
                                   'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            if missing and error.code == 404:
                return None
            # Do not print request/response or tokens. No retry on conflicts.
            raise StorageError('storage_http_' + str(error.code)) from None
        except Exception:
            raise StorageError('storage_unavailable') from None

    def read(self, path, default=None):
        payload = self.request('GET', '/contents/' + path + '?ref=' + quote(self.branch, safe=''), missing=True)
        if payload is None:
            self.shas[path] = None
            return copy.deepcopy(default)
        if payload.get('type') != 'file' or payload.get('size', 0) > 900_000 or payload.get('encoding') != 'base64':
            raise StorageError('storage_file_invalid')
        self.shas[path] = payload['sha']
        try:
            return json.loads(base64.b64decode(payload['content']))
        except Exception:
            raise StorageError('storage_file_invalid') from None

    def write(self, path, value):
        if path not in self.shas:
            self.read(path)
        content = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
        if len(content) > 900_000:
            raise StorageError('storage_file_limit')
        payload = {'message': 'LOG collector checkpoint', 'branch': self.branch,
                   'content': base64.b64encode(content).decode()}
        if self.shas[path]:
            payload['sha'] = self.shas[path]
        result = self.request('PUT', '/contents/' + path, payload)
        self.shas[path] = result['content']['sha']


def claim(store, now):
    state = store.read('state.json', {'schema_version': 1, 'last_slot': None, 'latest_round': None})
    slot = slot_at(now)
    if state.get('schema_version') != 1:
        raise StorageError('invalid_state')
    last = state.get('last_slot')
    if last is not None and (type(last) is not int or last % INTERVAL):
        raise StorageError('invalid_state')
    if last is not None and last >= slot:
        return None, state
    state.update(last_slot=slot, last_status='started', started_at=now.isoformat(timespec='seconds'))
    # Durable claim BEFORE Docker or any profile request. A failure cannot retry this slot.
    store.write('state.json', state)
    return slot, state


def collect(names, capture_one, checkpoint, deadline, clock=time.monotonic, pause=time.sleep):
    report = {'schema_version': 1, 'purpose': 'research_only', 'status': 'collecting',
              'started_at': utc_now(), 'roster': names, 'attempts': []}
    consecutive = 0
    for nick in names:
        if clock() + 65 > deadline:
            report['status'] = 'time-limit'
            break
        try:
            raw = capture_one(nick)
            attempt = project_attempt(nick, raw)
        except Exception:
            attempt = {'requested_nick': nick, 'valid_profile': False, 'cause': 'capture-failed'}
        report['attempts'].append(attempt)
        if attempt['valid_profile'] or attempt.get('cause') == 'profile-not-found':
            consecutive = 0
        else:
            consecutive += 1
        if len(report['attempts']) % 10 == 0:
            checkpoint(report)
        if consecutive >= 3:
            report['status'] = 'source-unavailable'
            break
        if len(report['attempts']) < len(names):
            pause(5)
    if report['status'] == 'collecting':
        report['status'] = 'complete' if all(a['valid_profile'] for a in report['attempts']) else 'partial'
    report['finished_at'] = utc_now()
    checkpoint(report)
    return report


def main():
    if sys.platform != 'linux':
        raise SystemExit('Linux required')
    store = None
    state = None
    slot = None
    session = None
    container = 'log-round-' + uuid.uuid4().hex[:12]
    summary = {'mode': 'remote-private-validation', 'published': False}
    stage = 'private_storage'
    started = time.monotonic()
    try:
        store = PrivateStore(os.environ.get('DATA_REPOSITORY', ''), os.environ.get('DATA_TOKEN', ''))
        names = roster_names(store.read('roster.json'))
        slot, state = claim(store, datetime.now(timezone.utc))
        if slot is None:
            summary.update(status='skipped', reason='slot_already_attempted')
            return 0
        stage = 'solver_startup'
        docker('run', '--detach', '--name', container, '--pull=always',
               '--memory=2g', '--cpus=2', '--pids-limit=512', '--log-driver=none',
               '-p', '127.0.0.1:8193:8191', '-e', 'LOG_LEVEL=warning', '-e', 'LOG_HTML=false', IMAGE)
        wait_ready(time.monotonic() + 60)
        stage = 'session_creation'
        created = call({'cmd': 'sessions.create'})
        if created.get('status') != 'ok' or not created.get('session'):
            raise RuntimeError('session_creation')
        session = created['session']
        stage = 'collection'
        round_id = datetime.fromtimestamp(slot, timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        path = 'rounds/' + round_id + '.json'
        def checkpoint(report):
            payload = copy.deepcopy(report)
            payload['round_id'] = round_id
            payload['finished_at'] = utc_now()
            store.write(path, payload)
        report = collect(names, lambda n: capture(n, session), checkpoint,
                         started + COLLECTION_SECONDS)
        valid = sum(a['valid_profile'] for a in report['attempts'])
        skins = sum(bool(a.get('observation', {}).get('appearance')) for a in report['attempts'])
        state.update(last_status=report['status'], finished_at=report['finished_at'])
        if valid:
            state['latest_round'] = path
        store.write('state.json', state)
        summary.update(status=report['status'], total=len(names), attempted=len(report['attempts']),
                       valid_profiles=valid, valid_skins=skins, pending=len(names)-len(report['attempts']))
    except Exception:
        summary.update(status='failed', failed_stage=stage)
        if store and state and slot is not None:
            try:
                state.update(last_status='failed', finished_at=utc_now())
                store.write('state.json', state)
            except Exception:
                summary['state_finalize_failed'] = True
    finally:
        if session:
            try:
                call({'cmd': 'sessions.destroy', 'session': session})
            except Exception:
                summary['session_cleanup_failed'] = True
        if slot is not None:
            try:
                docker('rm', '--force', container, timeout=15)
            except Exception:
                summary['container_cleanup_failed'] = True
        summary['elapsed_seconds'] = round(time.monotonic() - started, 2)
        text = json.dumps(summary, ensure_ascii=False)
        print(text, flush=True)
        summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a', encoding='utf-8') as output:
                output.write('## Ronda LOG · datos privados\n\n```json\n' + text + '\n```\n')
    return 0 if summary.get('status') == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
