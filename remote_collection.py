"""Bounded collector. Code is public; roster, ledger and rounds stay private."""
import base64
import argparse
import copy
from datetime import datetime, timezone
import json
import os
import re
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

from capture_core import call, capture
from remote_collector_probe import docker, wait_ready, IMAGE
from remote_configuration import load as load_administration

INTERVAL = 3 * 3600
MAX_PLAYERS = 150
COLLECTION_SECONDS = 18 * 60
MAX_FILE_BYTES = 8_000_000
MAX_EARLY_WAIT = 10 * 60
PROFILE_PAUSE_SECONDS = 3
DIAGNOSTICS_VERSION = 1


class StorageError(Exception):
    pass


def failure_diagnostic(error, stage):
    """Fixed codes only. Never serialize exceptions, commands, URLs or provider text."""
    component = ('github-storage' if stage == 'private_storage' else
                 'solver' if stage in ('solver_startup', 'session_creation', 'collection') else
                 'collector')
    category = 'unexpected-error'
    if isinstance(error, StorageError):
        component = 'github-storage'
        code = str(error)
        if re.fullmatch(r'storage_http_[1-5][0-9]{2}', code):
            status = int(code[-3:])
            return {'category': 'github-http-error', 'component': component, 'http_status': status}
        category = ('github-unavailable' if code == 'storage_unavailable' else
                    'storage-validation-failed')
    elif isinstance(error, HTTPError):
        category = 'http-error'
        return {'category': category, 'component': component, 'http_status': error.code}
    elif isinstance(error, subprocess.TimeoutExpired):
        category = 'docker-command-timeout'
        component = 'runner-docker'
    elif isinstance(error, subprocess.CalledProcessError):
        return {'category': 'docker-command-failed', 'component': 'runner-docker',
                'exit_code': error.returncode}
    elif isinstance(error, FileNotFoundError) and stage == 'solver_startup':
        category = 'docker-not-found'
        component = 'runner-docker'
    elif isinstance(error, URLError):
        reason = error.reason
        category = ('dns-error' if isinstance(reason, socket.gaierror) else
                    'transport-timeout' if isinstance(reason, (TimeoutError, socket.timeout)) else
                    'connection-refused' if isinstance(reason, ConnectionRefusedError) else
                    'connection-reset' if isinstance(reason, ConnectionResetError) else
                    'transport-unavailable')
    elif isinstance(error, (TimeoutError, socket.timeout)):
        category = 'transport-timeout'
    elif isinstance(error, ConnectionRefusedError):
        category = 'connection-refused'
    elif isinstance(error, ConnectionResetError):
        category = 'connection-reset'
    elif isinstance(error, json.JSONDecodeError):
        category = 'invalid-json'
    elif isinstance(error, ValueError):
        category = 'configuration-or-validation-failed'
    return {'category': category, 'component': component}


def capture_diagnostic(entry, attempt, seconds):
    """Keep useful HTTP/browser evidence, excluding titles and raw error messages."""
    diagnostic = {'category': 'ok', 'component': 'capture',
                  'duration_seconds': round(max(0, seconds), 2)}
    status = entry.get('reported_page_status')
    if type(status) is int and 100 <= status <= 599:
        diagnostic['page_http_status'] = status
    solver = entry.get('solver_status')
    if solver in ('ok', 'error'):
        diagnostic['solver_status'] = solver
    if attempt['valid_profile']:
        return diagnostic
    if attempt.get('cause') == 'profile-not-found':
        diagnostic.update(category='profile-not-found', component='source-profile')
        return diagnostic
    # Chromium's stable error identifiers are evidence; arbitrary text is discarded.
    message = entry.get('message')
    message = message[:4096] if isinstance(message, str) else ''
    browser_codes = ('ERR_NAME_NOT_RESOLVED', 'ERR_CONNECTION_TIMED_OUT',
                     'ERR_CONNECTION_RESET', 'ERR_CONNECTION_CLOSED', 'ERR_CONNECTION_REFUSED',
                     'ERR_INTERNET_DISCONNECTED', 'ERR_PROXY_CONNECTION_FAILED',
                     'ERR_CERT_AUTHORITY_INVALID', 'ERR_CERT_DATE_INVALID', 'ERR_HTTP_RESPONSE_CODE_FAILURE')
    code = next((code for code in browser_codes if re.search(r'net::' + code + r'\b', message)), None)
    if code:
        diagnostic.update(category='browser-network-error', component='source-or-runner-network',
                          browser_error=code)
    elif type(status) is int and status in (403, 429):
        diagnostic.update(category='source-access-denied' if status == 403 else 'source-rate-limited',
                          component='source-http')
    elif type(status) is int and 500 <= status <= 599:
        diagnostic.update(category='source-server-error', component='source-http')
    elif isinstance(entry.get('title'), str) and entry['title'].strip().casefold() in (
            'just a moment...', 'just a moment', 'un momento...', 'attention required! | cloudflare'):
        diagnostic.update(category='challenge-page', component='source-access')
    elif re.search(r'\b(?:timeout|timed out)\b', message, re.IGNORECASE):
        diagnostic.update(category='solver-page-timeout', component='source-or-browser')
    elif solver == 'error':
        diagnostic.update(category='solver-error', component='source-or-browser')
    elif type(status) is int and status != 200:
        diagnostic.update(category='source-http-error', component='source-http')
    else:
        diagnostic.update(category='profile-validation-failed', component='source-or-parser')
    return diagnostic


def capture_counts(attempts):
    counts = {}
    for attempt in attempts:
        if attempt['valid_profile']:
            continue
        category = attempt['diagnostic']['category']
        counts[category] = counts.get(category, 0) + 1
    return counts


def solver_health():
    """One loopback-only diagnostic, never a retry or another source request."""
    try:
        with urlopen('http://127.0.0.1:8193/health', timeout=2) as response:
            return 'ok' if response.status == 200 else 'http-error'
    except HTTPError:
        return 'http-error'
    except (TimeoutError, socket.timeout):
        return 'timeout'
    except URLError as error:
        return 'timeout' if isinstance(error.reason, (TimeoutError, socket.timeout)) else 'unreachable'
    except OSError:
        return 'unreachable'
    except Exception:
        return 'unknown'


def container_health(container):
    """Read only Docker state fields; never collect container logs or environment."""
    try:
        result = subprocess.run(['docker', 'inspect', '--format', '{{json .State}}', container],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                timeout=3, check=True, text=True)
        if len(result.stdout) > 8192:
            return {'available': False}
        value = json.loads(result.stdout)
        if (type(value.get('Running')) is not bool or type(value.get('OOMKilled')) is not bool
                or type(value.get('ExitCode')) is not int):
            return {'available': False}
        return {'available': True, 'running': value['Running'], 'oom_killed': value['OOMKilled'],
                'exit_code': value['ExitCode']}
    except Exception:
        return {'available': False}


def diagnostic_summary(output, summary):
    """Explanations are local fixed text, with no provider-controlled Markdown."""
    output.write('\n### Diagnóstico del recolector\n\n')
    status = summary.get('status')
    if status == 'partial':
        output.write('Ronda parcial: el workflow puede terminar verde aunque falten perfiles. '
                     'Las capturas fallidas no se convierten en ceros.\n\n')
    elif status == 'source-unavailable':
        output.write('Se activó el freno de tres fallos consecutivos. '
                     'No hubo reintentos ni recuperación de cortes anteriores.\n\n')
    labels = {
        'profile-not-found': 'La fuente devolvió un perfil no encontrado; revisar esa cuenta.',
        'browser-network-error': 'Chromium informó un error de red; el código exacto está en los logs de diagnóstico.',
        'source-access-denied': 'La página de origen respondió HTTP 403: acceso denegado.',
        'source-rate-limited': 'La página de origen respondió HTTP 429: límite de solicitudes.',
        'source-server-error': 'La página de origen respondió HTTP 5xx: error del servicio de origen.',
        'source-http-error': 'La página de origen respondió un estado HTTP no válido para capturar.',
        'challenge-page': 'Se reconoció una página de verificación de acceso, no un perfil.',
        'solver-page-timeout': 'El navegador agotó el tiempo al obtener la página; puede ser la fuente, el acceso o el navegador.',
        'solver-error': 'El solver informó un error sin evidencia suficiente para atribuir la causa.',
        'profile-validation-failed': 'No se obtuvo un perfil válido: revisar contenido de origen o compatibilidad del parser.',
        'transport-timeout': 'Una comunicación excedió el tiempo; revisar el componente indicado.',
        'connection-refused': 'El componente indicado rechazó la conexión.',
        'connection-reset': 'Se interrumpió la conexión con el componente indicado.',
        'transport-unavailable': 'La comunicación con el componente indicado no estuvo disponible.',
        'dns-error': 'Falló la resolución de nombre en la comunicación indicada.',
        'invalid-json': 'La respuesta no fue JSON válido para el componente indicado.',
        'unexpected-error': 'Error no clasificado; no se imprime el texto externo por privacidad.',
    }
    counts = summary.get('failure_counts') or {}
    if counts:
        output.write('| Código | Cantidad | Interpretación |\n| --- | ---: | --- |\n')
        for code, count in sorted(counts.items()):
            output.write(f'| `{code}` | {count} | {labels.get(code, labels["unexpected-error"])} |\n')
        output.write('\n')
    if 'solver_health' in summary:
        health = summary['solver_health']
        output.write('**Salud local del solver:** ' + ('respondió HTTP 200' if health == 'ok' else
                     'sin respuesta saludable; revisar Docker y el runner') + '.\n\n')
    container = summary.get('container_health') or {}
    if container.get('oom_killed'):
        output.write('**Docker informó OOM:** el contenedor fue terminado por falta de memoria. '
                     'Es evidencia de recursos del runner, no de la PC del administrador.\n\n')
    elif container.get('available') and not container['running']:
        output.write('**El contenedor estaba detenido.** Revisar su código de salida en el JSON.\n\n')
    output.write('Los códigos separan almacenamiento GitHub, Docker, comunicación con el '
                 'solver, respuesta HTTP de la fuente y validación de perfil. '
                 'Un timeout por sí solo no demuestra quién causó el fallo. '
                 'La salud del solver comprueba únicamente el servicio local.\n')


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def slot_at(now):
    return int(now.timestamp()) // INTERVAL * INTERVAL


def wait_for_slot(now=lambda: datetime.now(timezone.utc), clock=time.monotonic, pause=time.sleep):
    """One bounded wait, no profiles/storage requests and no collection retry."""
    current = now()
    target = slot_at(current) + INTERVAL
    remaining = target - current.timestamp()
    if remaining > MAX_EARLY_WAIT:
        return current, 0
    begun = clock()
    deadline = begun + MAX_EARLY_WAIT
    requested_sleep = 0
    # Also bound iterations if a broken clock or sleeper fails to advance.
    for _ in range(MAX_EARLY_WAIT // 30 + 2):
        current = now()
        remaining = target - current.timestamp()
        if remaining <= 0:
            if current.timestamp() >= target + INTERVAL:
                raise ValueError('clock_jump_during_wait')
            return current, round(clock() - begun, 2)
        budget = min(deadline - clock(), MAX_EARLY_WAIT - requested_sleep)
        if budget <= 0:
            raise ValueError('early_wait_limit')
        seconds = min(30, remaining, budget)
        requested_sleep += seconds
        pause(seconds)
    raise ValueError('early_wait_limit')


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


def tracking_start(policy):
    if policy is None:
        return None
    if (not isinstance(policy, dict) or set(policy) != {'schema_version', 'mode', 'starts_at'}
            or policy['schema_version'] != 1 or policy['mode'] != 'beta'
            or not isinstance(policy['starts_at'], str)
            or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T03:00:00Z', policy['starts_at'])):
        raise ValueError('invalid_tracking_policy')
    try:
        return datetime.fromisoformat(policy['starts_at'].replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('invalid_tracking_policy') from None


def collection_purpose(policy, now):
    start = tracking_start(policy)
    return 'beta_collection' if start is not None and now >= start else 'research_only'


def project_attempt(nick, entry):
    """Private stored projection. No HTML, headers, cookies or exceptions."""
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
    # TAG stays in the private round only; public exports keep their allowlist.
    if row.get('player_tag_checked') is True:
        safe['player_tag_checked'] = True
        tag = row.get('player_tag')
        if (isinstance(tag, dict)
                and tag.get('method') == 'public-profile-player-tag-v1'
                and tag.get('observed_at') == row['observed_at']
                and (tag.get('value') is None
                     or isinstance(tag.get('value'), str)
                     and 0 < len(tag['value'].strip()) <= 64)):
            safe['player_tag'] = {k: tag[k] for k in ('value', 'observed_at', 'method')}
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

    def read_bytes(self, path):
        payload = self.request('GET', '/contents/' + path + '?ref=' + quote(self.branch, safe=''), missing=True)
        if payload is None:
            self.shas[path] = None
            return None
        if payload.get('type') != 'file' or payload.get('size', 0) > MAX_FILE_BYTES:
            raise StorageError('storage_file_invalid')
        self.shas[path] = payload['sha']
        if payload.get('encoding') != 'base64':
            if not re.fullmatch(r'[a-f0-9]{40}', payload['sha']):
                raise StorageError('storage_file_invalid')
            payload = self.request('GET', '/git/blobs/' + payload['sha'])
            if payload.get('encoding') != 'base64' or payload.get('size', 0) > MAX_FILE_BYTES:
                raise StorageError('storage_file_invalid')
        try:
            result = base64.b64decode(payload['content'])
            if len(result) > MAX_FILE_BYTES:
                raise ValueError()
            return result
        except Exception:
            raise StorageError('storage_file_invalid') from None

    def read(self, path, default=None):
        content = self.read_bytes(path)
        if content is None:
            return copy.deepcopy(default)
        try:
            return json.loads(content)
        except Exception:
            raise StorageError('storage_json_invalid') from None

    def write(self, path, value):
        if path not in self.shas:
            self.read(path)
        content = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
        if len(content) > MAX_FILE_BYTES:
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


def claim_tag_verification(store, now):
    """One extra TAG rollout check, reserved durably without moving regular cuts."""
    state = store.read('state.json', {'schema_version': 1, 'last_slot': None, 'latest_round': None})
    last = state.get('last_slot')
    if (state.get('schema_version') != 1
            or last is not None and (type(last) is not int or last % INTERVAL)):
        raise StorageError('invalid_state')
    if state.get('tag_verification_v1') is not None:
        return None, state
    timestamp = int(now.timestamp())
    # A regular cut keeps its own filename; never overwrite that round.
    if timestamp % INTERVAL == 0 or last is not None and last > timestamp:
        raise StorageError('manual_check_conflicts_with_cut')
    state.update(tag_verification_v1=now.isoformat(timespec='seconds'),
                 last_status='started', started_at=now.isoformat(timespec='seconds'))
    store.write('state.json', state)
    return timestamp, state


def collect(names, capture_one, checkpoint, deadline, clock=time.monotonic, pause=time.sleep,
            purpose='research_only', tracking_started_at=None, progress=None):
    if purpose not in ('research_only', 'beta_collection'):
        raise ValueError('invalid_collection_purpose')
    if purpose == 'beta_collection':
        tracking_start({'schema_version': 1, 'mode': 'beta', 'starts_at': tracking_started_at})
    report = {'schema_version': 1, 'purpose': purpose, 'status': 'collecting',
              'started_at': utc_now(), 'roster': names, 'attempts': []}
    if purpose == 'beta_collection':
        if datetime.fromisoformat(report['started_at'].replace('Z', '+00:00')) < tracking_start({'schema_version': 1, 'mode': 'beta', 'starts_at': tracking_started_at}):
            raise ValueError('collection_predates_tracking_start')
        report['tracking_started_at'] = tracking_started_at
    consecutive = 0
    for nick in names:
        if clock() + 65 > deadline:
            report['status'] = 'time-limit'
            break
        begun = clock()
        try:
            raw = capture_one(nick)
            attempt = project_attempt(nick, raw)
            diagnostic = capture_diagnostic(raw, attempt, clock() - begun)
        except Exception as error:
            attempt = {'requested_nick': nick, 'valid_profile': False, 'cause': 'capture-failed'}
            diagnostic = {**failure_diagnostic(error, 'collection'),
                          'duration_seconds': round(max(0, clock() - begun), 2)}
        attempt['diagnostic'] = diagnostic
        report['attempts'].append(attempt)
        if progress and (not attempt['valid_profile'] or len(report['attempts']) % 10 == 0):
            progress({'attempted': len(report['attempts']),
                      'valid_profiles': sum(a['valid_profile'] for a in report['attempts']),
                      'capture': diagnostic})
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
            pause(PROFILE_PAUSE_SECONDS if attempt['valid_profile'] else 5)
    if report['status'] == 'collecting':
        report['status'] = 'complete' if all(a['valid_profile'] for a in report['attempts']) else 'partial'
    report['finished_at'] = utc_now()
    report['diagnostics'] = {'version': DIAGNOSTICS_VERSION,
                             'failure_counts': capture_counts(report['attempts'])}
    checkpoint(report)
    return report


def main():
    parser = argparse.ArgumentParser()
    timing = parser.add_mutually_exclusive_group()
    timing.add_argument('--wait-for-slot', action='store_true')
    timing.add_argument('--verify-tags-once', action='store_true')
    args = parser.parse_args()
    if sys.platform != 'linux':
        raise SystemExit('Linux required')
    store = None
    state = None
    slot = None
    session = None
    container = 'log-round-' + uuid.uuid4().hex[:12]
    summary = {'mode': 'remote-private-validation', 'published': False,
               'diagnostics_version': DIAGNOSTICS_VERSION}
    if re.fullmatch(r'[a-f0-9]{40}', os.environ.get('GITHUB_SHA', '')):
        summary['code_revision'] = os.environ['GITHUB_SHA']
    if re.fullmatch(r'[0-9]{1,20}', os.environ.get('GITHUB_RUN_ID', '')):
        summary['run_id'] = os.environ['GITHUB_RUN_ID']
    if args.verify_tags_once:
        summary['manual_check'] = 'tag-verification-v1'
    stage = 'slot_timing'
    started = time.monotonic()
    stage_started = started
    timings = {}
    solver_attempted = False

    def emit(value):
        print(json.dumps({'mode': 'collector-diagnostic', 'diagnostics_version': DIAGNOSTICS_VERSION,
                          'stage': stage, **value}, ensure_ascii=False), flush=True)

    def enter_stage(value):
        nonlocal stage, stage_started
        current = time.monotonic()
        timings[stage] = round(timings.get(stage, 0) + current - stage_started, 2)
        stage, stage_started = value, current
        emit({'event': 'stage_started', 'elapsed_seconds': round(current - started, 2)})

    try:
        enter_stage('slot_timing')
        now = datetime.now(timezone.utc)
        if args.wait_for_slot:
            remaining = slot_at(now) + INTERVAL - now.timestamp()
            if remaining <= MAX_EARLY_WAIT:
                print(json.dumps({'status': 'waiting_for_slot', 'seconds_until_slot': round(remaining, 2)}), flush=True)
            now, waited = wait_for_slot()
            summary['waited_seconds'] = waited
        # Waiting never consumes the capture budget or claims the previous cut.
        collection_started = time.monotonic()
        enter_stage('private_storage')
        store = PrivateStore(os.environ.get('DATA_REPOSITORY', ''), os.environ.get('DATA_TOKEN', ''))
        administration = load_administration(store)
        roster = administration['roster'] if administration else store.read('roster.json')
        if administration and not roster['nicks']:
            summary.update(status='skipped', reason='no_active_accounts')
            return 0
        names = roster_names(roster)
        tracking = store.read('tracking-policy.json')
        tracking_start(tracking)
        slot, state = claim_tag_verification(store, now) if args.verify_tags_once else claim(store, now)
        if slot is None:
            summary.update(status='skipped', reason='tag_verification_already_attempted' if args.verify_tags_once else 'slot_already_attempted')
            return 0
        enter_stage('solver_startup')
        solver_attempted = True
        docker('run', '--detach', '--name', container, '--pull=always',
               '--memory=2g', '--cpus=2', '--pids-limit=512', '--log-driver=none',
               '-p', '127.0.0.1:8193:8191', '-e', 'LOG_LEVEL=warning', '-e', 'LOG_HTML=false', IMAGE)
        wait_ready(time.monotonic() + 60)
        enter_stage('session_creation')
        created = call({'cmd': 'sessions.create'})
        if created.get('status') != 'ok' or not created.get('session'):
            raise RuntimeError('session_creation')
        session = created['session']
        enter_stage('collection')
        round_id = datetime.fromtimestamp(slot, timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        path = 'rounds/' + round_id + '.json'
        def checkpoint(report):
            payload = copy.deepcopy(report)
            payload['round_id'] = round_id
            payload['finished_at'] = utc_now()
            store.write(path, payload)
        purpose = collection_purpose(tracking, datetime.now(timezone.utc))
        report = collect(names, lambda n: capture(n, session), checkpoint,
                         collection_started + COLLECTION_SECONDS, purpose=purpose,
                         tracking_started_at=tracking['starts_at'] if purpose == 'beta_collection' else None,
                         progress=emit)
        valid = sum(a['valid_profile'] for a in report['attempts'])
        skins = sum(bool(a.get('observation', {}).get('appearance')) for a in report['attempts'])
        state.update(last_status=report['status'], finished_at=report['finished_at'],
                     last_diagnostics=report['diagnostics'])
        if valid:
            state['latest_round'] = path
        store.write('state.json', state)
        summary.update(status=report['status'], total=len(names), attempted=len(report['attempts']),
                       valid_profiles=valid, valid_skins=skins, pending=len(names)-len(report['attempts']),
                       failure_counts=report['diagnostics']['failure_counts'])
    except Exception as error:
        summary.update(status='failed', failed_stage=stage,
                       failure=failure_diagnostic(error, stage))
        if stage == 'slot_timing' and str(error) in ('early_wait_limit', 'clock_jump_during_wait'):
            summary['failure_reason'] = str(error)
        if store and state and slot is not None:
            try:
                state.update(last_status='failed', finished_at=utc_now(),
                             last_diagnostics={'version': DIAGNOSTICS_VERSION,
                                               'failure': summary['failure'], 'failed_stage': stage})
                store.write('state.json', state)
            except Exception:
                summary['state_finalize_failed'] = True
    finally:
        if solver_attempted and summary.get('status') not in ('complete', 'skipped'):
            enter_stage('local_diagnostics')
            summary['solver_health'] = solver_health()
            summary['container_health'] = container_health(container)
        enter_stage('cleanup')
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
        timings[stage] = round(time.monotonic() - stage_started, 2)
        summary['stage_seconds'] = timings
        text = json.dumps(summary, ensure_ascii=False)
        print(text, flush=True)
        result_path = os.environ.get('COLLECTION_RESULT_FILE')
        if result_path:
            from pathlib import Path
            Path(result_path).parent.mkdir(parents=True, exist_ok=True)
            Path(result_path).write_text(text, encoding='utf-8')
        summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a', encoding='utf-8') as output:
                output.write('## Ronda LOG · datos privados\n\n```json\n' + text + '\n```\n')
                diagnostic_summary(output, summary)
    return 0 if summary.get('status') in ('complete', 'skipped') or (summary.get('status') == 'partial' and summary.get('pending') == 0 and summary.get('valid_profiles', 0) > 0) else 1


if __name__ == '__main__':
    raise SystemExit(main())
