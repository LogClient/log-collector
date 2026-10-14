"""Private stored round -> canonical export -> bounded static Pages deployment."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.request import Request, urlopen
import zipfile
from remote_collection import PrivateStore, StorageError
from remote_configuration import load as load_administration

PROJECT = 'comunidad-log'
MAX_FILES = 150
MAX_STATIC_BYTES = 25_000_000
RUN = Path('.run')
PUBLIC_PAGES = {'index.html', '404.html', 'activity.html', 'compare.html', 'draw.html',
                'events.html', 'maestrias.html', 'player.html', 'players.html',
                'privacy.html', 'ranking.html', 'records.html'}
PUBLIC_DATA = {'data/config.js', 'data/live-preview.js', 'data/events.json',
               'data/simulation-draws.json', 'data/tournament-history.json'}
ENGINE_FILES = frozenset({
    'scripts/remote_public_export.cjs', 'scripts/lib/observation-pipeline.cjs',
    'scripts/lib/mastery-state.cjs', 'scripts/lib/mastery-experiment.js',
    'scripts/lib/skywars-score.js', 'scripts/lib/periods.js',
    'scripts/lib/records-state.cjs', 'scripts/lib/ranking-state.cjs',
    'web/js/core/ranking.js',
})


def unpack_engine(blob, output):
    """Trusted private code, isolated from both checkout and static deployment."""
    if not isinstance(blob, bytes) or not 0 < len(blob) <= 1_000_000:
        raise ValueError('private_engine_missing_or_size')
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != ENGINE_FILES:
            raise ValueError('private_engine_inventory')
        if sum(item.file_size for item in archive.infolist()) > 2_000_000:
            raise ValueError('private_engine_size')
        # Validate every entry before writing anything. No fallback to public code.
        for item in archive.infolist():
            if (item.is_dir() or item.flag_bits & 1
                    or (item.external_attr >> 16) & 0o170000 == 0o120000):
                raise ValueError('private_engine_entry')
        for name in names:
            target = output / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))
    return output / 'scripts/remote_public_export.cjs'


def static_name(name):
    return (name in PUBLIC_PAGES or name == '_headers' or name in PUBLIC_DATA
            or bool(re.fullmatch(r'assets/[\w-]+\.(?:ttf|woff2?|png|webp|svg|txt)', name))
            or bool(re.fullmatch(r'css/[\w-]+\.css', name))
            or bool(re.fullmatch(r'js/(?:components|core|pages|site)/[\w-]+\.js', name))
            or name in {'vendor/skinview3d/LICENSE', 'vendor/skinview3d/skinview3d.bundle.js'})


def check_config(config):
    if (config.get('schema_version') != 1 or config.get('project') != PROJECT
            or not re.fullmatch(r'[a-f0-9]{64}', config.get('template_sha256', ''))
            or config.get('allow_paid_services') is not False or config.get('allow_functions') is not False
            or not isinstance(config.get('files'), dict) or not 1 <= len(config['files']) <= MAX_FILES):
        raise ValueError('publication_configuration')
    for name, digest in config['files'].items():
        if (not static_name(name)
                or '..' in name or name.startswith('functions/') or '_worker' in name
                or not re.fullmatch(r'[a-f0-9]{64}', digest)):
            raise ValueError('forbidden_static_file')
    for required in ['index.html', '404.html', '_headers', 'data/live-preview.js', 'privacy.html']:
        if required not in config['files']:
            raise ValueError('missing_static_file')


def unpack(blob, config, output):
    check_config(config)
    if not isinstance(blob, bytes) or hashlib.sha256(blob).hexdigest() != config['template_sha256']:
        raise ValueError('template_integrity')
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != set(config['files']):
            raise ValueError('template_inventory')
        if sum(item.file_size for item in archive.infolist()) > MAX_STATIC_BYTES:
            raise ValueError('template_size')
        for item in archive.infolist():
            if item.is_dir() or (item.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('template_symlink')
            content = archive.read(item)
            if hashlib.sha256(content).hexdigest() != config['files'][item.filename]:
                raise ValueError('template_file_integrity')
            target = output / item.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)


def cloudflare_project(account, token):
    if not re.fullmatch(r'[a-f0-9]{32}', account) or not token:
        raise ValueError('cloudflare_credentials')
    request = Request('https://api.cloudflare.com/client/v4/accounts/' + account + '/pages/projects/' + PROJECT,
                      headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    result = payload.get('result') or {}
    if payload.get('success') is not True or result.get('name') != PROJECT or result.get('production_branch') != 'main':
        raise ValueError('cloudflare_project_mismatch')
    if result.get('source'):
        raise ValueError('expected_direct_upload_project')
    for environment in result.get('deployment_configs', {}).values():
        for key in ['kv_namespaces', 'durable_object_namespaces', 'd1_databases', 'r2_buckets', 'services', 'queues', 'hyperdrive', 'ai', 'browser', 'vectorize']:
            if environment.get(key):
                raise ValueError('billable_binding_detected')
    return result


def execute(command, timeout, env=None, cwd=None):
    return subprocess.run(command, check=True, timeout=timeout, env=env, cwd=cwd,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def reserve_publication(store, key, retry):
    ledger = store.read('publication-control.json', {'schema_version': 1})
    if ledger.get('schema_version') != 1:
        raise ValueError('publication_ledger_invalid')
    if ledger.get('published_key') == key:
        return None
    attempts = ledger.get('attempts', 0) if ledger.get('attempted_key') == key else 0
    if attempts and (not retry or attempts >= 2):
        raise ValueError('publication_already_attempted')
    ledger.update(attempted_key=key, attempts=attempts+1, status='started')
    store.write('publication-control.json', ledger)
    return ledger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--retry-publication', action='store_true')
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    summary = {'mode': 'private-history-static-export', 'published': False}
    stage = 'private_storage'
    started = time.monotonic()
    store = ledger = None
    try:
        if os.environ.get('ZERO_COST_REVIEWED') != 'true' or os.environ.get('CLOUDFLARE_FREE_REVIEWED') != 'true':
            raise ValueError('cost_review_required')
        store = PrivateStore(os.environ.get('DATA_REPOSITORY', ''), os.environ.get('DATA_TOKEN', ''))
        administration = load_administration(store)
        control = store.read('state.json')
        round_path = control.get('latest_round', '')
        if not re.fullmatch(r'rounds/\d{8}T\d{6}Z\.json', round_path):
            raise ValueError('no_complete_remote_round')
        round_data = store.read(round_path)
        config = store.read('publication-config.json')
        check_config(config)
        stage = 'private_template'
        RUN.mkdir(exist_ok=True)
        # Unique directory: no stale worker/functions file can survive a prior run.
        output = RUN / ('site-' + str(time.time_ns()))
        unpack(store.read_bytes('site-template.zip'), config, output)
        stage = 'private_engine'
        engine = unpack_engine(store.read_bytes('private-engine.zip'),
                               RUN / ('engine-' + str(time.time_ns())))
        engine_state = store.read('engine-state.json')
        if engine_state is None:
            engine_state = store.read('bootstrap-state.json')
        input_data = {'state': engine_state, 'round': round_data, 'roster': administration['roster'] if administration else store.read('roster.json'),
                      'members': administration['members'] if administration else store.read('publication-members.json'), 'modalities': config['modalities'],
                      'tracking': store.read('tracking-policy.json')}
        input_path = RUN / 'engine-input.json'
        output_state = RUN / 'engine-state.json'
        input_path.write_text(json.dumps(input_data), encoding='utf-8')
        stage = 'canonical_export'
        clean_env = dict(os.environ)
        for name in ['DATA_TOKEN', 'CLOUDFLARE_API_TOKEN']:
            clean_env.pop(name, None)
        execute(['node', str(engine), str(input_path), str(output_state), str(output/'data/live-preview.js')], 60, clean_env)
        new_state = json.loads(output_state.read_text(encoding='utf-8'))
        # Do not expose failed source updates as if the whole site was refreshed.
        accepted = len(new_state.get('last_round', {}).get('accepted', []))
        if accepted == 0:
            raise ValueError('no_accepted_captures')
        stage = 'private_state_save'
        if engine_state.get('scope') == 'preview-native' and new_state.get('scope') == 'beta-native':
            # Preserve the remote state as it existed immediately before official tracking.
            backup = 'engine-before-beta-' + new_state['records']['started_at'][:10].replace('-', '') + '.json'
            if store.read(backup) is None:
                store.write(backup, engine_state)
        store.write('engine-state.json', new_state)
        if administration:
            for name, value in [('data/events.json', administration['events']), ('data/simulation-draws.json', administration['draws'])]:
                if name not in config['files']:
                    raise ValueError('missing_administrative_static_file')
                (output/name).write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
        actual_files = {p.relative_to(output).as_posix() for p in output.rglob('*') if p.is_file()}
        if actual_files != set(config['files']) or any(p.is_symlink() for p in output.rglob('*')):
            raise ValueError('unexpected_output_file')
        if any((output/name).stat().st_size > 25*1024*1024 for name in actual_files):
            raise ValueError('asset_size_limit')
        # Keep the visible beta notice accurate. This only changes copy after validation.
        for name in actual_files:
            if name.endswith('.html'):
                page = (output/name).read_text(encoding='utf-8')
                page = page.replace('Beta · datos reales. La actualización automática todavía está en preparación.',
                                    'Beta · datos reales. Consultá la fecha de la última revisión.')
                (output/name).write_text(page, encoding='utf-8')
        summary.update(status='prepared', accepted_captures=accepted,
                       tracking_scope=new_state.get('scope'), records_started_at=new_state.get('records', {}).get('started_at'),
                       confirmed_records=len(new_state.get('records', {}).get('records', {})))
        if args.prepare_only:
            summary['status'] = 'prepared_without_deployment'
            return 0
        stage = 'cloudflare_preflight'
        account, token = os.environ.get('CLOUDFLARE_ACCOUNT_ID', ''), os.environ.get('CLOUDFLARE_API_TOKEN', '')
        cloudflare_project(account, token)
        key = round_data['round_id'] + ':' + config['template_sha256'] + (':' + administration['revision'] if administration else '')
        stage = 'publication_reservation'
        ledger = reserve_publication(store, key, args.retry_publication)
        if ledger is None:
            summary['status'] = 'already_published'
            return 0
        stage = 'static_deployment'
        deploy_env = dict(clean_env)
        deploy_env.update(CLOUDFLARE_API_TOKEN=token, CLOUDFLARE_ACCOUNT_ID=account,
                          WRANGLER_SEND_METRICS='false', CI='true',
                          WRANGLER_LOG_PATH=str((RUN/'wrangler-logs').resolve()))
        # Start CLI in an empty directory, not the code repo where extra files might exist.
        deploy_cwd = RUN / ('deploy-' + str(time.time_ns()))
        deploy_cwd.mkdir()
        cli = Path('deploy-tool/node_modules/wrangler/bin/wrangler.js').resolve()
        execute(['node', str(cli), 'pages', 'deploy', str(output.resolve()), '--project-name', PROJECT,
                 '--branch', 'main', '--commit-dirty=true'], 300, deploy_env, deploy_cwd)
        stage = 'publication_finalize'
        ledger.update(status='published', published_key=key, published_round=round_data['round_id'],
                      admin_revision=administration['revision'] if administration else None,
                      published_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
        store.write('publication-control.json', ledger)
        summary.update(status='success', published=True)
    except Exception:
        summary.update(status='failed', failed_stage=stage)
        if stage in ('static_deployment', 'publication_finalize'):
            summary['deployment_outcome_requires_check'] = True
        if ledger and store:
            try:
                ledger['status'] = 'failed'
                store.write('publication-control.json', ledger)
            except Exception:
                summary['ledger_finalize_failed'] = True
    finally:
        summary['elapsed_seconds'] = round(time.monotonic()-started, 2)
        text = json.dumps(summary, ensure_ascii=False)
        print(text, flush=True)
        target = os.environ.get('GITHUB_STEP_SUMMARY')
        if target:
            with open(target, 'a', encoding='utf-8') as out:
                out.write('## Publicación estática LOG\n\n```json\n'+text+'\n```\n')
    return 0 if summary.get('status') in ('success', 'already_published') else 1


if __name__ == '__main__':
    raise SystemExit(main())
