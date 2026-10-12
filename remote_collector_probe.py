"""Manual Linux compatibility proof, never a production scheduler/exporter."""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from urllib.request import urlopen

from capture_core import call, capture

IMAGE = 'ghcr.io/flaresolverr/flaresolverr@sha256:c80ae007ce2ccdcd217a12426e4f039ef763ff90738c808d38810c3e59323767'  # v3.5.2, immutable


def validate_nick(value):
    if not re.fullmatch(r'[A-Za-z0-9_]{1,32}', value):
        raise ValueError('Usá un nick de Minecraft, sin enlaces ni espacios.')
    return value


def summarize(entry):
    # Explicit projection: never print raw HTML, cookies, counters, URL or errors.
    observation = entry.get('observation') or {}
    return {
        'valid_profile': entry.get('valid_profile') is True,
        'skin_metadata_valid': bool(observation.get('appearance')),
        'player_not_found': entry.get('player_not_found') is True,
        'duration_seconds': entry.get('duration_seconds'),
    }


def docker(*args, timeout=90):
    # No shell expansion, no private environment forwarded into the container.
    return subprocess.run(['docker', *args], check=True, timeout=timeout,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_ready(deadline):
    while time.monotonic() < deadline:
        try:
            with urlopen('http://127.0.0.1:8193/health', timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(2)
    raise TimeoutError('solver_startup')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nick', required=True, type=validate_nick)
    args = parser.parse_args(argv)
    if sys.platform != 'linux':
        parser.error('Esta prueba está preparada para Linux con Docker.')
    container = 'log-probe-' + uuid.uuid4().hex[:12]
    session = None
    stage = 'solver_startup'
    started = time.monotonic()
    result = {'mode': 'manual_compatibility_probe', 'published': False}
    try:
        docker('run', '--detach', '--name', container, '--pull=always',
               '--memory=2g', '--cpus=2', '--pids-limit=512', '--log-driver=none',
               '-p', '127.0.0.1:8193:8191', '-e', 'LOG_LEVEL=warning',
               '-e', 'LOG_HTML=false', IMAGE)
        wait_ready(time.monotonic() + 60)
        stage = 'session_creation'
        created = call({'cmd': 'sessions.create'})
        if created.get('status') != 'ok' or not created.get('session'):
            raise RuntimeError('session_creation')
        session = created['session']
        stage = 'profile_capture'
        result.update(summarize(capture(args.nick, session)))  # Exactly one attempt.
        result['status'] = 'success' if result['valid_profile'] else 'unavailable'
    except Exception:
        result.update(status='failed', failed_stage=stage)
    finally:
        if session:
            try:
                call({'cmd': 'sessions.destroy', 'session': session})
            except Exception:
                result['session_cleanup_failed'] = True
        try:
            docker('rm', '--force', container, timeout=15)
        except Exception:
            result['container_cleanup_failed'] = True
        result['elapsed_seconds'] = round(time.monotonic() - started, 2)
        encoded = json.dumps(result, ensure_ascii=False)
        print(encoded, flush=True)
        summary = os.environ.get('GITHUB_STEP_SUMMARY')
        if summary:
            with open(summary, 'a', encoding='utf-8') as output:
                output.write('## Prueba de compatibilidad LOG\n\n```json\n' + encoded + '\n```\n')
    return 0 if result.get('status') == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
