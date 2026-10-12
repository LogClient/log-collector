"""One atomic, private administrative projection. Never contains credentials."""
import json
import re

FILE = 'admin-publication.json'
MAX_BYTES = 2_000_000
FORBIDDEN = {'member_id', 'account_id', 'discord_id', 'discord_ids', 'discord_accounts',
             'age', 'age_declared_on', 'country', 'notes', 'note', 'reason', 'actor_id',
             'password_hash', 'token', 'interview', 'incidents', 'source_file', 'joined_on', 'left_on'}


def inspect(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in FORBIDDEN:
                raise ValueError('private_administrative_field')
            inspect(child)
    elif isinstance(value, list):
        for child in value:
            inspect(child)
    elif isinstance(value, str) and re.search(r'github_pat_|ghp_|mfa\.[\w-]{20,}|^[A-Za-z]:[\\/]|https?://(?:localhost|127\.0\.0\.1)', value, re.I):
        raise ValueError('private_administrative_value')


def validate(value):
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'revision', 'roster', 'members', 'events', 'draws'}
            or value['schema_version'] != 1 or not re.fullmatch(r'[a-f0-9]{64}', value.get('revision', ''))):
        raise ValueError('invalid_administrative_projection')
    roster, members = value['roster'], value['members']
    if (not isinstance(roster, dict) or set(roster) != {'schema_version', 'nicks'} or roster['schema_version'] != 1
            or not isinstance(members, dict) or set(members) != {'schema_version', 'public_nicks'} or members['schema_version'] != 1):
        raise ValueError('invalid_administrative_membership')
    names, public = roster['nicks'], members['public_nicks']
    for collection in (names, public):
        if (not isinstance(collection, list) or len(collection) > 150
                or any(not isinstance(n, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,32}', n) for n in collection)
                or len({n.lower() for n in collection}) != len(collection)):
            raise ValueError('invalid_administrative_nicks')
    if not {n.lower() for n in public}.issubset({n.lower() for n in names}):
        raise ValueError('invalid_administrative_principals')
    if (not isinstance(value['events'], dict) or set(value['events']) != {'events'} or not isinstance(value['events']['events'], list)
            or not isinstance(value['draws'], dict) or set(value['draws']) != {'simulations'} or not isinstance(value['draws']['simulations'], list)
            or len(value['events']['events']) > 100 or len(value['draws']['simulations']) > 100):
        raise ValueError('invalid_administrative_events')
    if any(d.get('mode') != 'official' or d.get('fictional') is not False for d in value['draws']['simulations']):
        raise ValueError('simulation_not_publishable')
    inspect(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_BYTES:
        raise ValueError('administrative_projection_size')
    return value


def load(store):
    value = store.read(FILE)
    return None if value is None else validate(value)
