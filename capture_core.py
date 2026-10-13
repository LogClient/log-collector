# Generated from the same parser/validator used by the local collector.
import json
import re
import time
import unicodedata
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import unquote, urlparse, parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError
API = 'http://127.0.0.1:8193/v1'
SOURCES = ('top_kills', 'top_wins', 'manual')

def profile_nick(url):
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "stats.universocraft.com" or parsed.username or parsed.password or parsed.port not in (None, 443):
        return None
    match = re.fullmatch(r"/jugador/([A-Za-z0-9_]{1,32})/?", unquote(parsed.path))
    return match.group(1) if match else None

def integer_counter(text):
    if not isinstance(text, str):
        return None
    value = text.strip().replace("\u00a0", " ").replace("\u202f", " ")
    if re.fullmatch(r"\d+", value, flags=re.ASCII):
        result = int(value)
    elif re.fullmatch(r"\d{1,3}(,\d{3})+", value, flags=re.ASCII):
        result = int(value.replace(",", ""))
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", value, flags=re.ASCII):
        result = int(value.replace(".", ""))
    elif re.fullmatch(r"\d{1,3}( \d{3})+", value, flags=re.ASCII):
        result = int(value.replace(" ", ""))
    else:
        return None
    return result if result <= 2**53 - 1 else None

def normalize_label(text):
    text = unicodedata.normalize("NFKD", text or "")
    return " ".join("".join(char for char in text if not unicodedata.combining(char)).upper().split())

def skin_texture(image):
    """Read the public render metadata; never request its wrapper URL."""
    if not isinstance(image, str):
        return None
    try:
        wrapper = urlparse(image)
        candidates = parse_qs(wrapper.query).get('url', [])
        value = candidates[0] if len(candidates) == 1 else image
        texture = urlparse(value)
        if (texture.scheme not in ('http', 'https') or texture.hostname != 'textures.minecraft.net'
                or texture.port is not None or texture.username or texture.password
                or texture.query or texture.fragment
                or not re.fullmatch(r'/texture/[a-f0-9]{32,64}', texture.path)):
            return None
        return 'https://textures.minecraft.net' + texture.path
    except ValueError:
        return None

def observation(payload, expected_url, source):
    expected = profile_nick(expected_url)
    if not expected or not payload.get("ready"):
        raise ValueError("Esperando que cargue el perfil y SkyWars")
    nick = payload.get("nick", "")
    if (profile_nick(payload.get("url", "")) or "").casefold() != expected.casefold() or nick.casefold() != expected.casefold():
        raise ValueError("El perfil está cambiando; esperando identidad consistente")
    if source not in SOURCES:
        raise ValueError("Origen de muestra desconocido")
    raw = {}
    for item in payload.get("stats", []):
        label = normalize_label(item.get("label"))
        if not label:
            continue
        if label in raw and raw[label] != item.get("value"):
            raise ValueError("SkyWars contiene contadores contradictorios")
        raw[label] = item.get("value")
    counters = {key: integer_counter(raw.get(label)) for key, label in
                (("wins", "VICTORIAS"), ("kills", "ASESINATOS"), ("deaths", "MUERTES"))}
    if any(value is None for value in counters.values()):
        raise ValueError("Faltan contadores válidos de SkyWars; no se guardó como cero")
    row = {"nick": nick, "modality": "skywars", **counters,
            "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "profile_url": f"https://stats.universocraft.com/jugador/{nick}",
            "selection_metric": source, "selection_rank": None, "raw_skywars": raw}
    if 'player_tag' in payload:
        row['player_tag_checked'] = True
    values = payload.get('player_tag')
    if (isinstance(values, list) and len(values) <= 16
            and all(isinstance(v, str) and 0 < len(v.strip()) <= 64 for v in values)):
        # UniversoCraft also renders cosmetic symbols as ProfilePlayerTag nodes.
        # They are not a membership TAG; keep ambiguity between actual text TAGs.
        tags = [v.strip() for v in values
                if any(char.isalnum() for char in unicodedata.normalize('NFKC', v))]
        if len(tags) <= 1:
            row['player_tag'] = {'value': tags[0] if tags else None,
                                 'observed_at': row['observed_at'],
                                 'method': 'public-profile-player-tag-v1'}
    texture = skin_texture(payload.get('skin_image'))
    if texture:
        row['appearance'] = {'server_id': 'universocraft', 'profile_url': row['profile_url'],
                             'texture_url': texture, 'observed_at': row['observed_at'],
                             'method': 'public-profile-metadata'}
    return row

class ProfileParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.nick = ''
        self.title = ''
        self.rows = []
        self.row = None
        self.skin_image = None
        self.profile_count = 0
        self.player_tags = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'meta' and attrs.get('property') == 'og:image':
            self.skin_image = attrs.get('content')
        classes = attrs.get('class', '').split()
        inherited = self.stack[-1] if self.stack else {}
        state = dict(inherited)
        state.update(tag=tag)
        if 'ProfileName' in classes:
            state['profile'] = True
            self.profile_count += 1
        if 'ProfilePlayerTag' in classes and state.get('profile'):
            state['player_tag_index'] = len(self.player_tags)
            self.player_tags.append('')
        if tag == 'h1' and state.get('profile'):
            state['nick'] = True
        if tag == 'title':
            state['title'] = True
        if attrs.get('game'):
            state['skywars'] = attrs['game'] == 'skywars'
        if state.get('skywars') and 'GameStat' in classes:
            self.row = {'label': '', 'value': ''}
            state['row_root'] = True
        else:
            state.pop('row_root', None)
        if 'GameStatName' in classes:
            state['field'] = 'label'
        if 'GameStatValue' in classes:
            state['field'] = 'value'
        if tag not in ('area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'):
            self.stack.append(state)

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index]['tag'] == tag:
                closed = self.stack[index:]
                if any(state.get('row_root') for state in closed) and self.row is not None:
                    self.rows.append({key: value.strip() for key, value in self.row.items()})
                    self.row = None
                del self.stack[index:]
                break

    def handle_data(self, data):
        state = self.stack[-1] if self.stack else {}
        if 'player_tag_index' in state:
            self.player_tags[state['player_tag_index']] += data
        if state.get('nick'):
            self.nick += data
        if state.get('title'):
            self.title += data
        if state.get('skywars') and state.get('field') and self.row is not None:
            self.row[state['field']] += data

def call(payload):
    request = Request(API, data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
    try:
        response = urlopen(request, timeout=55)
    except HTTPError as error:
        response = error
    with response:
        return json.load(response)

def capture(nick, session):
    url = f'https://stats.universocraft.com/jugador/{nick}'
    start = time.monotonic()
    result = call({'cmd': 'request.get', 'url': url, 'session': session, 'maxTimeout': 45000})
    entry = {'requested_nick': nick, 'duration_seconds': round(time.monotonic() - start, 2),
             'solver_status': result.get('status'), 'message': result.get('message')}
    solution = result.get('solution') or {}
    entry['reported_page_status'] = solution.get('status')
    html = solution.get('response') or ''
    entry['player_not_found'] = '¡No se ha encontrado el jugador buscado!' in html
    parser = ProfileParser()
    parser.feed(html)
    entry['title'] = parser.title.strip()
    try:
        if result.get('status') != 'ok' or solution.get('status') != 200:
            raise ValueError('FlareSolverr did not return a successful page')
        payload = {'url': solution.get('url'), 'ready': bool(parser.rows),
                   'nick': parser.nick.strip(), 'stats': parser.rows,
                   'skin_image': parser.skin_image, 'player_tag': [v.strip() for v in parser.player_tags] if parser.profile_count == 1 else None}
        entry['observation'] = observation(payload, url, 'manual')
        entry['valid_profile'] = True
    except ValueError as error:
        entry.update(valid_profile=False, validation_error=str(error))
    return entry
