import __main__

import datetime
import fcntl
import itertools
import re
from gzip import compress, decompress
from hashlib import sha256
from json import dumps as json_dumps, loads as json_loads
from math import atan2, cos, pow, radians, sin, sqrt
from pathlib import Path

import requests
from humanize import naturaltime
from jinja2 import Environment, FileSystemLoader
from lxml import etree
from retrying import retry

from .config import ENABLE_CACHE, ENABLE_OVERPASS_CACHE, PROXIES


BASE_DIR = Path(__main__.__file__).parent
BASE_NAME = Path(__main__.__file__).stem

CACHE_DIR = BASE_DIR / "cache"

DAYS = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
PT_ARTICLES = {"e", "a", "à", "o", "de", "do", "da", "dos", "das"}

POSTCODE_CITIES = {
    "linda a velha": "linda-a-velha",
}


class DiffDict:
    def __init__(self, data=None):
        if data is None:
            self.data = {"tags": {}}
            self.kind = "new"
        else:
            self.data = data
            self.kind = "old"
        self.old_tags = {}

    def diff(self):
        return [[self.lat, self.lon], {key: [old_value, self[key]] for key, old_value in self.old_tags.items()}]

    def revert(self, key):
        if key in self.old_tags:
            self[key] = self.old_tags[key]
            self.old_tags.pop(key)
            if not self[key]:
                self.data["tags"].pop(key)
            if not self.old_tags and self.kind == "mod":
                self.kind = "old"

    @property
    def lat(self):
        return self.data.get("center", {}).get("lat", self.data.get("lat"))

    @property
    def lon(self):
        return self.data.get("center", {}).get("lon", self.data.get("lon"))

    def __getitem__(self, key):
        return self.data["tags"].get(key) or ""

    def __setitem__(self, key, value):
        if self[key] == value:
            return
        if not key in self.old_tags:
            self.old_tags[key] = self[key]
        self.data["tags"][key] = value
        if self.kind == "old":
            self.kind = "mod"

    def __repr__(self):
        return repr(dict(data=self.data, kind=self.kind))


class Locker:
    def __init__(self, name):
        self._name = name

    def __enter__(self):
        self.fp = open(f"{BASE_DIR}/{self._name}.lock", "wb")
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)

    def __exit__(self, _type, value, tb):
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
        self.fp.close()


def cache_name(key):
    return CACHE_DIR / f"{BASE_NAME}-{str(datetime.date.today())}-{sha256(key.encode()).hexdigest()[:10]}.cache"


def fetch_json_data(url, params={}, *, encoding="utf-8", headers=None, data=None, json=None, post_process=None):
    cache_file = cache_name(f"{url}:{params}:{headers}:{data}:{json}").with_suffix(".cache.data.gz")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url} {params}")
        common_args = dict(
            params=params,
            headers={"user-agent": "mikedld-osm/1.0", **(headers or {})},
            proxies=PROXIES)
        if data is not None or json is not None:
            r = requests.post(url, **common_args, data=data, json=json)
        else:
            r = requests.get(url, **common_args)
        r.raise_for_status()
        result = r.content
        if ENABLE_CACHE:
            cache_file.write_bytes(compress(result))
    else:
        result = decompress(cache_file.read_bytes())
    result = result.decode(encoding)
    if post_process:
        result = post_process(result)
    return json_loads(result)


def fetch_html_data(url, params={}, *, encoding="utf-8", headers=None):
    cache_file = cache_name(f"{url}:{params}:{headers}").with_suffix(".cache.data.gz")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url} {params}")
        r = requests.get(url, params=params, headers={"user-agent": "mikedld-osm/1.0", **(headers or {})})
        r.raise_for_status()
        result = r.content
        if ENABLE_CACHE:
            cache_file.write_bytes(compress(result))
    else:
        result = decompress(cache_file.read_bytes())
    result = result.decode(encoding)
    return etree.fromstring(result, etree.HTMLParser())


@retry(stop_max_attempt_number=3, wait_fixed=10000)
def overpass_query(query, country="PT"):
    full_query = f'[out:json]; area[admin_level=2]["ISO3166-1"="{country}"] -> .country; {query} out meta center;'
    cache_file = cache_name(full_query).with_suffix(".cache.overpass.gz")
    if not ENABLE_OVERPASS_CACHE or not cache_file.exists():
        # print(f"Querying Overpass: {full_query}")
        r = requests.post("http://overpass-api.de/api/interpreter", data=full_query)
        r.raise_for_status()
        result = r.json()["elements"]
        if ENABLE_OVERPASS_CACHE:
            cache_file.write_bytes(compress(json_dumps(result).encode("utf-8")))
    else:
        result = json_loads(decompress(cache_file.read_bytes()).decode("utf-8"))
    return result


def titleize(name):
    return "".join(
        word if word in PT_ARTICLES else (word.upper() if re.fullmatch(r"[ivxlcdm]{2,}", word) else word.capitalize())
        for word in re.split(r"\b", name.lower()))


def distance(a, b):
    dlat = radians(b[0]) - radians(a[0])
    dlon = radians(b[1]) - radians(a[1])
    a = pow(sin(dlat / 2), 2) + cos(radians(a[0])) * cos(radians(b[0])) * pow(sin(dlon / 2), 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = 6371000 * c
    return d


def opening_weekdays(days):
    ranges =[]
    for k, g in itertools.groupby(enumerate(days), lambda x: x[0] - x[1]):
        g = list(g)
        ranges.append((g[0][1],g[-1][1]))
    ranges = [
        DAYS[a] if a == b else (f"{DAYS[a]},{DAYS[b]}" if a == b - 1 else f"{DAYS[a]}-{DAYS[b]}")
        for a, b in ranges
    ]
    return ",".join(ranges)


# https://stackoverflow.com/a/78259311/583456
def gregorian_easter(year):
    century = year // 100
    lunar_adj = (8 * century + 13) // 25
    solar_adj = -century + century // 4
    total_adj = solar_adj + lunar_adj
    leap_months = (210 * year - year % 19 + 19 * total_adj + 266) // 570
    full_moon = (6725 * year + 18) // 19 + 30 * leap_months - lunar_adj + year // 4 + 3
    if 286 <= (total_adj + year % 19 * 11) % 30 * 19 - year % 19 <= 312:
        full_moon -= 1
    week = full_moon // 7 - 38
    return datetime.date.fromordinal(week * 7)


def lookup_postcode(postcode):
    with Locker("postal_codes"):
        codes = {}
        codes_file = BASE_DIR / "postal_codes.json"
        if codes_file.exists():
            codes = json_loads(codes_file.read_text())
        if postcode not in codes:
            cp = postcode.split("-", 1)
            page = requests.get("https://www.codigo-postal.pt/", params={"cp4": cp[0], "cp3": cp[1] if len(cp) > 1 else ""}, headers={"user-agent": "mikedld-osm/1.0"})
            page.raise_for_status()
            page_tree = etree.fromstring(page.content.decode("utf-8"), etree.HTMLParser())
            place_els = page_tree.xpath("//div[@class='places']/p[not(@id)]")
            page_coords = [
                x.split(",")
                for el in place_els
                for x in (["".join(el.xpath(".//*[contains(@class, 'gps')]/text()")).strip()] if "".join(el.xpath(".//span[@class='cp']/text()")).startswith(postcode) else [])
                if x
            ]
            if page_coords:
                coords = [
                    sum([float(x[0]) for x in page_coords]) / len(page_coords),
                    sum([float(x[1]) for x in page_coords]) / len(page_coords),
                ]
                places = [
                    "".join(el.xpath(".//span[@class='cp']/following-sibling::text()")).strip()
                    for el in place_els
                    if "".join(el.xpath(".//span[@class='cp']/text()")).startswith(postcode)
                ]
                #places = [
                #    "".join(el.xpath(".//span[@class='local'][1]/text()")).split(",")[0].strip()
                #    for el in place_els
                #    if "".join(el.xpath(".//span[@class='cp']/text()")).startswith(postcode)
                #]
                places = [(k, len(list(g))) for k, g in itertools.groupby(sorted(places))]
                place = sorted(places, key=lambda x: -x[1])[0][0]
                codes[postcode] = [coords, place]
                codes_file.write_text(json_dumps(codes))
        result = codes.get(postcode)
        if result:
            result[1] = titleize(POSTCODE_CITIES.get(result[1].lower(), result[1]))
        return result


def write_diff(title, ref, diff, html=True, osm=True):
    #for d in diff:
    #    if d.kind == "old":
    #        continue
    #    print(d.diff())

    env = Environment(loader = FileSystemLoader(BASE_DIR / "templates"))
    env.filters["fromisoformat"] = datetime.datetime.fromisoformat
    env.filters["naturaltime"] = naturaltime
    env.filters["lat"] = lambda x: x.lat
    env.filters["lon"] = lambda x: x.lon

    context = {
        "name": BASE_NAME,
        "title": title,
        "ref": ref,
        "diff": diff,
        "have_html": html,
        "have_osm": osm,
    }

    if html:
        template = env.get_template("diff_html.jinja")
        output = template.render(context)
        with open(f"{BASE_DIR}/{BASE_NAME}.html", "w+") as f:
            print(output, file=f)

    if osm:
        template_osm = env.get_template("diff_osm.jinja")
        output_osm = template_osm.render(context)
        with open(f"{BASE_DIR}/{BASE_NAME}.osm", "w+") as f:
            print(output_osm, file=f)

    with Locker("stats"):
        stats = {}
        stats_file = BASE_DIR / "stats.json"
        if stats_file.exists():
            stats = json_loads(stats_file.read_text())
        now = datetime.datetime.now(datetime.UTC)
        if old_stats := dict(stats.get(BASE_NAME, {})):
            if datetime.datetime.fromisoformat(old_stats["date"]).date() != now.date():
                old_stats.pop("title", None)
                old_stats.pop("previous", None)
            else:
                old_stats = dict(old_stats.get("previous", {}))
        stats[BASE_NAME] = {
            "title": title,
            "date": now.strftime("%FT%TZ"),
            "total": len(diff),
            "new": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "new"],
            "mod": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "mod"],
            "del": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "del"],
            "previous": old_stats
        }
        stats_file.write_text(json_dumps(stats))

LANDLINE_REGIONAL_CODES = {
    "Abrantes": 	"241",
    "Angra do Heroísmo": 	"295",
    "Arganil": 	"235",
    "Aveiro": 	"234",
    "Barreiro": 	"207",
    "Beja": 	"284",
    "Braga": 	"253",
    "Bragança": 	"273",
    "Caldas da Rainha": 	"262",
    "Castelo Branco": 	"272",
    "Castro Verde": 	"286",
    "Chaves": 	"276",
    "Coimbra": 	"239",
    "Covilhã": 	"275",
    "Estremoz": 	"268",
    "Évora": 	"266",
    "Faro": 	"289",
    "Figueira da Foz": 	"233",
    "Funchal": 	"291",
    "Gouveia": 	"238",
    "Guarda": 	"271",
    "Horta": 	"292",
    "Idanha-a-Nova": 	"277",
    "Leiria": 	"244",
    "Lisboa": 	"21",
    "Marco de Canaveses": 	"255",
    "Mealhada": 	"231",
    "Mirandela": 	"278",
    "Moura": 	"285",
    "Odemira": 	"283",
    "Penafiel": 	"255",
    "Peso da Régua": 	"254",
    "Pombal": 	"236",
    "Ponta Delgada": 	"296",
    "Ponte de Sôr": 	"242",
    "Portalegre": 	"245",
    "Portimão": 	"282",
    "Porto": 	"22",
    "Proença-a-Nova": 	"274",
    "Santarém": 	"243",
    "Santiago do Cacém": 	"269",
    "São João da Madeira": 	"256",
    "Seia": 	"238",
    "Setúbal": 	"265",
    "Tavira": 	"281",
    "Torre de Moncorvo": 	"279",
    "Torres Novas": 	"249",
    "Torres Vedras": 	"261",
    "Valença": 	"251",
    "Viana do Castelo": 	"258",
    "Vila Franca de Xira": 	"263",
    "Vila Nova de Famalicão": 	"252",
    "Vila Real": 	"259",
    "Viseu": 	"232",
}

def format_phonenumber(phone):
    phone = re.sub(r"[^\d]", "", phone)
    
    if len(phone) == 9:
        phone = "351" + phone
    elif len(phone) != 12:
        return phone
    
    if phone.startswith("3519"): # Mobile numbers, formatted as +351 9xx xxx xxx
        return f"+{phone[:3]} {phone[3:6]} {phone[6:9]} {phone[9:]}"
    elif phone.startswith("3512"): # Landline numbers
        region_code = phone[3:6]
        if region_code in LANDLINE_REGIONAL_CODES.values():
            return f"+{phone[:3]} {phone[3:6]} {phone[6:9]} {phone[9:]}"
        region_code = phone[3:5]
        if region_code in LANDLINE_REGIONAL_CODES.values():
            return f"+{phone[:3]} {phone[3:5]} {phone[5:8]} {phone[8:]}"
        return phone
    else:
        return phone

"""
Convert a list of numbers to a list of ranges.
Example:
to_ranges([1, 2, 3, 5, 6, 8, 9]) 
returns [[1, 3], [5, 6], [8, 9]]
"""
def to_ranges(nums):
    if not nums:
        return []

    nums.sort()
    ranges = []
    start = end = nums[0]

    for num in nums[1:]:
        if num == end + 1:
            end = num
        else:
            ranges.append([start, end])
            start = end = num
    ranges.append([start, end])

    return ranges

def merge_weekdays(days):
    days_index = [DAYS.index(day) for day in days]
    days_index.sort()
    
    days_ranges = to_ranges(days_index)

    # check if we can merge the last and first ranges, since there may be a range from Sunday to Monday
    if days_ranges and len(days_ranges) > 1 and days_ranges[0][0] == 0 and days_ranges[-1][1] == 6:
        days_ranges[0][0] = days_ranges[-1][0]
        days_ranges.pop()
    
    days = [f"{DAYS[start]}-{DAYS[end]}" if start != end else DAYS[start] for start, end in days_ranges]
    return days
