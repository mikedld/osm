import __main__

import datetime
import fcntl
import itertools
import json
import re
from hashlib import sha256
from math import atan2, cos, pow, radians, sin, sqrt
from pathlib import Path

import requests
from humanize import naturaltime
from jinja2 import Environment, FileSystemLoader

from .config import ENABLE_OVERPASS_CACHE


BASE_DIR = Path(__main__.__file__).parent
BASE_NAME = Path(__main__.__file__).stem

DAYS = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
PT_ARTICLES = {"e", "a", "o", "de", "do", "da", "dos", "das"}


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
    return f"{BASE_DIR}/{BASE_NAME}-{str(datetime.date.today())}-{sha256(key.encode()).hexdigest()[:10]}.cache"


def overpass_query(query):
    full_query = f"[out:json]; {query} out meta center;"
    cache_file = Path(f"{cache_name(full_query)}.json")
    if not cache_file.exists():
        # print(f"Querying Overpass: {full_query}")
        result = requests.post("http://overpass-api.de/api/interpreter", data=full_query).json()
        if ENABLE_OVERPASS_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


def titleize(name):
    return "".join(
        word if word in PT_ARTICLES else word.capitalize()
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
            stats = json.loads(stats_file.read_text())
        stats[BASE_NAME] = {
            "title": title,
            "date": datetime.datetime.now(datetime.UTC).strftime("%FT%TZ"),
            "total": len(diff),
            "diff": sum(1 for d in diff if d.kind != "old"),
        }
        stats_file.write_text(json.dumps(stats))
