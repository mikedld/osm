import __main__

import datetime
import fcntl
import itertools
import json
import re
from hashlib import sha256
from pathlib import Path

import requests
from jinja2 import Environment, FileSystemLoader

from .config import ENABLE_CACHE


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
    return f"{BASE_DIR}/{BASE_NAME}-{str(datetime.date.today())}-{sha256(key.encode()).hexdigest()[:10]}"


def overpass_query(query):
    full_query = f"[out:json]; {query} out meta center;"
    cache_file = Path(f"{cache_name(full_query)}.json")
    if not cache_file.exists():
        # print(f"Querying Overpass: {full_query}")
        result = requests.post("http://overpass-api.de/api/interpreter", data=full_query).json()
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


def titleize(name):
    return "".join(
        word if word in PT_ARTICLES else word.capitalize()
        for word in re.split(r"\b", name.lower()))


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

def write_diff(name, ref, diff):
    #for d in diff:
    #    if d.kind == "old":
    #        continue
    #    print(d.diff())

    env = Environment(loader = FileSystemLoader(BASE_DIR / "templates"))
    env.filters["lat"] = lambda x: x.lat
    env.filters["lon"] = lambda x: x.lon
    template = env.get_template("diff.jinja")
    output = template.render(name=name, ref=ref, diff=diff)

    with open(f"{BASE_DIR}/{BASE_NAME}.html", "w+") as f:
        print(output, file=f)

    with Locker("stats"):
        stats = {}
        stats_file = BASE_DIR / "stats.json"
        if stats_file.exists():
            stats = json.loads(stats_file.read_text())
        stats[BASE_NAME] = {
            "title": name,
            "date": datetime.datetime.now(datetime.UTC).strftime("%FT%TZ"),
            "total": len(diff),
            "diff": sum(1 for d in diff if d.kind != "old"),
        }
        stats_file.write_text(json.dumps(stats))
