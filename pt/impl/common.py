import datetime
import fcntl
import itertools
import re
from gzip import compress, decompress
from hashlib import sha256
from json import dumps as json_dumps
from json import loads as json_loads
from math import asin, atan2, cos, degrees, pi, radians, sin, sqrt
from pathlib import Path

import pytz
import requests
from humanize import naturaltime
from jinja2 import Environment, FileSystemLoader
from lxml import etree
from retrying import retry
from shapely import voronoi_polygons
from shapely.geometry import Point, Polygon, shape

import __main__

from .config import ENABLE_CACHE, ENABLE_OVERPASS_CACHE, PROXIES


BASE_DIR = Path(__main__.__file__).parent
BASE_NAME = Path(__main__.__file__).stem

CACHE_DIR = BASE_DIR / "cache"

EARTH_RADIUS = 6378137

LISBON_TZ = pytz.timezone("Europe/Lisbon")

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
        if key not in self.old_tags:
            self.old_tags[key] = self[key]
        self.data["tags"][key] = value
        if self.kind == "old":
            self.kind = "mod"

    def __repr__(self):
        return repr({"data": self.data, "kind": self.kind})


class RedoIter:
    def __init__(self, items):
        self.redo = False
        self._items = items

    def __iter__(self):
        i = 0
        while i < len(self._items):
            self.redo = False
            yield self._items[i]
            if not self.redo:
                i += 1


class Locker:
    def __init__(self, name):
        self._name = name

    def __enter__(self):
        self.fp = (BASE_DIR / f"{self._name}.lock").open("wb")
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)

    def __exit__(self, _type, value, tb):
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
        self.fp.close()


def cache_name(key):
    today = datetime.datetime.now(datetime.UTC).astimezone(LISBON_TZ).date()
    return CACHE_DIR / f"{BASE_NAME}-{today}-{sha256(key.encode()).hexdigest()[:10]}.cache"


def fetch_json_data(
    url,
    params=None,
    *,
    encoding="utf-8",
    headers=None,
    var_headers=None,
    data=None,
    json=None,
    post_process=None,
    verify_cert=True,
):
    cache_file = cache_name(f"{url}:{params}:{headers}:{data}:{json}").with_suffix(".cache.data.gz")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url} {params}")  # noqa: ERA001
        common_args = {
            "params": params or {},
            "headers": {"user-agent": "mikedld-osm/1.0", **(headers or {}), **(var_headers or {})},
            "proxies": PROXIES,
            "verify": verify_cert,
        }
        if data is not None or json is not None:
            r = requests.post(url, **common_args, data=data, json=json, timeout=120)
        else:
            r = requests.get(url, **common_args, timeout=120)
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


def fetch_html_data(url, params=None, *, encoding="utf-8", headers=None):
    cache_file = cache_name(f"{url}:{params}:{headers}").with_suffix(".cache.data.gz")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url} {params}")  # noqa: ERA001
        r = requests.get(url, params=params or {}, headers={"user-agent": "mikedld-osm/1.0", **(headers or {})}, timeout=120)
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
        # print(f"Querying Overpass: {full_query}")  # noqa: ERA001
        r = requests.post("http://overpass-api.de/api/interpreter", data=full_query, timeout=300)
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
        for word in re.split(r"\b", name.lower())
    )


def frange(x, y, step):
    while x < y:
        yield x
        x += step


def distance(a, b):
    dlat = radians(b[0]) - radians(a[0])
    dlon = radians(b[1]) - radians(a[1])
    a = pow(sin(dlat / 2), 2) + cos(radians(a[0])) * cos(radians(b[0])) * pow(sin(dlon / 2), 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = 6371000 * c
    return d


def relation_polygon(rel_id, params=None):
    full_params = {
        "id": rel_id,
        "params": params or "0",
    }
    # TODO: Make BASE_NAME-agnostic to avoid extra fetches.
    return shape(fetch_json_data("https://polygons.openstreetmap.fr/get_geojson.py", params=full_params))


def country_polygon():
    return relation_polygon(295480, "0.020000-0.005000-0.005000")


def offset(c1, distance, bearing):
    lat1 = radians(c1[1])
    lon1 = radians(c1[0])
    d_by_r = distance / EARTH_RADIUS
    lat = asin(sin(lat1) * cos(d_by_r) + cos(lat1) * sin(d_by_r) * cos(bearing))
    lon = lon1 + atan2(sin(bearing) * sin(d_by_r) * cos(lat1), cos(d_by_r) - sin(lat1) * sin(lat))
    return [degrees(lon), degrees(lat)]


def circle(center, radius, *, edges=32, bearing=0, direction=1):
    start = radians(bearing)
    coordinates = [offset(center, radius, start + (direction * 2 * pi * -i) / edges) for i in range(edges)]
    coordinates.append(coordinates[0])
    return Polygon(coordinates)


def label_point(g):
    pts = {Point(p) for e in voronoi_polygons(g, only_edges=True).geoms for p in e.coords if g.contains(Point(p))}
    pmax = max(pts, default=None, key=lambda p: min(p.distance(g.exterior), min(p.distance(g.interiors), default=360)))
    if pmax is None:
        pmax = g.centroid
    return (pmax.x, pmax.y)


def cover_polygon(g, radius, query):
    geoms = [g]
    sradius = radius * 0.95

    while geoms:
        g = geoms.pop()
        if hasattr(g, "geoms"):
            geoms.extend(g.geoms)
            continue
        if g.is_empty:
            continue

        rp = label_point(g)

        if circle(rp, radius).contains(g):
            g -= circle(rp, query(rp))
        else:
            bounds = g.bounds
            lat_step = offset(bounds[0:2], sradius, 0)[1] - bounds[1]
            lat_offset = lat_step - (rp[1] - bounds[1]) % lat_step
            lon_step = offset(bounds[0:2], sradius * 2, 90)[0] - bounds[0]
            lon_offset = lon_step - (rp[0] - bounds[0]) % lon_step

            odd = int((rp[1] - bounds[1]) / lat_step) % 2 != 0
            for lat in frange(bounds[1] - lat_offset, bounds[3] + lat_step, lat_step):
                for lon in frange(bounds[0] - lon_offset + (0 if odd else lon_step / 2), bounds[2] + lon_step, lon_step):
                    c = [lon, lat]
                    if not circle(c, radius).intersects(g):
                        continue
                    g -= circle(c, query(c))
                odd = not odd

        geoms.append(g)


def opening_weekdays(days):
    ranges = []
    for _k, g in itertools.groupby(enumerate(days), lambda x: x[0] - x[1]):
        g = list(g)
        ranges.append((g[0][1], g[-1][1]))
    ranges = [DAYS[a] if a == b else (f"{DAYS[a]},{DAYS[b]}" if a == b - 1 else f"{DAYS[a]}-{DAYS[b]}") for a, b in ranges]
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
            page = requests.get(
                "https://www.codigo-postal.pt/",
                params={"cp4": cp[0], "cp3": cp[1] if len(cp) > 1 else ""},
                headers={"user-agent": "mikedld-osm/1.0"},
                timeout=120,
            )
            page.raise_for_status()
            page_tree = etree.fromstring(page.content.decode("utf-8"), etree.HTMLParser())
            place_els = page_tree.xpath("//div[@class='places']/p[not(@id)]")
            page_coords = [
                x.split(",")
                for el in place_els
                for x in (
                    ["".join(el.xpath(".//*[contains(@class, 'gps')]/text()")).strip()]
                    if "".join(el.xpath(".//span[@class='cp']/text()")).startswith(postcode)
                    else []
                )
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
                places = [(k, len(list(g))) for k, g in itertools.groupby(sorted(places))]
                place = sorted(places, key=lambda x: -x[1])[0][0]
                codes[postcode] = [coords, place]
                codes_file.write_text(json_dumps(codes))
        result = codes.get(postcode)
        if result:
            result[1] = titleize(POSTCODE_CITIES.get(result[1].lower(), result[1]))
        return result


def write_diff(title, ref, diff, *, html=True, osm=True):
    env = Environment(loader=FileSystemLoader(BASE_DIR / "templates"), autoescape=True)
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
        with (BASE_DIR / f"{BASE_NAME}.html").open("w+") as f:
            print(output, file=f)

    if osm:
        template_osm = env.get_template("diff_osm.jinja")
        output_osm = template_osm.render(context)
        with (BASE_DIR / f"{BASE_NAME}.osm").open("w+") as f:
            print(output_osm, file=f)

    with Locker("stats"):
        stats = {}
        stats_file = BASE_DIR / "stats.json"
        if stats_file.exists():
            stats = json_loads(stats_file.read_text())
        now = datetime.datetime.now(datetime.UTC).astimezone(LISBON_TZ)
        if old_stats := dict(stats.get(BASE_NAME, {})):
            old_date = datetime.datetime.fromisoformat(old_stats["date"]).astimezone(LISBON_TZ).date()
            if old_date != now.date():
                old_stats.pop("title", None)
                old_stats.pop("previous", None)
            else:
                old_stats = dict(old_stats.get("previous", {}))
        stats[BASE_NAME] = {
            "title": title,
            "date": now.isoformat(),
            "total": len(diff),
            "new": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "new"],
            "mod": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "mod"],
            "del": [[d.data["id"], d.lat, d.lon] for d in diff if d.kind == "del"],
            "previous": old_stats,
        }
        stats_file.write_text(json_dumps(stats))


def format_phonenumber(phone):
    phone = re.sub(r"\D+", "", phone)
    if not phone:
        return ""

    if len(phone) == 9:
        phone = f"351{phone}"
    elif len(phone) != 12:
        return f"<ERR:{phone}>"

    if phone.startswith("351"):
        return f"+{phone[:3]} {phone[3:6]} {phone[6:9]} {phone[9:]}"

    return f"<ERR:{phone}>"


def to_ranges(nums):
    """
    Convert a list of numbers to a list of ranges.
    Example:
    to_ranges([1, 2, 3, 5, 6, 8, 9])
    returns [[1, 3], [5, 6], [8, 9]]
    """

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
