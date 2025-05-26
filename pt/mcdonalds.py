#!/usr/bin/env python3

import datetime
import html
import itertools
import json
import re
import uuid
from multiprocessing import Pool
from pathlib import Path

import requests
from lxml import etree

from impl.common import DiffDict, cache_name, overpass_query, distance, opening_weekdays, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

BRANCHES = {
    "Aeroporto de Lisboa - T1": "Aeroporto de Lisboa - Terminal 1",
    "Aeroporto de Lisboa - T2": "Aeroporto de Lisboa - Terminal 2",
    "Coimbra Forum": "Coimbra - Fórum",
    "D Carlos I": "Dom Carlos I",
    "Vila Real Nosso Shopping": "Vila Real - Nosso Shopping",
}
DAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h(\d{2}) às (\d{2})h(\d{2})": r"\1:\2-\3:\4",
    r"(\d{1})h(\d{2}) às (\d{2})h(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})h(\d{2}) às (\d{1})h(\d{2})": r"\1:\2-0\3:\4",
    r"(\d{1})h(\d{2}) às (\d{1})h(\d{2})": r"0\1:\2-0\3:\4",
}


def fetch_stores_data(url):
    cache_file = Path(f"{cache_name(url)}.json")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        r = requests.get(url)
        r.raise_for_status()
        result = r.content.decode("utf-8")
        result = re.sub(r"^.*var restaurantsJson[ ]*=[ ]*'\[(.+)\]'.*$", r"[\1]", result, flags=re.S)
        result = json.loads(result)
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


def fetch_store_data(store):
    url = f"https://www.mcdonalds.pt{store['Url']}"
    cache_file = Path(f"{cache_name(url)}.html")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        r = requests.get(url)
        r.raise_for_status()
        result = r.content.decode("utf-8")
        result_tree = etree.fromstring(result, etree.HTMLParser())
        etree.indent(result_tree)
        result = etree.tostring(result_tree, encoding="utf-8", pretty_print=True).decode("utf-8")
        if ENABLE_CACHE:
            cache_file.write_text(result)
    else:
        result = cache_file.read_text()
    result_tree = etree.fromstring(result)
    return {
        **store,
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, "mcdonalds:" + store["Url"].split("/")[2])),
        "schedules": {
            "".join(el.xpath('h6/text()')).strip(): {
                re.sub(r"V[ée]spera\s+(de\s+)?[Ff]eriado", r"Véspera Feriado", "".join(el2.xpath('cite/text()')).strip()): "".join(el2.xpath('span/text()')).strip()
                for el2 in el.xpath("ul/li")
            }
            for el in result_tree.xpath("//div[contains(@class, 'restaurantSchedule__service')]")
        },
        **json.loads(re.sub(r"[\r\n]", "", "".join(result_tree.xpath("//script[@type='application/ld+json']/text()")))),
    }


def schedule_time(v):
    sa = v
    sb = "<ERR>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def opening_hours(data, title):
    schedule = {k: v for k, v in data["schedules"].get(title, {}).items() if k in DAYS}
    if not schedule:
        return None

    schedule = [
        {
            "d": DAYS.index(k),
            "t": schedule_time(v),
        }
        for k, v in schedule.items()
    ]
    schedule = [
        {
            "d": sorted([x["d"] for x in g]),
            "t": k
        }
        for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
    ]
    schedule = [
        f"{opening_weekdays(x['d'])} {x['t']}"
        for x in sorted(schedule, key=lambda x: x["d"][0])
    ]
    if exs := {k: v for k, v in data["schedules"].get(title, {}).items() if k not in DAYS}:
        for k, v in exs.items():
            if k == "Véspera Feriado":
                t = schedule_time(v)
                if len(schedule) != 1 or schedule[0] != f"Mo-Su {t}":
                    schedule.append(f"PH -1 day {t}")
            else:
                schedule.append("<ERR>")
    schedule = "; ".join(schedule)
    if schedule == "Mo-Su 00:00-00:00":
        schedule = "24/7"
    return schedule


if __name__ == "__main__":
    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[amenity][amenity!=charging_station][amenity!=bicycle_rental][amenity!=social_facility][amenity!=parking][name~"McDonald"](area.p); );')["elements"]]

    data_url = "https://www.mcdonalds.pt/restaurantes"
    new_data = fetch_stores_data(data_url)
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_store_data, new_data))

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["Lat"], nd["Lng"]]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 250], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["Lat"]
            d.data["lon"] = nd["Lng"]
            old_data.append(d)
            new_node_id -= 1

        tags_to_reset = set()

        d[REF] = public_id
        d["amenity"] = "fast_food"
        d["cuisine"] = "burger"
        d["name"] = "McDonald's"
        d["brand"] = "McDonald's"
        d["brand:wikidata"] = "Q38076"
        d["brand:wikipedia"] = "pt:McDonald's"
        d["branch"] = BRANCHES.get(nd["Name"].strip(), nd["Name"].strip())
        d["takeaway"] = "yes"

        if schedule := opening_hours(nd, "Restaurante"):
            d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"
        if schedule := opening_hours(nd, "McDrive"):
            d["drive_through"] = "yes"
            d["opening_hours:drive_through"] = schedule
        else:
            if d["drive_through"] != "no":
                tags_to_reset.add("drive_through")
            tags_to_reset.add("opening_hours:drive_through")

        phone = re.sub(r"\s+", "", nd["contactPoint"]["telephone"])
        if len(phone) == 13:
            phone = f"+351 {phone[4:7]} {phone[7:10]} {phone[10:13]}"
            if phone[5:6] == "9":
                d["contact:mobile"] = phone
            else:
                tags_to_reset.add("contact:mobile")
            if phone[5:6] != "9":
                d["contact:phone"] = phone
            else:
                tags_to_reset.add("contact:phone")
        d["contact:website"] = f"https://www.mcdonalds.pt{nd['Url']}"
        d["contact:facebook"] = "McDonaldsPortugal"
        d["contact:youtube"] = "https://www.youtube.com/McDonaldsPortugal"
        d["contact:instagram"] = "mcdonaldsportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/mcdonald's-corporation/"
        d["contact:tiktok"] = "mcdonalds.pt"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["address"]
        postcode = re.sub(r"[^0-9]+", "", address["postalCode"])
        if len(postcode) == 7 and postcode.endswith("000"):
            postcode = postcode[:4]
        if len(postcode) == 4:
            postcode += d["addr:postcode"][5:] if len(d["addr:postcode"]) == 8 else "000"
        if len(postcode) == 7:
            d["addr:postcode"] = f"{postcode[0:4]}-{postcode[4:]}"
        elif postcode:
            d["addr:postcode"] = "<ERR>"
        if not d["addr:street"] and not (d["addr:housenumber"] or d["addr:housename"] or d["nohousenumber"]) and not d["addr:place"] and not d["addr:suburb"]:
            d["x-dld-addr"] = html.unescape(nd["address"]["streetAddress"]).strip()

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("McDonald's", REF, old_data, osm=True)
