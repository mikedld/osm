#!/usr/bin/env python3

import itertools
import re
from urllib.parse import urljoin

from impl.common import DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, write_diff


DATA_URL = "https://jysk.pt/api/stores"

REF = "ref"

CITIES = {
    "2840-009": "Seixal",
    "4700-068": "Braga",
    "4710-426": "Braga",
    "8200-856": "Guia",
}


def fetch_data():
    result = fetch_json_data(DATA_URL)
    return result["data"]


def format_time(v):
    return f"{v // 100:02d}:{v % 100:02d}"


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"jysk",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        branch = re.sub(r"^(.+?\w)-(\w.+)$", r"\1 - \2", nd["name"])
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["latitude"]
            d.data["lon"] = nd["longitude"]
            old_data.append(d)
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = "furniture"
        d["name"] = "JYSK"
        d["brand"] = "JYSK"
        d["brand:wikidata"] = "Q138913"
        d["brand:wikipedia"] = "pt:Jysk"
        d["branch"] = branch

        schedule = [
            {
                "d": (x["day"] + 6) % 7,
                "t": f"{format_time(x['startHours'])}-{format_time(x['endHours'])}"
                if x["startHours"] and x["endHours"]
                else "off",
            }
            for x in nd["openingHours"]
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k,
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
        d["opening_hours"] = "; ".join(schedule)
        d["source:opening_hours"] = "website"

        phone = re.sub(r"\D+", "", nd["telephone"] or "")
        if phone and len(phone) == 12:
            phone = phone.removeprefix("351")
        if phone and len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["contact:email"] = nd["email"]
        d["website"] = urljoin(DATA_URL, nd["url"])
        d["contact:facebook"] = "JYSK-Portugal-532889397052861"
        d["contact:instagram"] = "jysk.portugal"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        d["addr:postcode"] = nd["zipCode"]
        d["addr:city"] = CITIES.get(d["addr:postcode"], nd["city"].strip(" ,"))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([nd["street"], nd["streetSupplement"] or ""]).strip("; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Jysk", REF, old_data)
