#!/usr/bin/env python3

import itertools
import re
from multiprocessing import Pool

import requests

from impl.common import DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, titleize, write_diff


LEVEL1_DATA_URL = "https://limmia-wasky-public-api-c934cd99c58c.herokuapp.com/localsPages/listStaticLocalsPages"
LEVEL2_DATA_URL = "https://limmia-wasky-public-api-c934cd99c58c.herokuapp.com/localsPages/listStaticLocalsPagesIdentifier"

REF = "ref"

CITIES = {
    "2600-723": "Castanheira do Ribatejo",
    "3830-743": "Gafanha da Nazaré",
    "4470-274": "Moreira",
    "4760-501": "Vila Nova de Famalicão",
}
STREET_ABBREVS = [
    [r"\bav\.? ", "avenida "],
    [r"\beng\. ", "engenheiro "],
    [r"\bdr\. ", "doutor "],
    [r"\bgen\. ", "general "],
    [r"\bpte\. ", "ponte "],
    [r"\br\. ", "rua "],
    [r"\btv\. ", "travessa "],
]


def fetch_level1_data():
    return fetch_json_data(LEVEL1_DATA_URL)["response"]["locations"]


def fetch_level2_data(data):
    params = {
        "identifier": data["identifier"],
    }
    result = fetch_json_data(LEVEL2_DATA_URL, params=params)["response"]["locations"][0]
    return {
        **data,
        **result,
    }


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"washy",i](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["identifier"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["lat"]), float(nd["lng"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["lng"])
            old_data.append(d)
            new_node_id -= 1

        d[REF] = public_id
        d["shop"] = "laundry"
        d["name"] = "Washy"
        d["brand"] = "Washy"
        d["brand:wikidata"] = "Q129416138"
        if branch := re.sub(r"^Washy Lavandaria", "", nd["name"]).strip():
            d["branch"] = branch
        d["self_service"] = "yes"

        schedule = [
            {
                "d": x["dayOfWeek"] - 1,
                "t": f"{x['from1']}-{x['to1'][:5]}",
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
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = nd["phone"].replace(" ", "")
        phone = phone.removeprefix("+351")
        if phone and len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = f"https://www.washy.pt/onde-estamos/#!/{requests.utils.quote(nd['city'])}/{public_id}"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        d["addr:postcode"] = nd["zip"].strip()
        d["addr:city"] = CITIES.get(d["addr:postcode"], nd["city"].strip())
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            street = nd["streetAndNumber"].replace("  ", " ")
            street = re.sub(r"^[Cc]ontinente( [Mm]odelo| [Bb]om [Dd]ia)?[^,]*,\s*", "", street)
            street = re.sub(r",\s*[Cc]ontinente( [Mm]odelo| [Bb]om [Dd]ia)?[^,]*", "", street)
            for r in STREET_ABBREVS:
                street = re.sub(r[0], r[1], street.lower())
            if m := re.fullmatch(r"(.+?),?\s+(\d+(?:-\d+)?|lote (?:[\d.]+))(?:,?\s+(loja \w+))?", street):
                d["addr:street"] = titleize(m[1])
                d["addr:housenumber"] = titleize(m[2])
                if m[3]:
                    d["addr:unit"] = titleize(m[3])
            elif re.match(r"^sitio ", street):
                d["addr:place"] = titleize(street)
            elif re.match(r"^quinta ", street):
                d["addr:suburb"] = titleize(street)
            else:
                m = street.split(",", 1)
                d["addr:street"] = titleize(m[0])
                if len(m) > 1:
                    d["addr:place"] = titleize(m[1].strip())

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["identifier"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Washy", REF, old_data, osm=True)
