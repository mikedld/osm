#!/usr/bin/env python3

import itertools
from multiprocessing import Pool

from impl.common import (
    DiffDict,
    distance,
    fetch_html_data,
    fetch_json_data,
    opening_weekdays,
    overpass_query,
    titleize,
    write_diff,
)


LEVEL1_DATA_URL = "https://www.5asec.pt/ajax/stores"
LEVEL2_DATA_URL = "https://www.5asec.pt/pt-pt/node/{id}"

REF = "ref"

DAYS = ["2ª feira", "3ª feira", "4ª feira", "5ª feira", "6ª feira", "Sábado", "Domingo"]
CITIES = {
    "4425-500": "Ermesinde",
    "4930-594": "Valença",
    "6430-183": "Mêda",
}


def fetch_level1_data():
    return fetch_json_data(LEVEL1_DATA_URL)


def fetch_level2_data(data):
    result_tree = fetch_html_data(LEVEL2_DATA_URL.format(id=data["nid"]))
    return {
        **data,
        "link": result_tree.xpath("//head/link[@rel='canonical']/@href")[0],
        "address": "".join(result_tree.xpath("//*[contains(@class,'field-name-field-address')]//text()")).strip(),
        "zip-code": "".join(result_tree.xpath("//*[contains(@class,'field-name-field-zip-code')]//text()")).strip(),
        "city": "".join(result_tree.xpath("//*[contains(@class,'field-name-field-city')]//text()")).strip(),
        "phone": "".join(result_tree.xpath("//*[contains(@class,'field-name-field-phone')]//text()")).strip(),
        "schedule": [
            {
                "label": el.xpath("./*/*[@class='field-label']/text()")[0],
                "items": [eli.xpath("./*[@class='placeholder']/text()") for eli in el.xpath(".//*[@class='period']")],
            }
            for el in result_tree.xpath("//*[contains(@class, 'schedule-day')]")
        ],
    }


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"5[ ]?[AÀÁaàá][ ]?[Ss]ec"](area.country);')]

    for nd in new_data:
        public_id = nd["nid"]
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
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = round(float(nd["lat"]), 7) or 38.306893
            d.data["lon"] = round(float(nd["lng"]), 7) or -17.050891
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "dry_cleaning"
        d["name"] = "5àsec"
        d["brand"] = "5àsec"
        d["brand:wikidata"] = "Q2817899"
        d["brand:wikipedia"] = "pt:5àsec"
        d["branch"] = titleize(nd["title"].strip())
        if d["self_service"] not in ("", "no"):
            d["self_service"] = "no"

        schedule = [
            {
                "d": DAYS.index(x["label"]),
                "t": ",".join([f"{(t[0] + '00')[:5]}-{(t[1] + '00')[:5]}" for t in x["items"]]).replace("H", ":") or "off",
            }
            for x in nd["schedule"]
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

        phones = [
            f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
            for phone in nd["phone"].replace(" ", "").split("/")
            if len(phone) == 9
        ]
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = nd["link"]
        d["contact:facebook"] = "5asecportugal"
        d["contact:youtube"] = "https://www.youtube.com/@5asecpt"
        d["contact:instagram"] = "5asecportugal"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        d["addr:postcode"] = nd["zip-code"]
        d["addr:city"] = CITIES.get(nd["zip-code"], titleize(nd["city"]))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            street = [*nd["address"].split(",", 1), ""]
            d["addr:street"] = street[0].strip()
            d["addr:housenumber"] = street[1].strip()

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["nid"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("5àsec", REF, old_data, osm=True)
