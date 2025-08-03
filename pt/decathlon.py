#!/usr/bin/env python3

import itertools
import re

from unidecode import unidecode

from impl.common import BASE_NAME, DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, titleize, write_diff
from impl.config import CONFIG


XCONFIG = CONFIG[BASE_NAME]

DATA_URL = "https://api.woosmap.com/stores/search"

REF = "ref"

CITIES = {
    "2830-411": "Coina",
    "4450-820": "Perafita",
    "4475-023": "Castêlo da Maia",
    "8200-425": "Guia",
}


def fetch_data():
    params = {
        "key": XCONFIG["api_key"],
        "query": 'country:="PT"',
    }
    headers = {
        "referer": "https://www.decathlon.pt/",
    }
    result = fetch_json_data(DATA_URL, params=params, headers=headers)
    result = [
        {
            **x["properties"],
            **x["geometry"],
        }
        for x in result["features"]
    ]
    return result


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r"\W", " ", e)
    e = e.strip()
    e = re.sub(r"\s+", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"Decathlon"](area.country);')]

    for nd in new_data:
        public_id = nd["store_id"]
        branch = titleize(nd["name"].strip())
        is_ex = branch.endswith("Express")
        is_con = branch.endswith("Connect")
        branch = re.sub(r"\s+(Express|Connect)$", "", branch)
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = list(reversed(nd["coordinates"]))
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{int(public_id)}"
            d.data["lat"], d.data["lon"] = reversed(nd["coordinates"])
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "sports"
        d["name"] = "Decathlon Express" if is_ex else ("Decathlon Connect" if is_con else "Decathlon")
        d["brand"] = "Decathlon"
        d["brand:wikidata"] = "Q509349"
        d["brand:wikipedia"] = "pt:Decathlon (empresa)"
        d["branch"] = branch

        schedule = nd["opening_hours"]["usual"]
        if any(v for k, v in schedule.items()):
            schedule = [
                {
                    "d": int(k) - 1,
                    "t": ",".join([f"{x['start']}-{x['end']}" for x in v]) if v else "off",
                }
                for k, v in schedule.items()
            ]
            schedule = [
                {
                    "d": sorted([x["d"] for x in g]),
                    "t": k,
                }
                for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
            ]
            schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
        else:
            schedule = ["Mo-Su off"]
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = nd["contact"].get("phone", "")
        phone = phone.removeprefix("+351")
        if phone.replace("0", "") and len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = f"https://www.decathlon.pt/store-view/loja-de-desporto-{get_url_part(nd['name'])}-{public_id}"
        d["contact:facebook"] = "decathlonportugal"
        d["contact:youtube"] = "https://www.youtube.com/@decathlon_portugal"
        d["contact:instagram"] = "decathlonportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/decathlonportugal/"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = nd["address"]["zipcode"]
        if len(postcode) == 4:
            if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                postcode = d["addr:postcode"]
            else:
                postcode += "-000"
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, nd["address"]["city"])
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([x.strip() for x in nd["address"]["lines"]])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["store_id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Decathlon", REF, old_data, osm=True)
