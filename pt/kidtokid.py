#!/usr/bin/env python3

import html
import itertools
import re

from lxml import etree

from impl.common import DiffDict, fetch_json_data, overpass_query, titleize, opening_weekdays, distance, write_diff


DATA_URL = "https://www.kidtokid.pt/wp-admin/admin-ajax.php"

REF = "ref"

DAYS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
CITIES = {
}


def fetch_data():
    params = {
        "action": "store_search",
        "lat": 38.306893,
        "lng": -17.050891,
        "max_results": "999",
        "search_radius": "999",
        "autoload": "1",
    }
    result = fetch_json_data(DATA_URL, params=params)
    result = [x for x in result if x["country"] == "Portugal"]
    result = [
        {
            **x,
            "hours": [
                el.xpath(".//td//text()")
                for el in etree.fromstring(x["hours"], etree.HTMLParser()).xpath("//tr")
            ]
        }
        for x in result
    ]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"Kid to [Kk]id|KID2KID"](area.country);')]

    for nd in new_data:
        public_id = str(nd["id"])
        branch = html.unescape(nd["store"]).replace("–", "-")
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
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["lng"])
            old_data.append(d)

        d[REF] = public_id
        if d.kind == "new":
            d["shop"] = "second_hand" # ???
        d["name"] = "Kid to Kid"
        # d["brand"] = ""
        # d["brand:wikidata"] = ""
        # d["brand:wikipedia"] = ""
        d["branch"] = branch

        schedule = [
            {
                "d": DAYS.index(x[0].lower()),
                "t": x[1].replace(" ", "") if x[1] != "Fechado" else "off",
            }
            for x in nd["hours"]
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
        d["opening_hours"] = "; ".join(schedule)
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        phone = nd["phone"].split("(")[0].replace(" ", "")
        if len(phone) == 13 and phone.startswith("+351"):
            phone = phone[4:]
        if phone and len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.add("contact:email")
        d["website"] = nd["permalink"] or "https://www.kidtokid.pt/"
        d["contact:facebook"] = "k2k.portugal"
        d["contact:youtube"] = "https://www.youtube.com/@kidtokidportugal1262"
        d["contact:instagram"] = "kidtokidportugal"
        d["contact:pinterest"] = "kidtokid"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode, city = nd["zip"], nd["city"]
        if " " in postcode:
            postcode, city = postcode.split(" ", 1)
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, titleize(city))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([nd["address"], nd["address2"]]).strip("; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == str(nd["id"])):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Kid to Kid", REF, old_data)
