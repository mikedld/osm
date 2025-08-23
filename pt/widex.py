#!/usr/bin/env python3

import itertools
from urllib.parse import quote_plus, unquote_plus

from impl.common import DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, write_diff


DATA_URL = "https://prod-cd.widex.pt/sitecore/api/ssc/WSA-Retail-Feature-ShopFinder-Controllers/ShopFinder/1/GetShopsForBusinessContext"

REF = "ref"


def fetch_data():
    params = {
        "brand": "HearUSA",
        "country": "PT",
        "take": 8000,
        "audience": "Retail",
        "urlformat": "country/zip(8)/city/title/id",
        "detailsurl": "/centros-auditivos/pagina-centro/",
        "baAvailableShops": "undefined",
    }
    result = fetch_json_data(DATA_URL, params=params)
    result = [x for x in result if x["headingLabel"]["isPrimary"]]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"Widex"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"].lower()
        branch = nd["title"].removeprefix("Widex").strip()
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
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["latitude"]
            d.data["lon"] = nd["longitude"]
            old_data.append(d)
            new_node_id -= 1

        d[REF] = public_id
        d["shop"] = "hearing_aids"
        d["name"] = "Widex"
        # d["brand"] = "Widex"  # noqa: ERA001
        # d["brand:wikidata"] = "Q3440110"  # noqa: ERA001
        # d["brand:wikipedia"] = "pt:Widex"  # noqa: ERA001
        d["branch"] = branch

        schedule = [
            {
                "d": x["dayOfWeek"] - 1,
                "t": "-".join((x["from"], x["to"])).replace("24:", "00:"),
            }
            for x in nd["openingHoursCollection"]
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k if k != "-" else "off",
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            d["source:opening_hours"] = "website"

        phone = nd["contactPhone"].replace(" ", "").split("-")[0]
        if len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["contact:email"] = nd["contactEmail"]
        d["website"] = f"https://www.widex.pt/{quote_plus(unquote_plus(nd['shopDetailsLink']), safe='/')}"
        d["contact:facebook"] = "widex.portugal"
        d["contact:youtube"] = "https://www.youtube.com/@WidexPT"
        d["contact:instagram"] = "widex_pt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/widex-portugal-especialistas-em-audicao"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        d["addr:postcode"] = nd["postalCode"]
        d["addr:city"] = nd["city"]
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([x for x in (nd["addressLine1"], nd["addressLine2"], nd["addressLine3"]) if x])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["id"].lower()):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Widex", REF, old_data, osm=True)
