#!/usr/bin/env python3

import datetime
import itertools
import re

from impl.common import LISBON_TZ, DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, titleize, write_diff


DATA_URL = "https://www.starbucks.pt/api/v2/stores/"
DATA_LOCATIONS = [
    [39.681823, -8.003540, 250],
    [32.771436, -16.704712, 250],
    [38.381039, -28.020630, 250],
]

REF = "ref"

CITIES = {
    "lisbon": "lisboa",
    "oporto": "porto",
    "portimao": "portimão",
    "vila nova de gaia porto": "vila nova de gaia",
}


def fetch_data():
    result = []
    for dl in DATA_LOCATIONS:
        params = {
            "filter[coordinates][latitude]": dl[0],
            "filter[coordinates][longitude]": dl[1],
            "filter[radius]": dl[2],
        }
        result.extend(fetch_json_data(DATA_URL, params=params)["data"])
    result = [x["attributes"] for x in result if x["attributes"]["address"]["countryCode"] == "PT"]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query("nwr[amenity][name=Starbucks](area.country);")]

    for nd in new_data:
        public_id = nd["storeNumber"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["coordinates"]["latitude"]), float(nd["coordinates"]["longitude"])]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)

        tags_to_reset = set()

        d[REF] = public_id
        d["amenity"] = "cafe"
        d["cuisine"] = "coffee_shop"
        d["name"] = "Starbucks"
        d["official_name"] = "Starbucks Coffee"
        d["brand"] = "Starbucks"
        d["brand:wikidata"] = "Q37158"
        d["brand:wikipedia"] = "pt:Starbucks"

        if nd["open24x7"]:
            d["opening_hours"] = "24/7"
        elif nd["openHours"]:
            schedule = [
                {
                    "d": datetime.datetime.strptime(x["date"].split("T")[0], "%Y-%m-%d").astimezone(LISBON_TZ).date().weekday(),
                    "t": f"{x['openTime'][:5]}-{x['closeTime'][:5]}",
                }
                for x in nd["openHours"]
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

        phone = nd["phoneNumber"].lstrip("0")
        if len(phone) == 12 and phone.startswith("351"):
            phone = phone[3:]
        if phone and len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://www.starbucks.pt/"
        d["contact:facebook"] = "StarbucksPortugal"
        d["contact:twitter"] = "starbucksPTG"
        d["contact:instagram"] = "starbucksptg"
        d["contact:tiktok"] = "starbucksportugal"
        d["contact:email"] = "starbucks@starbucks.pt"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        city = nd["address"]["city"].lower()
        d["addr:city"] = titleize(CITIES.get(city, city))

        postcode = re.sub(r"[^0-9]+", "", nd["address"]["postalCode"])
        if len(postcode) == 7:
            d["addr:postcode"] = f"{postcode[:4]}-{postcode[4:]}"

        if d.kind == "new" and not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"]):
            d["x-dld-addr"] = "; ".join(
                [
                    x
                    for x in (
                        nd["address"]["streetAddressLine1"],
                        nd["address"]["streetAddressLine2"],
                        nd["address"]["streetAddressLine3"],
                    )
                    if x
                ]
            )

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["storeNumber"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Starbucks", REF, old_data)
