#!/usr/bin/env python3

import datetime
import json
import itertools
import re
from pathlib import Path

import requests

from impl.common import DiffDict, cache_name, overpass_query, titleize, opening_weekdays, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

CITIES = {
    "lisbon": "lisboa",
    "oporto": "porto",
    "portimao": "portimão",
    "vila nova de gaia porto": "vila nova de gaia",
}


def fetch_data(url):
    cache_file = Path(f"{cache_name(url)}.json")
    if not cache_file.exists():
        # print(f"Querying URL: {url}")
        result = requests.get(url).content.decode("utf-8")
        result = json.loads(result)
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


if __name__ == "__main__":
    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[amenity][name=Starbucks](area.p); );')["elements"]]

    continent_data_url = "https://www.starbucks.pt/api/v2/stores/?filter[coordinates][latitude]=39.681823&filter[coordinates][longitude]=-8.003540&filter[radius]=250"
    madeira_data_url = "https://www.starbucks.pt/api/v2/stores/?filter[coordinates][latitude]=32.771436&filter[coordinates][longitude]=-16.704712&filter[radius]=250"
    azores_data_url = "https://www.starbucks.pt/api/v2/stores/?filter[coordinates][latitude]=38.381039&filter[coordinates][longitude]=-28.020630&filter[radius]=250"
    new_data = [x["attributes"] for x in (fetch_data(continent_data_url)["data"] + fetch_data(madeira_data_url)["data"] + fetch_data(azores_data_url)["data"]) if x["attributes"]["address"]["countryCode"] == "PT"]

    for nd in new_data:
        public_id = nd["storeNumber"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = nd["coordinates"]

            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(coord["latitude"])
            d.data["lon"] = float(coord["longitude"])
            old_data.append(d)

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
                    "d": datetime.datetime.strptime(x["date"].split("T")[0], "%Y-%m-%d").date().weekday(),
                    "t": f"{x['openTime'][:5]}-{x['closeTime'][:5]}",
                }
                for x in nd["openHours"]
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

        phone = nd["phoneNumber"].lstrip("0")
        if len(phone) == 12 and phone.startswith("351"):
            phone = phone[3:]
        if phone:
            phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}" if len(phone) == 9 else "???"
        d["contact:phone"] = phone

        d["contact:website"] = "https://www.starbucks.pt/"
        d["contact:facebook"] = "StarbucksPortugal"
        d["contact:twitter"] = "starbucksPTG"
        d["contact:instagram"] = "starbucksptg"
        d["contact:tiktok"] = "starbucksportugal"
        d["contact:email"] = "starbucks@starbucks.pt"

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
            d["addr:*"] = "; ".join([x for x in (nd["address"]["streetAddressLine1"], nd["address"]["streetAddressLine2"], nd["address"]["streetAddressLine3"]) if x])

    for d in old_data:
        if d.kind == "new":
            continue
        ref = d[REF]
        if any(nd for nd in new_data if ref == nd["storeNumber"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Starbucks", REF, old_data)
