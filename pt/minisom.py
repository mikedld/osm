#!/usr/bin/env python3

import datetime
import itertools
import re
from multiprocessing import Pool

from unidecode import unidecode

from impl.common import BASE_NAME, DiffDict, fetch_json_data, overpass_query, distance, opening_weekdays, write_diff
from impl.config import CONFIG


XCONFIG = CONFIG[BASE_NAME]

LEVEL1_DATA_URL = "https://emea.api.amplifoninternal.com/b2c-emea/store-locator/v2/getStores"
LEVEL2_DATA_URL = "https://emea.api.amplifoninternal.com/b2c-emea/store-locator/v2/getOpeningTimesByStore"

REF = "ref"


def fetch_level1_data():
    headers = {
        "x-api-key": XCONFIG["api_key"],
    }
    payload = {
        "countryCode": "004",
        "latitude": "38.772151",
        "longitude": "-9.1179465",
        "locale": "pt_PT",
        "limit": 1000,
        "radius": 100000,
    }
    return fetch_json_data(LEVEL1_DATA_URL, headers=headers, json=payload)


def fetch_level2_data(data):
    headers = {
        "x-api-key": XCONFIG["api_key"],
    }
    today = datetime.date.today()
    payload = {
        "countryCode": data["country"],
        "type": data["type"],
        "shopNumber": data["shopNumber"],
        "locale": data["locale"],
        "startDate": today.isoformat(),
        "endDate": (today + datetime.timedelta(days=7)).isoformat(),
    }
    result = fetch_json_data(LEVEL2_DATA_URL, headers=headers, json=payload)
    return {
        **data,
        **result,
    }


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r'\W', " ", e)
    e = e.strip()
    e = re.sub(r"\s+", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"Minisom|Amplifon"](area.country);')]

    for nd in new_data:
        public_id = nd["shopNumber"]
        branch = nd["shopName"].removeprefix("Minisom").strip()
        if m := re.fullmatch(r"(.+?)(?:,|\s+-\s+(?=Av\.)|\s+(?=\())(.+)", branch):
            branch = m[1].strip()
            location = m[2].strip()
        else:
            location = None
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
            d.data["id"] = f"-{public_id.strip('c')}"
            d.data["lat"] = nd["latitude"]
            d.data["lon"] = nd["longitude"]
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "hearing_aids"
        d["name"] = "Minisom"
        d["brand"] = "Amplifon"
        d["brand:wikidata"] = "Q477222"
        d["brand:wikipedia"] = "en:Amplifon"
        d["branch"] = branch

        schedule = [
            {
                "d": k - 1,
                "t": ",".join([
                    f"{x['startTime']}-{x['endTime']}" if x["openingStatus"] != 0 else "off"
                    for x in g
                ]),
            }
            for k, g in itertools.groupby(nd["openingTimes"], key=lambda x: x["dayOfWeek"])
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k if k == "off" or nd["type"] != "P" else '"por marcação"'
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [
            f"{opening_weekdays(x['d'])} {x['t']}"
            for x in sorted(schedule, key=lambda x: x["d"][0])
        ]
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        mobiles = []
        phones = []
        contacts = [re.sub(r"\D+", "", x) for x in (nd["phoneNumber1"], nd["phoneNumber2"]) if x]
        if not contacts:
            contacts = ["800100210"]
        for phone in contacts:
            if phone == "80010210":
                phone = "800100210"
            if len(phone) == 9:
                phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
                if phone[5:6] == "9":
                    mobiles.append(phone)
                else:
                    phones.append(phone)
        if mobiles:
            d["contact:mobile"] = ";".join(mobiles)
        else:
            tags_to_reset.add("contact:mobile")
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["contact:website"] = f"https://www.minisom.pt/centros-minisom/aparelhos-auditivos-{get_url_part(nd['city'])}/minisom-{get_url_part(branch)}-{nd['type'].lower()}{public_id}"
        d["contact:facebook"] = "Minisom"
        d["contact:youtube"] = "https://www.youtube.com/@Minisom_Portugal"
        d["contact:instagram"] = "minisom_amplifon"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        d["addr:postcode"] = nd["cap"].strip()
        d["addr:city"] = nd["city"].strip()
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["address"] + (f"; {location}" if location else "")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["shopNumber"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Minisom", REF, old_data, osm=True)
