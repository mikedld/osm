#!/usr/bin/env python3

import re
from multiprocessing import Pool

from lxml import etree
from unidecode import unidecode

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.recheio.pt/portal/pt-PT/webruntime/api/apex/execute"

REF = "ref"

SCHEDULE_DAYS = (
    ("RCH_WeekHours__c", "Mo-Fr"),
    ("RCH_SaturdayHours__c", "Sa"),
    ("RCH_SundayHours__c", "Su"),
    # ("RCH_Holidays__c", "PH"),  # noqa: ERA001
)
HOLIDAYS = {
    r"\b(\d{2}) de junho": r"Jun \1",
    r"\b(\d{1}) de junho": r"Jun 0\1",
    r"corpo de deus": "easter +60 days",
    r"dia de portugal": "Jun 10",
    r"feriado muni(ci)?pal": "PH",
}
PHONES = (
    ("RCH_PhoneNumber__c", "geral"),
    ("RCH_PhoneNumber2__c", "frutaria"),
    ("RCH_PhoneNumber3__c", "peixaria"),
    ("RCH_PhoneNumber4__c", "talho"),
)
CITIES = {
    "2785-190": "Abóboda",
    "2855-574": "Corroios",
}


def fetch_level1_data():
    params = {
        "language": "pt-PT",
        "asGuest": "true",
        "htmlEncode": "false",
    }
    payload = {
        "namespace": "",
        "classname": "@udd/01pQD000000MOZF",
        "method": "getStoresByRegion",
        "isContinuation": False,
        "cacheable": False,
    }
    result = fetch_json_data(DATA_URL, params=params, json=payload)
    result = [x for k, v in result["returnValue"].items() for x in v]
    return result


def fetch_level2_data(data):
    params = {
        "language": "pt-PT",
        "asGuest": "true",
        "htmlEncode": "false",
    }
    payload = {
        "namespace": "",
        "classname": "@udd/01pQD000000MOZF",
        "method": "getStoreById",
        "isContinuation": False,
        "cacheable": False,
        "params": {
            "id": data["Id"],
        },
    }
    result = fetch_json_data(DATA_URL, params=params, json=payload)
    if not result.get("returnValue", {}):
        print(data["Id"], result)
    return {
        **data,
        **result.get("returnValue", {}),
    }


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r"\W", " ", e)
    e = e.strip()
    e = re.sub(r"\s+", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"recheio",i](area.country);')]

    for nd in new_data:
        public_id = nd["RCH_ExternalId__c"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["RCH_LatitudeLongitude__c"]["latitude"], nd["RCH_LatitudeLongitude__c"]["longitude"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["RCH_LatitudeLongitude__c"]["latitude"]
            d.data["lon"] = nd["RCH_LatitudeLongitude__c"]["longitude"]
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "wholesale"
        # d["wholesale"] = "supermarket"  # noqa: ERA001
        d["name"] = "Recheio"
        d["brand"] = "Recheio"
        d["brand:wikidata"] = "Q7302409"
        d["brand:wikipedia"] = "en:Recheio"
        d["branch"] = nd["Name"]

        schedule = []
        for key, days in SCHEDULE_DAYS:
            value = re.sub(
                r"[\s\uFEFF]+", " ", ";".join(etree.fromstring(nd[key], etree.HTMLParser()).xpath("//text()")), flags=re.DOTALL
            )
            value = value.split(":", 1)[1]
            value = re.sub(r"\s*;(\s*;)*\s*", ";", value)
            value = re.sub(r"^[\s;]+|[\s;]+$", "", value)
            value = re.sub(r"\s*;?\s*-\s*;?\s*", "-", value)
            value = re.sub(r"\s*;?\s*\(\s*;?\s*", " (", value)
            value = re.sub(r"\)?[;,](?=\d+:\d+)", ");", value)
            value = re.sub(r"(?<!\));", ",", value)
            value = re.sub(r"\b(\d:\d{2})\b", r"0\1", value)
            value = value.split(";")
            for v in value:
                if m := re.fullmatch(r"(.+) \((.+)\)", v):
                    v = [m[1], [x.lower().strip() for x in m[2].split(",")]]
                else:
                    v = [v, []]
                if len(v[1]) == 1 and (m := re.fullmatch(r"encerra das (\d{2}:\d{2}) às (\d{2}:\d{2})", v[1][0])):
                    outer = v[0].split("-")
                    inner = [m[1], m[2]]
                    v = [f"{outer[0]}-{inner[0]},{inner[1]}-{outer[1]}", []]
                if v[0] == "Encerrado":
                    v[0] = "off"
                if schedule and schedule[-1][1] == v[0]:
                    schedule[-1][0] += f",{days}"
                    if schedule[-1][0] == "Mo-Fr,Sa":
                        schedule[-1][0] = "Mo-Sa"
                    elif schedule[-1][0] == "Mo-Sa,Su":
                        schedule[-1][0] = "Mo-Su"
                else:
                    schedule.append([days, v[0]])
        if schedule:
            d["opening_hours"] = "; ".join([" ".join(x) for x in schedule])
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phones = []
        for key, comment in PHONES:
            phone = nd.get(key, "").replace(" ", "")
            if phone:
                phones.append(f'+351 {phone[0:3]} {phone[3:6]} {phone[6:9]} "{comment}"')
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["contact:email"] = nd["RCH_ManagerEmail__c"]
        d["website"] = f"https://www.recheio.pt/portal/pt-PT/store-locator/detail?id={nd['Id']}"
        d["contact:facebook"] = "Recheio.pt"
        d["contact:youtube"] = "@recheiopt"
        d["contact:instagram"] = "recheiopt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/recheiosa/"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["RCH_Address__c"]
        postcode = address["postalCode"]
        if len(postcode) == 4:
            if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                postcode = d["addr:postcode"]
            else:
                postcode += "-000"
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, titleize(address["city"]))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address["street"].replace("\n", "; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["RCH_ExternalId__c"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Recheio", REF, old_data, osm=True)
