#!/usr/bin/env python3

import itertools
import re

from impl.common import DiffDict, fetch_json_data, overpass_query, distance, opening_weekdays, write_diff


DATA_URL = "https://www.audika.pt/api/clinics/getclinics/{347A23B3-5B62-480A-984B-F51C53E516E8}"

REF = "ref"

DAYS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]


def fetch_data():
    return fetch_json_data(DATA_URL)


def schedule_time(v):
    ss = []
    for m in re.finditer(r"(\b\d+[:.]\d+)h?(\s*[ap]m)?\s*[-–]\s*(\b\d+[:.]\d+)h?(\s*[ap]m)?", v.lower()):
        x = [m[1].strip().replace(".", ":").rjust(5, "0"), (m[2] or "").strip(), m[3].strip().replace(".", ":").rjust(5, "0"), (m[4] or "").strip()]
        x[1] = x[1] or x[3]
        if x[0].startswith("12"):
            if x[1] == "am":
                x[0] = f"00{x[0][2]}"
        else:
            if x[1] == "pm":
                x[0] = f"{int(x[0].split(':')[0]) + 12}:{x[0].split(':')[1]}"
        if x[2].startswith("12"):
            if x[3] == "am":
                x[2] = f"00{x[2][2]}"
        else:
            if x[3] == "pm":
                x[2] = f"{int(x[2].split(':')[0]) + 12}:{x[2].split(':')[1]}"
        ss.append(f"{x[0]}-{x[2]}")
    if not ss and v.lower().strip() in ("", "encerrado", "fechado"):
        ss.append("off")
    return ",".join(ss)


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"Audika|Ac[uú]stica M[eé]dica"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["ItemId"].lower().strip("}{")
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["Latitude"]), float(nd["Longitude"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = float(nd["Latitude"])
            d.data["lon"] = float(nd["Longitude"])
            old_data.append(d)
            new_node_id -= 1

        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "hearing_aids"
        d["name"] = "Audika"
        d["brand"] = "Audika"
        d["brand:wikidata"] = "Q2870745"
        d["brand:wikipedia"] = "fr:Audika"
        d["branch"] = nd["Name"]

        schedule = [
            {
                "d": DAYS.index(x["DayName"].lower()),
                "t": schedule_time(x["DayHours"]),
            }
            for x in nd["BusinessHours"]
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
        schedule.sort(key=lambda x: x.endswith(" off"))
        schedule = "; ".join(schedule)
        d["opening_hours"] = schedule
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        phone = nd["PhoneNumber"].replace(" ", "")
        if len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["Email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.update({"contact:email"})
        d["website"] = f"https://www.audika.pt{nd['ItemUrl']}"
        d["contact:facebook"] = "audika.pt"
        d["contact:youtube"] = "https://www.youtube.com/@audika_pt"
        d["contact:instagram"] = "audika_pt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/audika-pt/"
        d["contact:tiktok"] = ""

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = [x.strip(", ") for x in re.split(r"(?=\b\d{4}-\d{3}\b)", nd["Address"])] + [""]
        postcode, city = [x.strip(", ") for x in f"{address[1]} ".split(" ", 1)]
        if postcode:
            d["addr:postcode"] = postcode
        if city:
            d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address[0]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["ItemId"].lower().strip("}{")):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Audika", REF, old_data, osm=True)
