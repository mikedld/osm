#!/usr/bin/env python3

import datetime
import html
import itertools
import json
import re

from impl.common import (
    BASE_DIR,
    BASE_NAME,
    LISBON_TZ,
    DiffDict,
    distance,
    fetch_json_data,
    gregorian_easter,
    opening_weekdays,
    overpass_query,
    write_diff,
)


DATA_URL = "https://www.pingodoce.pt/wp-content/themes/pingodoce/ajax/pd-ajax.php?action=pd_stores_get_stores"

REF = "ref"

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def fetch_data():
    return fetch_json_data(DATA_URL)["data"]["stores"]


def fixup_time(v):
    if v and len(v) == 5 and v[2] != ":":
        v = f"{v[:2]}:{v[3:]}"
    return v


def schedule_time(v):
    if v.get("closed"):
        return "off"
    opens_at = fixup_time(v.get("morningOpen", v.get("open")))
    closes_at = fixup_time(v.get("morningClose", v.get("close")))
    if opens_at == "closed":
        return "off"
    if opens_at == "00:00" and closes_at == "23:59":
        return "24/7"
    opens_at_2 = fixup_time(v.get("afternoonOpen"))
    closes_at_2 = fixup_time(v.get("afternoonClose"))
    return f"{opens_at}-{closes_at}" + (f",{opens_at_2}-{closes_at_2}" if opens_at_2 else "")


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            'nwr[shop][shop!=alcohol][shop!=florist][shop!=kiosk][~"^(name|brand)$"~"^Ping[ou] Doce"](area.country);'
        )
    ]

    custom_ohs = {}
    custom_ohs_file = BASE_DIR / f"{BASE_NAME}-custom-ohs.json"
    if custom_ohs_file.exists():
        custom_ohs = json.loads(custom_ohs_file.read_text())

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["lat"]), float(nd["long"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["long"])
            old_data.append(d)

        branch = (
            re.sub(r"^(pd&go|pingo doce express)\s+(-\s+)?", "", html.unescape(nd["name"]), flags=re.IGNORECASE)
            .replace("  ", " ")
            .strip()
        )
        is_pdgo = html.unescape(nd["name"]).lower().startswith("pd&go")
        is_pdex = html.unescape(nd["name"]).lower().startswith("pingo doce express")
        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "convenience" if is_pdgo else "supermarket"
        d["name"] = "Pingo Doce" + (" & Go" if is_pdgo else (" Express" if is_pdex else ""))
        d["brand"] = "Pingo Doce"
        d["brand:wikidata"] = "Q1575057"
        d["brand:wikipedia"] = "pt:Pingo Doce"
        d["branch"] = branch

        if custom_oh := nd["schedules"]["exceptions"]:
            if public_id not in custom_ohs:
                custom_ohs[public_id] = {}
            custom_ohs[public_id].update(**custom_oh)

        if nd["in_maintenance"] and nd["in_maintenance"] != "0":
            d["opening_hours"] = 'Mo-Su off "closed for maintenance"'
            if "opening_hours" in d.old_tags:
                d["source:opening_hours"] = "website"
        elif schedule := nd["schedules"]["full"]:
            schedule = [
                {
                    "d": DAYS.index(k),
                    "t": schedule_time(v),
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
            if exs := custom_ohs.get(public_id):
                today = datetime.datetime.now(datetime.UTC).astimezone(LISBON_TZ)
                for k, v in exs.items():
                    dt = datetime.datetime.fromisoformat(k)
                    if dt.year != today.year:
                        continue
                    if dt.date() == gregorian_easter(dt.year):
                        schedule.append(f"easter {schedule_time(v)}")
                        break
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = re.sub(r"[^0-9]+", "", nd["contact"])
        d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        d["website"] = nd["permalink"] if "://" in nd["permalink"] else f"https://www.pingodoce.pt/lojas/{nd['permalink']}/"
        d["contact:facebook"] = "pingodoce"
        d["contact:youtube"] = "pingodocept"
        d["contact:instagram"] = "pingodoce"
        d["contact:linkedin"] = "https://www.linkedin.com/company/pingo-doce"
        d["contact:tiktok"] = "pingodoce"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = nd["postal_code"].split(" ", 1)
        # d["addr:city"] = postcode[1]  # noqa: ERA001
        d["addr:postcode"] = postcode[0]
        # d["addr:street"] = nd["address"]  # noqa: ERA001
        # d["addr:housenumber"] = nd["number"]  # noqa: ERA001
        if d.kind == "new":
            d["x-dld-addr"] = f"{nd['address']}; {nd['number']}"

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    custom_ohs_file.write_text(json.dumps(custom_ohs))

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Pingo Doce", REF, old_data, osm=True)
