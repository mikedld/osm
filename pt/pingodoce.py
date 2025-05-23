#!/usr/bin/env python3

import datetime
import html
import itertools
import json
import re
from pathlib import Path

import requests

from impl.common import BASE_DIR, BASE_NAME, DiffDict, cache_name, overpass_query, titleize, distance, opening_weekdays, gregorian_easter, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def fetch_data(url):
    cache_file = Path(f"{cache_name(url)}.json")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        result = requests.get(url).content.decode("utf-8")
        result = json.loads(result)
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


def schedule_time(v):
    opens_at = v.get("morningOpen", v.get("open"))
    closes_at = v.get("morningClose", v.get("close"))
    if opens_at == "closed":
        return "off"
    if opens_at == "00:00" and closes_at == "23:59":
        return "24/7"
    opens_at_2 = v.get("afternoonOpen")
    closes_at_2 = v.get("afternoonClose")
    return f"{opens_at}-{closes_at}" + (f",{opens_at_2}-{closes_at_2}" if opens_at_2 else "")


if __name__ == "__main__":
    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[shop][shop!=alcohol][shop!=florist][shop!=kiosk][~"^(name|brand)$"~"^Ping[ou] Doce"](area.p); );')["elements"]]

    data_url = "https://www.pingodoce.pt/wp-content/themes/pingodoce/ajax/pd-ajax.php?action=pd_stores_get_stores"
    new_data = fetch_data(data_url)["data"]["stores"]

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["lat"]), float(nd["long"])]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 250], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["long"])
            old_data.append(d)

        branch = html.unescape(nd["name"]).replace("PD&GO", "PD&Go").replace("  ", " ")
        is_pdgo = branch.lower().startswith("pd&go")
        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "convenience" if is_pdgo else "supermarket"
        d["name"] = "Pingo Doce" + (" & Go" if is_pdgo else "")
        d["brand"] = "Pingo Doce"
        d["brand:wikidata"] = "Q1575057"
        d["brand:wikipedia"] = "pt:Pingo Doce"
        d["branch"] = branch

        if nd["in_maintenance"]:
            d["opening_hours"] = "Mo-Su off \"closed for maintenance\""
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
                    "t": k
                }
                for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
            ]
            schedule = [
                f"{opening_weekdays(x['d'])} {x['t']}"
                for x in sorted(schedule, key=lambda x: x["d"][0])
            ]
            if exs := nd["schedules"]["exceptions"]:
                for k, v in exs.items():
                    dt = datetime.datetime.fromisoformat(k)
                    if dt.date() == gregorian_easter(dt.year):
                        schedule.append(f"easter {schedule_time(v)}")
                        break
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = re.sub(r"[^0-9]+", "", nd["contact"])
        phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        if phone[5:6] == "9":
            d["contact:mobile"] = phone
            tags_to_reset.add("contact:phone")
        else:
            d["contact:phone"] = phone
            tags_to_reset.add("contact:mobile")
        d["contact:website"] = nd["permalink"]
        d["contact:facebook"] = "pingodoce"
        d["contact:youtube"] = "pingodocept"
        d["contact:instagram"] = "pingodoce"
        d["contact:linkedin"] = "https://www.linkedin.com/company/pingo-doce"
        d["contact:tiktok"] = "pingodoce"

        tags_to_reset.update({"phone", "mobile", "website"})

        # if d["source:addr"] != "survey":
        #     d["source:addr"] = "website"
        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = nd["postal_code"].split(" ", 1)
        # d["addr:city"] = postcode[1]
        d["addr:postcode"] = postcode[0]
        # d["addr:street"] = nd["address"]
        # d["addr:housenumber"] = nd["number"]
        if d.kind == "new":
            d["x-dld-addr"] = f"{nd['address']}; {nd['number']}"

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Pingo Doce", REF, old_data, osm=True)
