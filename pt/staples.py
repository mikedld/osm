#!/usr/bin/env python3

import itertools
import re

from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, write_diff


DATA_URL = "https://www.staples.pt/pt/pt/store-locator"

REF = "ref"

DAYS = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h(\d{2}) - (\d{2})h(\d{2})": r"\1:\2-\3:\4",
    "Encerrada": "off",
}
OFF_DAYS_MAPPING = {
    "1/jan": "Jan 01",
    "Domingo de Páscoa": "easter",
    "1/mai": "May 01",
    "23/jul": "Jul 23",
    "28/ago": "Aug 28",
    "25/dez": "Dec 25",
}
CITIES = {
    "2400-822": "Leiria",
    "2600-661": "Vila Franca de Xira",
    "2635-046": "Rio de Mouro",
    "2645-543": "Alcabideche",
    "2950-805": "Quinta do Anjo",
    "4520-000": "Santa Maria da Feira",
    "4560-221": "Marecos",
    "8400-618": "Parchal",
}


def fetch_data():
    def post_process(page):
        page = re.sub(r".*PointLocatorMap_js_items_points\s*=\s*\[(.*?)\];.*", r"[\1]", page, flags=re.DOTALL)
        return page

    return fetch_json_data(DATA_URL, post_process=post_process)


def schedule_time(v):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"^Staples"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = str(nd["point_id"])
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["coordX"]), float(nd["coordY"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = float(nd["coordX"])
            d.data["lon"] = float(nd["coordY"])
            old_data.append(d)
            new_node_id -= 1

        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "stationery"
        d["name"] = "Staples"
        d["brand"] = "Staples"
        d["brand:wikidata"] = "Q785943"
        d["brand:wikipedia"] = "pt:Staples Inc."
        d["branch"] = nd["name"]

        schedule_parts = nd["hours"].split("<br>")
        schedule = [x.xpath("./span/text()") for x in etree.fromstring(schedule_parts[-1], etree.HTMLParser()).xpath("//div")]
        holidays_hours = schedule_time(next(x for x in schedule if x[0] == "Feriados")[1])
        schedule = [
            {
                "d": DAYS.index(x[0]),
                "t": schedule_time(x[1]),
            }
            for x in schedule
            if x[0] != "Feriados"
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k,
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [(opening_weekdays(x["d"]), x["t"]) for x in sorted(schedule, key=lambda x: x["d"][0])]
        schedule = [(f"{x[0]},PH" if x[1] == holidays_hours else x[0], x[1]) for x in schedule]
        if off_days := next((x for x in schedule_parts[:-1] if "está encerrada" in x), None):
            off_days = [
                OFF_DAYS_MAPPING.get(x, f"<ERR:{x}>")
                for x in etree.fromstring(off_days, etree.HTMLParser()).xpath("//strong/text()")
            ]
            schedule.append((",".join(off_days), "off"))
        schedule = [" ".join(x) for x in schedule]
        d["opening_hours"] = "; ".join(schedule)

        d["contact:phone"] = f"+351 {nd['phone'][0:3]} {nd['phone'][3:6]} {nd['phone'][6:9]}"
        d["website"] = "https://www.staples.pt/"
        d["contact:facebook"] = "staplesportugal"
        d["contact:youtube"] = "https://www.youtube.com/@Staples757"
        d["contact:instagram"] = "staples.portugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/staplesportugal/"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        address = etree.fromstring(nd["address"], etree.HTMLParser()).xpath("//p//text()")
        postcode, city = address.pop(-1).split(" ", 1)
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, city)
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(address)

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["point_id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Staples", REF, old_data)
