#!/usr/bin/env python3

import html
import itertools
import json
import re
import uuid
from multiprocessing import Pool
from urllib.parse import urljoin, urlsplit

from impl.common import DiffDict, fetch_json_data, fetch_html_data, overpass_query, titleize, distance, opening_weekdays, lookup_postcode, write_diff


DATA_URL = "https://www.mcdonalds.pt/restaurantes"

REF = "ref"

BRANCHES = {
    "Aeroporto de Lisboa - T1": "Aeroporto de Lisboa - Terminal 1",
    "Aeroporto de Lisboa - T2": "Aeroporto de Lisboa - Terminal 2",
    "Coimbra Forum": "Coimbra - Fórum",
    "D Carlos I": "Dom Carlos I",
    "Vila Real Nosso Shopping": "Vila Real - Nosso Shopping",
}
DAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h(\d{2}) às (\d{2})h(\d{2})": r"\1:\2-\3:\4",
    r"(\d{1})h(\d{2}) às (\d{2})h(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})h(\d{2}) às (\d{1})h(\d{2})": r"\1:\2-0\3:\4",
    r"(\d{1})h(\d{2}) às (\d{1})h(\d{2})": r"0\1:\2-0\3:\4",
}
CITIES = {
    "2580-491": "Carregado",
    "2660-017": "Santo António dos Cavaleiros",
    "2770-129": "Paço de Arcos",
    "2785-784": "Matos Cheirinhos",
    "2820-287": "Charneca de Caparica",
    "2830-170": "Santo André",
    "2840-000": "Torre da Marinha",
    "2970-841": "Venda Nova",
    "3830-243": "Ílhavo",
    "4405-520": "Vila Nova de Gaia",
    "4425-500": "Ermesinde",
    "4435-000": "Rio Tinto",
    "4450-718": "Leça da Palmeira",
    "4450-820": "Leça da Palmeira",
    "4464-001": "Senhora da Hora",
    "4730-709": "Vila Verde",
    "9600-170": "Ribeira Grande",
}


def fetch_level1_data():
    def post_process(page):
        return re.sub(r"^.*var restaurantsJson\s*=\s*'\[(.+)\]'.*$", r"[\1]", page, flags=re.S)

    result = fetch_json_data(DATA_URL, post_process=post_process)
    return [
        {
            **x,
            "Url": urljoin(DATA_URL, x["Url"]),
        }
        for x in result
    ]


def fetch_level2_data(data):
    result_tree = fetch_html_data(data["Url"])
    return {
        **data,
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, "mcdonalds:" + urlsplit(data["Url"]).path.split("/")[2])),
        "schedules": {
            "".join(el.xpath('h6/text()')).strip(): {
                re.sub(r"V[ée]spera\s+(de\s+)?[Ff]eriado", r"Véspera Feriado", "".join(el2.xpath('cite/text()')).strip()): "".join(el2.xpath('span/text()')).strip()
                for el2 in el.xpath("ul/li")
            }
            for el in result_tree.xpath("//div[contains(@class, 'restaurantSchedule__service')]")
        },
        **json.loads(re.sub(r"[\r\n]", "", "".join(result_tree.xpath("//script[@type='application/ld+json']/text()")))),
    }


def schedule_time(v):
    sa = v
    sb = "<ERR>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def opening_hours(data, title):
    schedule = {k: v for k, v in data["schedules"].get(title, {}).items() if k in DAYS}
    if not schedule:
        return None

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
    if exs := {k: v for k, v in data["schedules"].get(title, {}).items() if k not in DAYS}:
        for k, v in exs.items():
            if k == "Véspera Feriado":
                t = schedule_time(v)
                if len(schedule) != 1 or schedule[0] != f"Mo-Su {t}":
                    schedule.append(f"PH -1 day {t}")
            else:
                schedule.append("<ERR>")
    schedule = "; ".join(schedule)
    if schedule == "Mo-Su 00:00-00:00":
        schedule = "24/7"
    return schedule


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[amenity][amenity!=charging_station][amenity!=bicycle_rental][amenity!=social_facility][amenity!=parking][name~"McDonald"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["Lat"], nd["Lng"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["Lat"]
            d.data["lon"] = nd["Lng"]
            old_data.append(d)
            new_node_id -= 1

        tags_to_reset = set()

        d[REF] = public_id
        d["amenity"] = "fast_food"
        d["cuisine"] = "burger"
        d["name"] = "McDonald's"
        d["brand"] = "McDonald's"
        d["brand:wikidata"] = "Q38076"
        d["brand:wikipedia"] = "pt:McDonald's"
        d["branch"] = BRANCHES.get(nd["Name"].strip(), nd["Name"].strip())
        d["takeaway"] = "yes"

        if schedule := opening_hours(nd, "Restaurante"):
            d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"
        if schedule := opening_hours(nd, "McDrive"):
            d["drive_through"] = "yes"
            d["opening_hours:drive_through"] = schedule
        else:
            if d["drive_through"] != "no":
                tags_to_reset.add("drive_through")
            tags_to_reset.add("opening_hours:drive_through")

        phone = re.sub(r"\s+", "", nd["contactPoint"]["telephone"])
        if len(phone) == 13:
            phone = f"+351 {phone[4:7]} {phone[7:10]} {phone[10:13]}"
            if phone[5:6] == "9":
                d["contact:mobile"] = phone
            else:
                tags_to_reset.add("contact:mobile")
            if phone[5:6] != "9":
                d["contact:phone"] = phone
            else:
                tags_to_reset.add("contact:phone")
        d["contact:website"] = nd["Url"]
        d["contact:facebook"] = "McDonaldsPortugal"
        d["contact:youtube"] = "https://www.youtube.com/McDonaldsPortugal"
        d["contact:instagram"] = "mcdonaldsportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/mcdonald's-corporation/"
        d["contact:tiktok"] = "mcdonalds.pt"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["address"]
        postcode = re.sub(r"[^0-9]+", "", address["postalCode"])
        if len(postcode) == 7 and postcode.endswith("000"):
            postcode = postcode[:4]
        if len(postcode) == 4:
            postcode += d["addr:postcode"][5:] if len(d["addr:postcode"]) == 8 else "000"
        if len(postcode) == 7:
            postcode = f"{postcode[0:4]}-{postcode[4:]}"
            d["addr:postcode"] = postcode
        elif postcode:
            postcode = None
            d["addr:postcode"] = "<ERR>"
        city = titleize(nd["City"].strip())
        if not city and postcode:
            location = lookup_postcode(postcode)
            if not location and "-" in postcode:
                location = lookup_postcode(postcode.split("-", 1)[0])
            if location:
                city = titleize(location[1])
        city = CITIES.get(postcode, city)
        d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = html.unescape(address["streetAddress"]).strip("; ").replace("–", "-")

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

    write_diff("McDonald's", REF, old_data, osm=True)
