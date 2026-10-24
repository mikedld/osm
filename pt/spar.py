#!/usr/bin/env python3

import itertools
import json
import re
from itertools import dropwhile, islice, takewhile
from multiprocessing import Pool

from impl.common import (
    DiffDict,
    distance,
    fetch_html_data,
    fetch_json_data,
    format_phonenumber,
    opening_weekdays,
    overpass_query,
    write_diff,
)


LEVEL1_DATA_URL = "https://www.spar.pt/loja/resumo"
LEVEL2_DATA_URL = "https://www.spar.pt/loja/detalhe/{id}/"


REF = "ref"

CONTACT_GROUPS = {
    "Telefone:",
    "Email:",
    "Aderente ao Folheto",
}
SCHEDULE_DAYS = {
    "segunda a sexta": [0, 1, 2, 3, 4],
    "sábado": [5],
    "domingo": [6],
    "feriados": [7],
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})[:.](\d{2})\s*-\s*(\d{2})[:.](\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})[:.](\d{2})\s*-\s*(\d{2})[:.](\d{2})": r"\1:\2-\3:\4",
    r"encerrad[ao]": r"off",
}
BRANCHES = {
    "305": "Furnas I",
    "307": "Ribeira Quente",
}
CITIES = {
    "2705-737": "São João das Lampas",
    "2775-366": "Parede",
    "4640-475": "Santa Marinha do Zêzere",
    "6100-266": "Cernache do Bonjardim",
    "7565-258": "Ermidas-Sado",
    "8400-536": "Carvoeiro",
    "8600-165": "Luz",
    "8800-591": "Cabanas de Tavira",
    "9625-511": "São Brás",
    "9630-141": "Nordeste",
    "9675-040": "Furnas",
    "9675-055": "Furnas",
    "9675-174": "Ribeira Quente",
    "9940-365": "São Roque do Pico",
}


def fetch_level1_data():
    def post_process(page):
        return re.sub(r"^.*var\s+lojasData\s*=\s*JSON.parse\('([^']*)'\);.*$", r'"\1"', page, flags=re.DOTALL)

    return json.loads(fetch_json_data(LEVEL1_DATA_URL, post_process=post_process))


def extract_contact_info(v, group):
    return list(takewhile(lambda x: x not in CONTACT_GROUPS, islice(dropwhile(lambda x: x != group, v), 1, None)))


def fetch_level2_data(data):
    url = LEVEL2_DATA_URL.format(id=data["id"])
    result_tree = fetch_html_data(url)
    details_el = result_tree.xpath("//*[@class='loja-ficha-info']")[0]
    return {
        **data,
        "url": url,
        "schedule": {
            el.xpath("(./dt)[1]/text()")[0]: el.xpath("(./dd)[1]/text()")[0]
            for el in details_el.xpath(".//*[@class='loja-horario-linha']")
        },
        "phones": [x.removeprefix("tel:") for x in details_el.xpath(".//a[starts-with(@href, 'tel:')]/@href")],
        "emails": [x.removeprefix("mailto:") for x in details_el.xpath(".//a[starts-with(@href, 'mailto:')]/@href")],
    }


def schedule_time(v, mapping):
    if not isinstance(v, str):
        return ",".join(schedule_time(x, mapping) for x in v)
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query(r'nwr[shop][shop!=newsagent][name~"\\bspar\\b",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = str(nd["id"])
        branch = re.sub(r"^SPAR\s+", "", nd["nome"], flags=re.IGNORECASE)
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float((nd["latitude"] or "38.306893").rstrip(",")), float(nd["longitude"] or "-17.050891")]
        if coord[1] > 0:
            coord[1] = -coord[1]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = d["shop"] or "supermarket"
        d["name"] = "Spar"
        d["brand"] = "Spar"
        d["brand:wikidata"] = "Q610492"
        d["brand:wikipedia"] = "pt:SPAR"
        d["branch"] = BRANCHES.get(public_id, branch)
        if nd["tipo"] == "ader":
            d["operator"] = "Spar"
        elif nd["tipo"] == "lp" and d["operator"].lower() == "spar":
            tags_to_reset.add("operator")

        schedule = nd["schedule"]
        if schedule:
            schedule = [
                {
                    "d": d,
                    "t": schedule_time(re.split(r" [|/] ", v.lower()), SCHEDULE_HOURS_MAPPING),
                }
                for k, v in schedule.items()
                for d in SCHEDULE_DAYS[k.lower()]
            ]
            schedule = [
                {
                    "d": sorted([x["d"] for x in g]),
                    "t": k,
                }
                for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
            ]
            schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
            schedule = "; ".join(schedule)
            d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        if phones := [format_phonenumber(x) for x in nd["phones"]]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if emails := nd["emails"]:
            d["contact:email"] = ";".join(emails)
        else:
            tags_to_reset.add("contact:email")
        d["website"] = nd["url"]
        d["contact:facebook"] = "sparportugal"
        d["contact:instagram"] = "sparportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/spar-portugal/"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode, city = (nd["codpostal"] + " ").split(" ", 1)
        if postcode:
            d["addr:postcode"] = postcode
        if city := city.strip():
            d["addr:city"] = CITIES.get(postcode, city)
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["rua"]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Spar", REF, old_data, osm=True)
