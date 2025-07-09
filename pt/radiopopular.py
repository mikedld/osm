#!/usr/bin/env python3

import itertools
import json
import re

from unidecode import unidecode

from impl.common import BASE_NAME, DiffDict, fetch_html_data, overpass_query, titleize, distance, opening_weekdays, write_diff
from impl.config import CONFIG


DATA_URL = "https://www.radiopopular.pt/lojas/"

REF = "ref"

SCHEDULE_DAYS = {
    "Domingo": "Su",
    "Domingo a 5ª": "Su-Th",
    "Domingo a Quinta": "Su-Th",
    "Domingo e Feriados": "Su,PH",
    "Domingos e Feriados": "Su,PH",
    "Segunda a Sábado": "Mo-Sa",
    "Sexta e Sabado": "Fr,Sa",
    "Sexta, Sábado e Vésperas de Feriados": "Fr,Sa,PH -1 days",
    "Todos os dias": "Mo-Su",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h\s*-\s*(\d{2})h": r"0\1:00-\2:00",
    r"(\d{2})h\s*-\s*(\d{2})h": r"\1:00-\2:00",
    r"(\d{2})h\s*-\s*(\d{2}):(\d{2})h": r"\1:00-\2:\3",
    r"(\d{2})[:h](\d{2})h?\s*(?:-|às)\s*(\d{2})[:h](\d{2})h?": r"\1:\2-\3:\4",
}
CITIES = {
    "2400-441": "Leiria",
    "2636-901": "Rio de Mouro",
    "3045-504": "Taveiro",
    "3080-847": "Figueira da Foz",
    "4400-062": "Vila Nova de Gaia",
    "4415-307": "Carvalhos",
    "4445-416": "Ermesinde",
    "4770-282": "Lagoa",
    "4950-852": "Monção",
    "6200-251": "Covilhã",
    "8365-307": "Alcantarilha",
    "9125-067": "Caniço",
}


def fetch_data():
    result_tree = fetch_html_data(DATA_URL)
    result = [
        json.loads(x)
        for x in result_tree.xpath("//@data-rp-info")
    ]
    return result


def schedule_time(v):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r'\W', " ", e)
    e = e.strip()
    e = re.sub(r"\s+", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"R[aá]dio.*Popular"](area.country);')]

    for nd in new_data:
        public_id = str(nd["id"])
        #branch = titleize(nd["name"].strip())
        #is_ex = branch.endswith("Express")
        #is_con = branch.endswith("Connect")
        #branch = re.sub(r"\s+(Express|Connect)$", "", branch)
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["latitude"]), float(nd["longitude"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["latitude"])
            d.data["lon"] = float(nd["longitude"])
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "electronics"
        d["name"] = "Radio Popular"
        d["brand"] = "Radio Popular"
        d["brand:wikidata"] = "Q11889669"
        d["brand:wikipedia"] = "pt:Rádio Popular (empresa)"
        d["branch"] = nd["name"]

        schedule = [
            [y.strip() for y in x.replace("<br>", "").strip().split(":", 1)]
            for x in re.sub(r"(?<=\D)(?<!\dh)\b\s*-\s*(?=\d)", ": ", nd["schedule"].strip()).split("\n")
        ]
        schedule = [
            " ".join([SCHEDULE_DAYS.get(x[0]), schedule_time(x[1])])
            for x in schedule
        ]
        d["opening_hours"] = "; ".join(schedule)
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        d["contact:phone"] = "+351 220 403 040"
        d["contact:website"] = f"https://www.radiopopular.pt/loja/{get_url_part(nd['name'])}/"
        d["contact:facebook"] = "RadioPopular.PT"
        d["contact:twitter"] = "radiopopularPT"
        d["contact:youtube"] = "https://www.youtube.com/@RadioPopular"
        d["contact:pinterest"] = "radiopopular"
        d["contact:instagram"] = "radiopopular"
        d["contact:tiktok"] = "radiopopular"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode, city = nd["postalcode"].split(" ", 1)
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, city)
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["address"].replace("<br>", "").replace("\n", "; ").strip()

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == str(nd["id"])):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Radio Popular", REF, old_data, osm=True)
