#!/usr/bin/env python3

import itertools
import re
from json import loads as json_loads

from lxml import etree
from shapely.geometry import Point

from impl.common import (
    DiffDict,
    country_polygon,
    distance,
    fetch_html_data,
    format_phonenumber,
    overpass_query,
    titleize,
    write_diff,
)


DATA_URL = "https://espacocasa.com/lojas"

REF = "ref"

BRANCHES = {
    "baixa banheira": "Baixa da Banheira",
    "bragrança": "Bragança",
    "caldas da rainhas": "Caldas da Rainha",
    "cova piedade": "Cova da Piedade",
    "funchal cancela": "Funchal - Cancela",
    "paços ferreira": "Paços de Ferreira",
    "são joão madeira": "São João da Madeira",
    "ubbo": "UBBO",
}
SCHEDULE_DAYS_MAPPING = {
    r"(2ª|segunda) [aà] sab[aá]do": "Mo-Sa",
    r"domingo a 5ª": "Su-Th",
    r"domingos?": "Su",
    r"domingos? e feriados": "Su,PH",
    r"sexta, sabado e vespera feriado": "Fr,Sa,PH - 1 days",
    r"todos os dias": "Mo-Su",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h às (\d{2})h": r"0\1:00-\2:00",
    r"(\d{1})h(\d{2}) às (\d{2})h(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})h(?:[- ]| às | - )(\d{2})h?": r"\1:00-\2:00",
    r"encerra": r"off",
}


def fetch_data():
    result_tree = fetch_html_data(DATA_URL)
    settings = json_loads(result_tree.xpath("(//script[@data-drupal-selector='drupal-settings-json'])[1]/text()")[0])
    extra = {
        "".join(el.xpath("./div[contains(@class, 'views-field-title')]//text()")).strip(): {
            "address": "\n".join(el.xpath("./div[contains(@class, 'views-field-body')]//text()"))
            .strip()
            .split("\n")[0]
            .strip(),
            "phones": [
                x.removeprefix("tel:").strip(" \t\x02")
                for x in el.xpath("./div[contains(@class, 'views-field-body')]//a[contains(@href, 'tel:')]/@href")
            ],
            "schedule": " ".join(el.xpath("./div[contains(@class, 'views-field-more-information')]//text()")).strip(),
        }
        for el in result_tree.xpath("//div[./div[contains(@class, 'views-field-title')]]")
    }
    result = [
        {
            "id": x["entity_id"],
            "lat": x["lat"],
            "lon": x["lon"],
            "name": "".join(etree.fromstring(x["popup"]["value"], etree.HTMLParser()).xpath("//h2//text()")).strip(),
        }
        for x in settings["leaflet"]["leaflet-map-view-physical-stores-physical-stores-block"]["features"]
    ]
    result = [{**x, **extra[x["name"]]} for x in result]

    # Filter out points outside Portugal (e.g. Spain)
    country_poly = country_polygon()
    result = [x for x in result if country_poly.contains(Point(x["lon"], x["lat"]))]

    return result


def schedule_time(v, mapping):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"espaça?o [ck]asa",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        branch = re.sub(r"\s+MyShop$", "", nd["name"], flags=re.IGNORECASE)
        is_myshop = nd["name"].lower().endswith("myshop")
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"]), float(nd["lon"])]
        if d is None:
            ds = [x for x in old_data if (not x[REF] or x[REF]) and distance([x.lat, x.lon], coord) < 250]
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
        d["shop"] = "houseware"
        d["name"] = "MyShop Espaço Casa" if is_myshop else "Espaço Casa"
        # d["brand"] = ""  # noqa: ERA001
        # d["brand:wikidata"] = ""  # noqa: ERA001
        # d["brand:wikipedia"] = ""  # noqa: ERA001
        d["branch"] = BRANCHES.get(branch.lower(), titleize(branch))

        schedule = [
            x.strip()
            for x in re.split(r"(\b\d\d?h(?:\d\d)?\s*(?:-|às|\s+)\s*\d\d?h?(?:\d\d)?|\bencerra)\s", nd["schedule"].lower())
            if x.strip()
        ]
        schedule = [
            {
                "d": schedule_time(x[1], SCHEDULE_DAYS_MAPPING),
                "t": schedule_time(x[0], SCHEDULE_HOURS_MAPPING),
            }
            for x in itertools.batched(schedule, 2)
        ]
        schedule = [f"{x['d']} {x['t']}" for x in schedule]
        d["opening_hours"] = "; ".join(schedule)
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        if phones := [format_phonenumber(x) for x in nd["phones"]]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://espacocasa.com/"
        d["contact:facebook"] = "espacocasalojas"
        d["contact:youtube"] = "https://www.youtube.com/channel/UCoR-lQ3gmJMPnEZs0dl2kQQ"  # @EspaçoCasa-f2z
        d["contact:instagram"] = "espacocasaonline"
        d["contact:linkedin"] = "https://www.linkedin.com/company/espacocasa/"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(nd["address"].split("\n")).strip("; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Espaço Casa", REF, old_data)
