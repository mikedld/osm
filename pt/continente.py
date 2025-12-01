#!/usr/bin/env python3

import itertools
import re
import uuid
from multiprocessing import Pool
from urllib.parse import urljoin, urlsplit

from impl.common import DiffDict, distance, fetch_html_data, opening_weekdays, overpass_query, write_diff


DATA_URL = "https://feed.continente.pt/lojas"

REF = "ref"

DAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
BRANCHES = {
    "Av. de Paris": "Avenida de Paris",
    "Barbosa Du Bocage": "Barbosa du Bocage",
    "Cufra (Av. da Boavista)": "Cufra (Avenida da Boavista)",
    "Gaia (Av. República)": "Gaia (Avenida República)",
    "Ilhavo": "Ílhavo",
    "Lisboa (Av. República)": "Lisboa (Avenida República)",
    "Loureshopping": "LoureShopping",
    "S. Bartolomeu de Messines": "São Bartolomeu de Messines",
    "Vila Pouca Aguiar": "Vila Pouca de Aguiar",
}
CITIES = {
    "1500-098": "Lisboa",
    "1685-581": "Caneças",
    "1700-086": "Lisboa",
    "1885-003": "Moscavide",
    "2685-010": "Sacavém",
    "2700-279": "Amadora",
    "2925-148": "Azeitão",
    "4200-008": "Porto",
    "4425-116": "Maia",
    "4460-841": "Senhora da Hora",
    "4520-605": "São João de Ver",
    "4770-405": "Pousada de Saramagos",
    "9135-060": "Camacha",
    "9230-085": "Santana",
    "9325-063": "Estreito Câmara de Lobos",
    "9600-516": "Ribeira Grande",
    "Agueda": "Águeda",
    "AlvercaDoRibatejo": "Alverca do Ribatejo",
    "Braganca": "Bragança",
    "Covilha": "Covilhã",
    "Fundao": "Fundão",
    "Grandola": "Grândola",
    "Loule": "Loulé",
    "Lousa": "Lousã",
    "Nazare": "Nazaré",
    "Olhao": "Olhão",
    "Oliveira de Azemeis": "Oliveira de Azeméis",
    "Pacos de Ferreira": "Paços de Ferreira",
    "Pte. da Barca": "Ponte da Barca",
    "Pte. da Pedra": "Ponte da Pedra",
    "Povoa de Varzim": "Póvoa de Varzim",
    "ReguengosDeMonsaraz": "Reguengos de Monsaraz",
    "Santarem": "Santarém",
    "Sao Cosme": "São Cosme",
    "Sao Joao da Madeira": "São João da Madeira",
    "Setubal": "Setúbal",
    "Tapada das Merces": "Tapada das Mercês",
    "VilaNovadeFamalicão": "Vila Nova de Famalicão",
}


def fetch_level1_data():
    result_tree = fetch_html_data(DATA_URL)
    result = [
        {
            "lat": float(el.attrib["data-lat"]),
            "lon": float(el.attrib["data-lng"]),
            "name": el.attrib["data-name"],
            "city": el.attrib["data-city"],
            "addr": el.xpath(".//p[contains(@class, 'storeMapHeader__store-addres')]/text()")[0].strip(),
            "url": urljoin(DATA_URL, el.xpath(".//a[contains(@class, 'storeMapHeader__store-link')]/@href")[0]),
        }
        for el in result_tree.xpath("//li[contains(@class, 'storeMapHeader__store')]")
    ]
    return result


def fetch_level2_data(data):
    result_tree = fetch_html_data(data["url"])
    return {
        **data,
        "id": str(
            uuid.uuid5(
                uuid.NAMESPACE_URL, "continente:" + re.sub(r"^continente-", "", urlsplit(data["url"]).path.split("/")[2])
            )
        ),
        "services": [x.strip().lower() for x in result_tree.xpath("//li[@class='serviceTag']//text()") if x.strip()],
        "schedule": {
            "".join(el.xpath(".//td[contains(@class, 'storeDetailHeaderMap__table-day')]/text()")): re.sub(
                r":0(\d\d)",
                r":\1",
                "-".join(el.xpath(".//td[contains(@class, 'storeDetailHeaderMap__table-time')]/time/text()")),
            )
            for el in result_tree.xpath("//table[contains(@class, 'storeDetailHeaderMap__table')]/tr")
        },
        "phone": re.sub(
            r"[^+0-9]",
            "",
            "".join(result_tree.xpath("//a[contains(@class, 'storeDetailHeaderMap__button')][contains(@href, 'tel:')]/@href")),
        ),
        "addr": [
            x.strip()
            for x in "".join(result_tree.xpath("//p[contains(@class, 'storeDetailHeaderMap__address')]/text()")).split("\n")
            if x.strip()
        ],
    }


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [
        DiffDict(e)
        for e in overpass_query('nwr[shop][shop!=newsagent][shop!=florist][shop!=tobacco][name~"continente",i](area.country);')
    ]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        name = re.sub(r"^(Continente( Bom Dia| Modelo)?).*", r"\1", nd["name"])
        branch = nd["name"][len(name) :].strip()
        is_bd = name == "Continente Bom Dia"
        is_mod = name == "Continente Modelo"
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["lat"], nd["lon"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
            elif is_mod and len(ds) == 2:
                if ds[0].data["type"] != "node" or not ds[0]["indoor"]:
                    ds[0], ds[1] = ds[1], ds[0]
                if (ds[0].data["type"] == "node" or ds[0]["indoor"]) and ds[1]["building"]:
                    d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["lat"]
            d.data["lon"] = nd["lon"]
            old_data.append(d)
            new_node_id -= 1

        d[REF] = public_id
        d["shop"] = (
            "supermarket" if not is_bd or ("talho" in nd["services"] and "mercearia" in nd["services"]) else "convenience"
        )
        d["name"] = name
        d["brand"] = "Continente Bom Dia" if is_bd else ("Continente Modelo" if is_mod else "Continente")
        d["brand:wikidata"] = "Q123570507" if is_bd else ("Q1892188" if is_mod else "Q2995683")
        d["brand:wikipedia"] = (
            "pt:Continente Bom Dia" if is_bd else ("pt:Continente Modelo" if is_mod else "pt:Continente (hipermercados)")
        )
        d["branch"] = BRANCHES.get(branch, branch)

        if schedule := nd["schedule"]:
            schedule = [
                {
                    "d": DAYS.index(k),
                    "t": v,
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
            schedule = "; ".join(schedule)
            d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = nd["phone"]
        if not phone.startswith("+351"):
            phone = f"+351{phone}"
        if len(phone) == 13:
            d["contact:phone"] = f"+351 {phone[4:7]} {phone[7:10]} {phone[10:13]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = nd["url"]
        d["contact:facebook"] = "continenteoficial"
        d["contact:youtube"] = "https://www.youtube.com/user/continentept"
        d["contact:instagram"] = "continente"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = re.sub(r".*\b(\d{4}(-\d{3})?)\s*$", r"\1", (nd["addr"] or [""])[0])
        if len(postcode) == 4:
            postcode += "-000"
        if len(postcode) == 8:
            d["addr:postcode"] = postcode
        elif postcode:
            d["addr:postcode"] = "<ERR>"
        d["addr:city"] = CITIES.get(postcode, CITIES.get(nd["city"], nd["city"]))
        if (
            not d["addr:street"]
            and not (d["addr:housenumber"] or d["addr:housename"] or d["nohousenumber"])
            and not d["addr:place"]
            and not d["addr:suburb"]
        ):
            d["x-dld-addr"] = "; ".join(nd["addr"])

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

    write_diff("Continente", REF, old_data, osm=True)
