#!/usr/bin/env python3

import itertools
import json
import re
from multiprocessing import Pool
from urllib.parse import urljoin

from impl.common import DiffDict, RedoIter, distance, fetch_html_data, overpass_query, write_diff


DATA_URL = "https://www.emel.pt/pt/parques/ajax/parques.ajax.php"

REF = "ref"

OWNER_FIXES = {
    "lidl": "Lidl",
}
OWNER_LOC_FIXES = {
    "av. infante d. henrique": "Avenida Infante Dom Henrique",
}
STAY_DURATIONS = {
    "mínimo": "15 minutes",
    "minimo a pagar 15 minutos": "15 minutes",
    "mínimo a pagar até 15 minutos": "15 minutes",
    "até 15 minutos": "15 minutes",
    "até 30 minutos": "30 minutes",
    "15 minutos": "15 minutes",
    "30 mins": "30 minutes",
    "30 minutos": "30 minutes",
    "1 h": "1 hour",
    "1 hora": "1 hour",
    "90 minutos": "90 minutes",
    "até 2 horas": "2 hours",
    "2 h": "2 hours",
    "2 horas": "2 hours",
    "2:01 horas": "2 hours 1 minute",
    "3 h": "3 hours",
    "3 horas": "3 hours",
    "4 h": "4 hours",
    "4 horas": "4 hours",
    "5 horas": "5 hours",
    "6 horas": "6 hours",
}
CHARGE_COMMENTS = {
    "a partir de 241 minutos:0,30€/ por fração de 15 minutos - preço ao minuto, com arredondamento a 0,05€": (
        "0.30 EUR/15 minutes @ (stay > 4 mours)"
    ),
}


def fetch_level1_data():
    result_tree = fetch_html_data(DATA_URL)
    result = [
        {
            "id": re.sub(r".*\(\s*(\d+)\s*\).*", r"\1", el.xpath("./footer/a[@href='#map_canvas']/@onclick")[0]),
            "coords": list(
                map(
                    float,
                    re.sub(r".*\(([-\s\d.]+,[-\s\d.]+),.*", r"\1", el.xpath("./footer/a[@href='#direction']/@onclick")[0])
                    .replace(" ", "")
                    .split(","),
                )
            ),
            "name": el.xpath("./div/h3/text()")[0].strip(),
            "address": el.xpath("./div/p/text()")[0].strip(),
            "url": el.xpath("./div/a/@href")[0],
            "services": [x.lower() for x in el.xpath("./ul[@class='servicos']//img/@alt")],
        }
        for el in result_tree.xpath("/html/body/div/ul/li")
    ]
    return result


def fetch_level2_data(data):
    url = urljoin(DATA_URL, data["url"])
    result_tree = fetch_html_data(url)
    main_el = result_tree.xpath("//section[contains(@class, 'main')]")[0]
    info_el = main_el.xpath("./div[contains(@class, 'parqueinfo')]")[0]
    locations = re.sub(r".*var\s+locations\s*=\s*\[(.+?)\];.*", r"\1", main_el.xpath(".//script/text()")[0], flags=re.DOTALL)
    locations = json.loads(
        re.sub(r",\s*-\s*(?=\d)", r",-", locations.split("// [")[0].replace('"', "&quot;").replace("'", '"'))
    )
    return {
        **data,
        "url": url,
        "schedule": [
            x.strip().lower() for x in info_el.xpath("./p[contains(./strong/text(), 'Horário:')]/text()") if x.strip()
        ],
        "type": info_el.xpath("./p[contains(./strong/text(), 'Tipologia:')]/text()")[0].strip().lower(),
        "capacity": int(info_el.xpath("./p[contains(./strong/text(), 'Número de Lugares:')]/text()")[0].strip()),
        "capacity:charging": int(info_el.xpath("./p[contains(./strong/text(), 'Lugares de Carregamento:')]/text()")[0].strip()),
        "capacity:disabled": int(
            info_el.xpath("./p[contains(./strong/text(), 'Lugares para Deficientes:')]/text()")[0].strip()
        ),
        "charge": list(
            itertools.batched(
                (
                    x.strip().lower()
                    for x in result_tree.xpath(".//li[contains(./h1/text(), 'Tarifário')]/div/table//td/text()")
                ),
                2,
            )
        ),
        "charge_comment": "; ".join(result_tree.xpath(".//li[contains(./h1/text(), 'Tarifário')]/div/text()"))
        .strip("; ")
        .lower(),
        "address": [x.strip() for x in locations[3:6]],
    }


def fixup_name(name):
    if len(parts := re.split(r"\s*\|\s*", name)) == 2:
        name = " - ".join((OWNER_FIXES.get(parts[0].lower(), parts[0]), OWNER_LOC_FIXES.get(parts[1].lower(), parts[1])))
    return name


def fixup_price(price):
    price = "-".join([x.replace(",", ".").strip("€ ").removesuffix(".00") for x in price.split("/")])
    return f"{price} EUR"


def pop_price(prices, predicate=None):
    for i, p in enumerate(prices):
        if predicate is None or predicate(*p):
            prices.pop(i)
            return p
    return None


def fixup_stay_duration(v):
    return STAY_DURATIONS.get(v) or f"<ERR:{v}>"


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "area[admin_level=7][name=Lisboa](area.country) -> .lisboa;"
            "("
            "nwr[amenity=parking][!zone]"
            "  [parking!=lane][parking!=street_side]"
            "  [access!=no][access!=private]"
            "  [!construction]"
            '  [~"^(name|brand|operator)$"~"emel",i](area.lisboa);'
            "nwr[amenity=parking][!zone]"
            "  [parking!=lane][parking!=street_side]"
            "  [access=private]"
            "  [ref]"
            "  [!construction]"
            '  [~"^(name|brand|operator)$"~"emel",i](area.lisboa);'
            ");"
        )
    ]

    old_node_ids = {d.data["id"] for d in old_data}

    new_data_iter = RedoIter(new_data)
    old_type = "relation"
    last_id = None
    for nd in new_data_iter:
        public_id = nd["id"]
        name_parts = re.split(r"\s*(?://|-)\s*(?=Exclusivo\b)", nd["name"].strip())
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id and od.data["type"] == old_type), None)
        coord = nd["coords"]
        if d is None:
            ds = [x for x in old_data if not x[REF] and x.data["type"] == old_type and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None and old_type in ("way", "relation"):
            old_type = "way" if old_type == "relation" else "node"
            new_data_iter.redo = True
            continue
        if d is None:
            if public_id == last_id:
                old_type = "relation"
                last_id = None
                continue
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["amenity"] = "parking"
        d["name"] = fixup_name(name_parts[0])
        d["operator"] = "EMEL"
        d["operator:type"] = "public"
        d["operator:wikidata"] = "Q30256831"

        if len(name_parts) > 1:
            exclusive = re.sub(r"^exclusivo( a)? ", "", name_parts[1].lower())
            if exclusive == "assinaturas mensais":
                d["access"] = "permit"
            elif exclusive == "residentes":
                d["access"] = "private"
                d["private"] = "residents"
            elif exclusive == "residentes e comerciantes":
                d["access"] = "private"
                d["private"] = "residents;merchants"

        if nd["type"] == "superfície":
            d["parking"] = "surface"
        elif nd["type"] == "estrutura":
            if d["parking"] not in ("underground", "multi-storey"):
                d["parking"] = "underground | multi-storey"
        else:
            d["x-dld-type"] = nd["type"]

        d["capacity"] = str(nd["capacity"])
        d["capacity:disabled"] = str(nd["capacity:disabled"])
        d["capacity:charging"] = str(nd["capacity:charging"])
        d["source:capacity"] = "website"

        if prices := list(nd["charge"]):
            d["fee"] = "yes"

            pop_price(prices, lambda k, _: k in ("bilhete perdido", "perda de bilhete"))

            fee_cond = []
            if free_for_all := pop_price(prices, lambda _, v: v == "gratuito"):
                fee_cond.append(f"no @ (stay <= {fixup_stay_duration(free_for_all[0])})")
            if free_for_lidl := pop_price(prices, lambda _, v: v == "grátis para clientes lidl"):
                fee_cond.append(f"no @ (customers AND stay <= {fixup_stay_duration(free_for_lidl[0])})")
            d["fee:conditional"] = "; ".join(fee_cond) if fee_cond else ""

            if max_daily_price := pop_price(prices, lambda k, _: k in ("máximo diário", "máximo dia")):
                d["charge"] = f"{fixup_price(max_daily_price[1])}/day"
            else:
                d["charge"] = ""

            charge_cond = [
                f"{fixup_price(price)} @ (stay <= {fixup_stay_duration(duration)})" for duration, price in reversed(prices)
            ]
            if extra_charge_cond := CHARGE_COMMENTS.get(nd["charge_comment"]):
                charge_cond.append(extra_charge_cond)
            d["charge:conditional"] = "; ".join(charge_cond)

        d["bicycle"] = "yes" if "bicipark" in nd["services"] else ""
        d["motorcycle"] = "yes" if "motociclos" in nd["services"] else ""
        d["wheelchair"] = d["wheelchair"] or ("yes" if "mobilidade reduzida" in nd["services"] else "")

        d["payment:cards"] = "yes" if "multibanco" in nd["services"] else ""
        d["payment:via_verde"] = "yes" if "via verde" in nd["services"] else ""

        if "parque 24h" in nd["services"]:
            d["opening_hours"] = "24/7"
            d["source:opening_hours"] = "website"

        d["website"] = nd["url"]
        d["contact:instagram"] = "emel_mobilidade"
        d["contact:linkedin"] = "https://www.linkedin.com/company/emelmobilidade/"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        d["addr:postcode"] = d["addr:postcode"] or nd["address"][1].strip()
        d["addr:city"] = "Lisboa"
        # if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
        #     d["x-dld-addr"] = "; ".join(nd["address"])  # noqa: ERA001

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

        if d.data["type"] == "relation":
            old_type = "way"
            last_id = public_id
            new_data_iter.redo = True
        else:
            old_type = "relation"
            last_id = None

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("EMEL", REF, old_data, osm=True)
