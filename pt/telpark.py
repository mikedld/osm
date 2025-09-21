#!/usr/bin/env python3

import re
from multiprocessing import Pool

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, write_diff


LEVEL1_DATA_URL = "https://www.telpark.com/pt/wp-json/wp/v2/country"
LEVEL2_DATA_URL = "https://www.telpark.com/pt/wp-json/wp/v2/locations"
LEVEL3_DATA_URL = "https://www.telpark.com/pt/wp-json/wp/v2/parkings"

REF = "ref"

BRANCHES = {
    "alves redol - técnico": "Alves Redol",
    "avenida de roma": "Roma",
    "d. joão i": "Dom João I",
    "panoramic": "Panoramic",
}
PAYMENT_METHODS = {
    "246": "credit_cards",  # Cartão de Crédito
    "249": "cash",  # Numerário
    "252": "via_verde",  # Via verde
    "255": "",  # Caixa de Pagamento Automático (mistake, should be in services group)
    "258": "",  # VIA-T
    "270": "app",  # App telpark
    "?01": "debit_cards",  # (missing, here for completeness)
}
SCHEDULE_MAPPING = {
    "08:00h - 22:00h": "Mo-Su 08:00-22:00",
    "24h": "24/7",
    "aberto ao público das 7:00 às 00:00": "Mo-Su 07:00-00:00",
    "segunda a sexta-feira: 7 h a 24 h": "Mo-Fr 07:00-00:00",
}


def fetch_paged_data(url, params):
    page_size = 100
    result = []
    while True:
        page_params = {
            "offset": len(result),
            "per_page": page_size,
        }
        page = fetch_json_data(url, params={**params, **page_params})
        result.extend(page)
        if len(page) < page_size:
            break
    return result


def fetch_level1_data():
    return {
        str(x["id"]): x["name"] for x in fetch_paged_data(LEVEL1_DATA_URL, {"_fields": "id,name"}) if x["name"] == "Portugal"
    }


def fetch_level2_data(country_id):
    return {
        str(x["id"]): x["title"]["rendered"]
        for x in fetch_paged_data(LEVEL2_DATA_URL, {"country": country_id, "_fields": "id,title"})
    }


def fetch_level3_data(locations):
    return [
        {
            **x["meta_box"]["header"],
            **x["meta_box"]["detail"],
            **x["meta_box"]["parking_info"],
            "id": str(x["id"]),
            "name": x["title"]["rendered"],
            "link": x["link"],
            "city": locations[x["meta_box"]["location"]],
        }
        for x in fetch_paged_data(LEVEL3_DATA_URL, {})
        if x["meta_box"]["location"] in locations
    ]


def lookup_title(items, title):
    return next((x for x in items if re.fullmatch(title, x["title"].strip(), flags=re.IGNORECASE)), None)


def fixup_price(price):
    price = "-".join([x.replace(",", ".").strip("€ ").removesuffix(".00") for x in price.split("/")])
    return f"{price} EUR"


class RedoIter:
    def __init__(self, items):
        self.redo = False
        self._items = items

    def __iter__(self):
        i = 0
        while i < len(self._items):
            self.redo = False
            yield self._items[i]
            if not self.redo:
                i += 1


if __name__ == "__main__":
    countries = fetch_level1_data()
    with Pool(4) as p:
        locations = {k: v for x in p.imap_unordered(fetch_level2_data, countries.keys()) for k, v in x.items()}
    new_data = fetch_level3_data(locations)

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "("
            'nwr[amenity=parking][~"^(name|brand|operator)$"~"(Tel|Em)park"](area.country);'
            'node[amenity=parking_entrance][~"^(name|brand|operator)$"~"(Tel|Em)park"](area.country);'
            ");"
        )
    ]

    new_data_iter = RedoIter(new_data)
    old_type = "relation"
    last_id = None
    for nd in new_data_iter:
        public_id = nd["id"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id and od.data["type"] == old_type), None)
        coord = [float(nd["latitude"]), float(nd["longitude"])]
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

        d[REF] = public_id
        d["amenity"] = "parking"
        d["name"] = BRANCHES.get(nd["name"].lower(), nd["name"])
        d["brand"] = "Telpark"
        d["operator"] = "Empark"

        if m := re.search(r"\b(\d+)\s+(?:veículos|lu(ga|ag)res)\b", nd["description"]):
            d["capacity"] = m[1].lstrip("0")

        d["fee"] = "yes"
        if info := lookup_title(nd["info_list"], r"preços gerais"):
            prices = []
            if price := lookup_title(info["items"], r"preço( da)? primeira hora"):
                prices.append(f"{fixup_price(price['content'])}/hour")
            if price := lookup_title(info["items"], r"máximo di[aá]rio"):
                prices.append(f"{fixup_price(price['content'])}/day")
            d["charge"] = "; ".join(prices)
        tags_to_reset.add("fee:amount")

        if info := lookup_title(nd["icon_list"], r"formas? de pag(ament)?o"):
            for method_id, method_name in PAYMENT_METHODS.items():
                if not method_name:
                    continue
                if method_id in info["icon_data"]:
                    d[f"payment:{method_name}"] = "yes"
                else:
                    tags_to_reset.add(f"payment:{method_name}")

        if (info := lookup_title(nd["info_list"], r"horários")) and (
            schedule := lookup_title(info["items"], r"hor[aá]rios? de serviç?o")
        ):
            d["opening_hours"] = SCHEDULE_MAPPING.get(schedule["content"].lower(), f"<ERR:{schedule['content']}>")
            d["source:opening_hours"] = "website"

        if phones := [x for x in re.split(r"[/,]", nd.get("tlf", "").replace(" ", "")) if x]:
            phones = [x.removeprefix("+351") for x in phones]
            phones = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in phones if len(x) == 9]
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = nd["link"]
        d["contact:facebook"] = "telpark.es"
        d["contact:youtube"] = "https://www.youtube.com/@telpark_oficial"
        d["contact:instagram"] = "telpark_app"
        d["contact:twitter"] = "telpark_es"
        d["contact:linkedin"] = "https://www.linkedin.com/company/empark-aparcamientos-y-servicios-s-a"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        address = nd["address"].strip().removesuffix(nd["city"]).strip()
        if m := re.fullmatch(r"(.+?)\s*[,.–]\s*(\d{4}(-\d{3})?)", address):
            postcode = m[2]
            if len(postcode) == 4:
                postcode += "-000"
            d["addr:postcode"] = postcode
            address = m[1]
        d["addr:city"] = nd["city"]
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address

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
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Telpark", REF, old_data, osm=True)
