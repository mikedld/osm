#!/usr/bin/env python3

import itertools
import json
import re
from pathlib import Path

import requests
from unidecode import unidecode

from impl.common import DiffDict, cache_name, overpass_query, titleize, distance, opening_weekdays, write_diff
from impl.config import CONFIG, ENABLE_CACHE


REF = "ref"

DAYS = ["Se", "Te", "Qu", "Qu", "Se", "Sá", "Do"]
CITIES = {
    "1170-221": "Lisboa",
    "1495-070": "Algés",
    "1700-330": "Lisboa",
    "2135-002": "Samora Correia",
    "2475-011": "Benedita",
    "2560-546": "Silveira",
    "2605-004": "Casal de Cambra",
    "2625-657": "Vialonga",
    "2635-003": "Rio de Mouro",
    "2635-445": "Rio de Mouro",
    "2655-139": "Ericeira",
    "2665-258": "Malveira",
    "2685-012": "Sacavém",
    "2690-189": "Santa Iria de Azoia",
    "2705-866": "Terrugem",
    "2710-297": "Linhó",
    "2715-004": "Pêro Pinheiro",
    "2725-041": "Algueirão-Mem Martins",
    "2725-397": "Mem Martins",
    "2735-184": "Cacém",
    "2735-535": "Agualva",
    "2740-287": "Porto Salvo",
    "2750-269": "Torre",
    "2780-000": "Porto Salvo",
    "2785-035": "Abóboda",
    "2785-338": "Tires",
    "2785-404": "Rana",
    "2795-195": "Linda-a-Velha",
    "2805-312": "Pragal",
    "2810-035": "Feijó",
    "2815-756": "Sobreda",
    "2829-516": "Monte de Caparica",
    "2830-239": "Santo André",
    "2835-418": "Lavradio",
    "2845-147": "Amora",
    "2845-484": "Amora",
    "2845-608": "Amora",
    "2855-238": "Miratejo",
    "2855-578": "Corroios",
    "2865-600": "Fernão Ferro",
    "2925-547": "Vila Nogueira de Azeitão",
    "2955-267": "Pinhal Novo",
    "2975-312": "Quinta do Conde",
    "3080-229": "Buarcos",
    "4100-179": "Ramalde",
    "4400-514": "Canidelo",
    "4405-702": "Gulpilhares",
    "4410-150": "São Félix da Marinha",
    "4415-343": "Pedroso",
    "4425-057": "Águas Santas",
    "4430-066": "Oliveira do Douro",
    "4430-904": "Avintes",
    "4435-123": "Rio Tinto",
    "4435-668": "Baguim do Monte",
    "4445-245": "Alfena",
    "4445-288": "Ermesinde",
    "4450-800": "Leça da Palmeira",
    "4455-558": "Perafita",
    "4460-235": "Senhora da Hora",
    "4460-237": "Senhora da Hora",
    "4465-255": "São Mamede de Infesta",
    "4470-605": "Moreira",
    "4475-299": "Nogueira",
    "4485-000": "Mindelo",
    "4510-243": "São Pedro da Cova",
    "4615-013": "Lixa",
    "4740-415": "Fonte Boa",
    "4760-706": "Ribeirão",
    "4764-501": "Vila Nova de Famalicão",
    "4770-409": "Pousada de Saramagos",
    "4795-007": "Aves",
    "4835-297": "Pevidém",
    "4835-408": "Silvares",
    "4935-208": "Darque",
    "7500-220": "Vila Nova de Santo André",
    "7580-206": "Alcácer do Sal",
    "8100-070": "Boliqueime",
    "8125-020": "Quarteira",
    "8135-016": "Almancil",
    "8400-330": "Parchal",
}
BRANCH_ABBREVS = [
    [r"–", "-"],
    [r"\bav\.? ", "avenida "],
    [r"\bd\.\s*", "dom "],
    [r"\beng\.º? ", "engenheiro "],
    [r"\bestr\. ", "estrada "],
    [r"\bmt\.\s*", "monte "],
    [r"\bqta\. ", "quinta "],
    [r"\bqt\.ª ", "quinta "],
    [r"\bs\. ", "são "],
    [r"\bsao ", "são "],
    [r"\bsta\. ", "santa "],
    [r"\bv\.f\.xira\b", "vila franca de xira"],
    [r"\bv\.n\.\s*gaia\b", "vila nova de gaia"],
    [r"\bagueda\b", "águeda"],
    [r"\bcor\.\s*", "coronel "],
    [r"\bg\.delgado\b", "general delgado"],
    [r"\bj\.\s*", "josé "],
]
STREET_ABBREVS = [
    [r"\bav\.? ", "avenida "],
    [r"\beng\. ", "engenheiro "],
    [r"\bengº ", "engenheiro "],
    [r"\bd\. ", "dom "],
    [r"\bdr\. ", "doutor "],
    [r"\bnª ", "nossa "],
    [r"\bprof\. ", "professor "],
    [r"\bqta\. ", "quinta "],
    [r"\br\. ", "rua "],
    [r"\bs\. ", "são "],
    [r"\bsao ", "são "],
]


def fetch_data(url):
    cache_file = Path(f"{cache_name(url)}.json")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        page_size = 250
        result = []
        while True:
            r = requests.get(url, params={
                "$select": "*",
                "$filter": "CountryRegion eq 'PT'",
                "key": CONFIG["lidl"]["api_key"],
                "$format": "json",
                "$orderby": "EntityID",
                "$skip": len(result),
                "$top": page_size,
            })
            r.raise_for_status()
            result_page = r.content.decode("utf-8")
            result_page = json.loads(result_page)["d"]["results"]
            result.extend(result_page)
            if len(result_page) < page_size:
                break
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    for r in result:
        r["icons"] = []
        for k in [x for x in r.keys() if x.startswith("INFOICON")]:
            if v := r[k]:
                r["icons"].append(v)
            r.pop(k)
        r["OpeningTimes"] = re.sub(r"</?b>", "", r["OpeningTimes"]).split("<br>")
    return result


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r'[)({}`´?*&$§!:;<>+#=°^"\[\]]+', "", e)
    e = re.sub(r"[/_., ']+", "-", e)
    e = re.sub(r"[/-]+", "-", e)
    return e.lower()


if __name__ == "__main__":
    data_url = "https://spatial.virtualearth.net/REST/v1/data/e470ca5678c5440aad7eecf431ff461a/Filialdaten-PT/Filialdaten-PT"
    new_data = fetch_data(data_url)

    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[shop][~"^(name|brand)$"~"[Ll][Ii][Dd][Ll]"](area.p); );')["elements"]]

    for nd in new_data:
        public_id = nd["EntityID"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["Latitude"]), float(nd["Longitude"])]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 100], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["Latitude"])
            d.data["lon"] = float(nd["Longitude"])
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["name"] = "Lidl"
        d["brand"] = "Lidl"
        d["brand:wikidata"] = "Q151954"
        d["brand:wikipedia"] = "pt:Lidl"
        if branch := nd["ShownStoreName"]:
            for r in BRANCH_ABBREVS:
                branch = re.sub(r[0], r[1], branch.lower())
            if re.sub(r"\W+", "", d["branch"].lower()) != re.sub(r"\W+", "", branch.lower()):
                d["branch"] = titleize(branch)

        #if "freeWiFi" in nd["icons"]:
        #    d["internet_access"] = "yes"
        #    d["internet_access:fee"] = "no"

        schedule = [
            re.split(r"\s+", x)
            for x in nd["OpeningTimes"]
        ]
        days = list(DAYS)
        days_offset = 0
        while days != [x[0] for x in schedule]:
            days = days[1:] + [days[0]]
            days_offset += 1
        schedule = [
            {
                "d": (x[0] + days_offset) % 7,
                "t": "off" if x[1][1] == "Fechado" else x[1][1],
            }
            for x in enumerate(schedule)
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
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        d["contact:phone"] = "+351 210 207 000"
        d["contact:website"] = f"https://www.lidl.pt/s/pt-PT/pesquisa-de-loja/{get_url_part(nd['Locality'])}/{get_url_part(nd['AddressLine'])}/"
        d["contact:facebook"] = "lidlportugal"
        d["contact:youtube"] = "https://www.youtube.com/user/LidlPortugal"
        d["contact:instagram"] = "lidlportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/lidl-portugal"
        d["contact:tiktok"] = "lidl.portugal"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = nd["PostalCode"]
        if len(postcode) == 4:
            if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                postcode = d["addr:postcode"]
            else:
                postcode += "-000"
        d["addr:postcode"] = postcode
        city = CITIES.get(postcode, nd["Locality"])
        d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            if m := re.fullmatch(r"(.+?),?\s+(([Ll]oja |[Ll]ote )?\d+\w?((-|\s+[ae]\s+|\s*[/,]\s*)\d+\w?)*|[Ss]/[Nn]|[Kk][Mm]\s*\d.*)", nd["AddressLine"]):
                street, num = m[1].lower(), m[2]
                for r in STREET_ABBREVS:
                    street = re.sub(r[0], r[1], street.lower())
                street = titleize(street)
                if re.match(r"^(Quinta|Casal|Villas)\b.+", street):
                    d["addr:place"] = street
                elif re.match(r"^(Urbanização)\b.+", street):
                    d["addr:suburb"] = street
                elif re.match(r"^(Mercado)\b.+", street):
                    d["addr:housename"] = street
                else:
                    d["addr:street"] = street
                if num.lower() == "s/n":
                    d["nohousenumber"] = "yes"
                    tags_to_reset.update({"addr:housenumber", "addr:milestone", "addr:unit"})
                elif num.lower().startswith("km"):
                    d["addr:milestone"] = re.sub(r"^km\s*", "", num, flags=re.I).replace(",", ".")
                    tags_to_reset.update({"addr:housenumber", "nohousenumber", "addr:unit"})
                elif num.lower().startswith("loja"):
                    d["addr:unit"] = titleize(num)
                    tags_to_reset.update({"addr:housenumber", "nohousenumber", "addr:milestone"})
                else:
                    d["addr:housenumber"] = re.sub(r"\s+a\s+", "-", re.sub(r"\s*e\s*", ";", re.sub(r"\s*[/,]\s*", ";", num)))
                    tags_to_reset.update({"nohousenumber", "addr:milestone", "addr:unit"})
            else:
                d["x-dld-addr"] = nd["AddressLine"]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["EntityID"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Lidl", REF, old_data, osm=True)
