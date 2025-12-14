#!/usr/bin/env python3

import itertools
import re
from datetime import date, datetime

from unidecode import unidecode

from impl.common import BASE_NAME, DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, titleize, write_diff
from impl.config import CONFIG


XCONFIG = CONFIG[BASE_NAME]

DATA_URL = "https://live.api.schwarz/odj/stores-api/v2/myapi/stores-frontend/stores"

REF = "ref"

CITIES = {
    "1495-070": "Algés",
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
    "2820-205": "Charneca de Caparica",
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
    [r"\s*/\s*", " / "],
    [r"\bav\.? ", "avenida "],
    [r"\bd\.\s*", "dom "],
    [r"\bdr\.\s*", "doutor "],
    [r"\beng\.º? ", "engenheiro "],
    [r"\bestr\. ", "estrada "],
    [r"\bmt\.\s*", "monte "],
    [r"\bqta\. ", "quinta "],
    [r"\bqt\.ª ", "quinta "],
    [r"\br\. ", "rua "],
    [r"\bs\. ", "são "],
    [r"\bsao ", "são "],
    [r"\bsta\.? ", "santa "],
    [r"\bsto\.? ", "santo "],
    [r"\bv\.f\.xira\b", "vila franca de xira"],
    [r"\bv\.n\.\s*gaia\b", "vila nova de gaia"],
    [r"\bagueda\b", "águeda"],
    [r"\bcor\.\s*", "coronel "],
    [r"\bg\.delgado\b", "general delgado"],
    [r"\bj\.\s*", "josé "],
]
CITY_FIXES = {
    "charneca da caparica": "charneca de caparica",
    "ponte de sôr": "ponte de sor",
    "são joão madeira": "são joão da madeira",
}
CITY_LOC_FIXES = {
    "25abril": "25 de Abril",
    "avenida camilo": "avenida de camilo",
    "avenida fernão de magalhães": "avenida de fernão de magalhães",
    "avenida im. conc.": "avenida imaculada conceição",
    "avenida principal / en1": "avenida principal / en 1",
    "avenida república": "avenida da república",
    "avenida rua smith": "avenida robert smith",
    "avenida visconde valmor": "avenida visconde de valmor",
    "en 101 - monção": "en 101",
    "mercado rio mouro": "mercado de rio de mouro",
    "monte burgos": "monte dos burgos",
    "oliveira junior": "oliveira júnior",
    "porto mós": "porto de mós",
    "póvoa santa iria": "póvoa de santa iria",
    "rua cruz da palmeira": "rua da cruz de palmeira",
    "rua engenheiro paulo barros": "rua engenheiro paulo de barros",
    "rua horta das figueiras": "rua da horta das figueiras",
    "rua jornal de são tirso": "rua do jornal santo tirso",
    "s.hora": "senhora da hora",
    "s.marinha": "santa marinha",
    "santa iria da azóia": "santa iria da azoia",
}


def fetch_data():
    page_size = 250
    result = []
    headers = {
        "x-apikey": XCONFIG["api_key"],
    }
    while True:
        params = {
            "country_code": "PT",
            "offset": len(result),
            "limit": page_size,
        }
        page = fetch_json_data(DATA_URL, params=params, headers=headers)["items"]
        result.extend(page)
        if len(page) < page_size:
            break
    return result


def fix_branch(branch):
    for r in BRANCH_ABBREVS:
        branch = re.sub(r[0], r[1], branch.lower())
    branch = re.sub(
        (
            r"^(aveiro|barcelos|batalha|famalicão|faro|gondomar|guimarães|leiria|loulé|moita|montijo|porto(?! alto)|sintra"
            r"|trofa|vila do conde)\b(?:\s*-)?\s*(.+)$"
        ),
        r"\1 - \2",
        branch,
    )
    if m := re.fullmatch(r"(.+?)\s+-\s+(.+)", branch):
        city, loc = m[1], m[2]
        city = CITY_FIXES.get(city, city)
        if m := re.fullmatch(r"(.+?)\s+(\d+)", loc):
            loc = f"{CITY_LOC_FIXES.get(m[1], m[1])} {m[2]}"
        else:
            loc = CITY_LOC_FIXES.get(loc, loc)
        branch = f"{city} - {loc}"
    else:
        branch = CITY_FIXES.get(branch, branch)
        branch = CITY_LOC_FIXES.get(branch, branch)
    return re.sub(r"\b(En \d+|Ikea|Aep)\b", lambda m: m[0].upper(), titleize(branch))


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r'[)({}`´?*&$§!:;<>+#=°^"\[\]]+', "", e)
    e = re.sub(r"[/_., ']+", "-", e)
    e = re.sub(r"[/-]+", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"lidl",i](area.country);')]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["objectNumber"]
        addr = nd["address"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(addr["latitude"]), float(addr["longitude"])]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)
            new_node_id -= 1
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["name"] = "Lidl"
        d["brand"] = "Lidl"
        d["brand:wikidata"] = "Q151954"
        d["brand:wikipedia"] = "pt:Lidl"
        d["branch"] = fix_branch(nd["storeName"])

        # icons = {x["name"] for x in nd["marketingData"]["infoIcons"]}  # noqa: ERA001
        # if "freeWiFi" in icons:
        #     d["internet_access"] = "yes"  # noqa: ERA001
        #     d["internet_access:fee"] = "no"  # noqa: ERA001

        schedule = [
            {
                "d": date.fromisoformat(x["date"]).weekday(),
                "t": "-".join(
                    [datetime.fromisoformat(y["from"]).strftime("%H:%M"), datetime.fromisoformat(y["to"]).strftime("%H:%M")]
                ),
            }
            for x in nd["openingHours"]["items"]
            for y in x["timeRanges"]
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k,
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        d["contact:phone"] = "+351 210 207 000"
        d["website"] = "/".join(
            [
                "https://www.lidl.pt/s/pt-PT/pesquisa-de-loja",
                get_url_part(addr["city"]),
                get_url_part("-".join(x for x in [addr["streetName"], addr["streetNumber"]] if x)),
                "",
            ]
        )
        d["contact:facebook"] = "lidlportugal"
        d["contact:youtube"] = "https://www.youtube.com/user/LidlPortugal"
        d["contact:instagram"] = "lidlportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/lidl-portugal"
        d["contact:tiktok"] = "lidl.portugal"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = addr["zip"]
        if len(postcode) == 4:
            if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                postcode = d["addr:postcode"]
            else:
                postcode += "-000"
        d["addr:postcode"] = postcode
        city = CITIES.get(postcode, addr["city"])
        d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(x for x in [addr["streetName"], addr["streetNumber"]] if x)

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Lidl", REF, old_data, osm=True)
