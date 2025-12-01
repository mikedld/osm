#!/usr/bin/env python3

import html
import itertools
import re
from urllib.parse import urlsplit

from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, opening_weekdays, overpass_query, titleize, write_diff


DATA_URL = "https://amanhecer.pt/wp-admin/admin-ajax.php"

REF = "ref"

DAYS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
BRANCH_FIXES = (
    (r"\s{2,}", " "),
    (r"–", "-"),
    (r"[’´`]", "'"),
    (r"[“”]", ""),
    (r"(^| )a ", r"\1A "),
    (r"(^| )o ", r"\1O "),
    (r"'S\b", "'s"),
    (r"\b3m\b", "3M"),
    (r"\bà do\b", "À do"),
    (r"\ba-dos-\b", "A-dos-"),
    (r"\b[Dd]'\s*", "d'"),
    (r"\bDelimarket\b", "DeliMarket"),
    (r"\bGi\b", "GI"),
    (r"\bEpac\b", "EPAC"),
    (r"\bEsuper\b", "ESuper"),
    (r"\b(Frescos do) Nh\b", r"\1 NH"),
    (r"^.*\b(Império das Carnes)\b", r"\1"),
    (r"\bLidermarche\b", "Lidermarché"),
    (r"\bLL\b", "II"),
    (r"\bLLL\b", "III"),
    (r"\bLILI\b", "Lili"),
    (r"\bMarketfish\b", "MarketFish"),
    (r"\bMini[- ]Mercado\b", "Minimercado"),
    (r"\bNa Mina\b", "na Mina"),
    (r"\bParaiso\b", "Paraíso"),
    (r"\bPorto Côvo\b", "Porto Covo"),
    (r"\bRm Costa\b", "R.M. Costa"),
    (r"\bS\. Bernardo\b", "São Bernardo"),
    (r"^Sa ", "SA "),
    (r"\bSupercastanholas\b", "Super Castanholas"),
    (r"\b(Minimercado) (Alameda)\b", r"\1 da \2"),
)
SUBNETS = {
    r"DeliMarket",
    r"Frescos do Oeste",
    r"Maxpreços",
    r"Mercearia da Lucinda",
    r"O Terreiro",
    r"Ponto Fresco",
    r"Xtra",
}
CITIES = {
    "2230-008": "Alcaravela",
    "2560-016": "A dos Cunhados",
    "2660-265": "Santo António dos Cavaleiros",
    "2660-294": "Santo António dos Cavaleiros",
    "2820-271": "Charneca de Caparica",
    "2825-359": "Costa da Caparica",
    "3060-318": "Febres",
    "4570-417": "São Pedro de Rates",
    "4615-676": "Lixa",
    "5450-140": "Perdas Salgadas",
    "6225-051": "Aldeia de São Francisco de Assis",
    "6270-133": "Paranhos da Beira",
    "6320-360": "Sabugal",
    "7570-610": "Melides",
    "7630-084": "Almograve",
    "7630-592": "São Miguel",
    "8800-119": "Luz de Tavira",
    "9325-032": "Estreito de Câmara de Lobos",
}


def fetch_data():
    params = {
        "action": "store_search",
        "lat": 38.306893,
        "lng": -17.050891,
        "max_results": "999",
        "search_radius": "999",
        "autoload": "1",
    }
    # Certificate is valid, but the chain is incomplete — leading to validation failure.
    result = fetch_json_data(DATA_URL, params=params, verify_cert=False)
    result = [x for x in result if x["country"] == "Portugal"]
    result = [
        {
            **x,
            "id": urlsplit(x["url"]).path.split("/")[3],
            "hours": [el.xpath(".//td//text()") for el in etree.fromstring(x["hours"], etree.HTMLParser()).xpath("//tr")],
        }
        for x in result
    ]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [
        DiffDict(e)
        for e in overpass_query('nwr[shop][shop!=pastry][~"^(name|brand|operator|website)$"~"amanhecer",i](area.country);')
    ]

    for nd in new_data:
        public_id = nd["id"]
        branch = titleize(html.unescape(nd["store"]))
        for a, b in BRANCH_FIXES:
            branch = re.sub(a, b, branch)
        is_warehouse = "Armazém" in branch
        branch = re.sub(r"[- ]*\bArmazém\b[- ]*", " ", branch).strip()
        subname = re.sub(r"^Amanhecer\s+(?![a-z])|\s+(-\s+.+|\(.+\))$|, Unipessoal.*$", "", branch)
        subname = re.sub(r"\b(" + "|".join(SUBNETS) + r")\s.+$", r"\1", subname)
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"] or 38.306893), float(nd["lng"] or -17.050891)]
        if coord[0] > 180:
            if m := re.fullmatch(r"\s*(3[789]|4[012])(\d+)", nd["lat"]):
                coord[0] = float(f"{m[1]}.{m[2]}")
            else:
                coord[0] = 38.306893
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

        d[REF] = public_id
        d["shop"] = "wholesale" if is_warehouse else (d["shop"] or "convenience")
        d["name"] = f"Amanhecer - {subname}{' - Armazém' if is_warehouse else ''}".replace("Amanhecer - Amanhecer", "Amanhecer")
        d["brand"] = "Amanhecer"
        d["brand:wikidata"] = "Q127510997"
        # d["brand:wikipedia"] = ""  # noqa: ERA001
        d["branch"] = branch

        tags_to_reset.update({"name:en", "name:pt"})

        if nd["hours"]:
            schedule = [
                {
                    "d": DAYS.index(x[0].lower()),
                    "t": x[1].replace(" ", "") if x[1] != "Fechado" else "off",
                }
                for x in nd["hours"]
            ]
            schedule = [
                {
                    "d": sorted([x["d"] for x in g]),
                    "t": k,
                }
                for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
            ]
            schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
            d["opening_hours"] = "; ".join(schedule)
            d["source:opening_hours"] = "website"

        phones = []
        # Website shows `fax` as "Telefone" and `phone` as "Telemovel" :-\
        for phone in (nd["phone"], nd["fax"]):
            phone = phone.replace(" ", "")
            if len(phone) == 13 and phone.startswith("+351"):
                phone = phone[4:]
            if len(phone) == 9:
                phones.append(f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}")
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = nd["permalink"]
        d["contact:email"] = nd["email"]
        if "lojasamanhecer" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};lojasamanhecer".strip(";")
        d["contact:instagram"] = "lojas.amanhecer"

        tags_to_reset.update({"phone", "mobile", "fax", "email", "contact:fax", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        postcode, city = nd["zip"], nd["city"]
        if " " in postcode:
            postcode, city = postcode.split(" ", 1)
        city = re.sub(
            r"\s+(bcl|cnf|eps|fnd|lga|lnh|lrs|mfr|mgr|ovr|pmz|pnf|sei|srp|tvr|vdg)$",
            "",
            city.split(",")[0],
            flags=re.IGNORECASE,
        )
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, re.sub(r"\bCôa\b", "Coa", titleize(city)))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([nd["address"], nd["address2"]]).strip("; ")

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

    write_diff("Amanhecer", REF, old_data)
