#!/usr/bin/env python3

import html
import itertools
import re

from impl.common import (
    DiffDict,
    distance,
    fetch_json_data,
    format_phonenumber,
    opening_weekdays,
    overpass_query,
    titleize,
    write_diff,
)


DATA_URL = "https://www.amanhecer.pt/mobify/bundle/6/site/prod/component/en-US/e85fc00d52a0100b79e376503e3c3b5d/webruntime/csrIslandContainerXes1r3wxwkjt6lxsa3res4fyuiygb5rlgqm1m3p180gnwlq19l9asr4rq3hah7tsmq_cmp.js"

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
    def post_process(page):
        page = re.sub(r"^.*\{storesJson:'\[(.+)\]',.*$", r"[\1]", page, flags=re.DOTALL)
        page = page.replace("\\'", "'")
        page = page.replace('\\\\"', '\\"')
        return page

    result = fetch_json_data(DATA_URL, post_process=post_process)
    result = [
        {
            **x,
            "id": re.sub(r"\D+", "", re.sub(r"^.*?(\d+)@amanhecer\.pt.*$", r"\1", x["email"])),
            "hours": [],
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

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]

        branch = titleize(html.unescape(nd["name"]))
        for a, b in BRANCH_FIXES:
            branch = re.sub(a, b, branch)
        is_warehouse = "Armazém" in branch
        branch = re.sub(r"[- ]*\bArmazém\b[- ]*", " ", branch).strip()
        subname = re.sub(r"^Amanhecer\s+(?![a-z])|\s+(-\s+.+|\(.+\))$|, Unipessoal.*$", "", branch)
        subname = re.sub(r"\b(" + "|".join(SUBNETS) + r")\s.+$", r"\1", subname)
        tags_to_reset = set()

        d = None  # next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["latitude"], nd["longitude"]]
        if d is None:
            ds = [x for x in old_data if x.data["id"] in old_node_ids and distance([x.lat, x.lon], coord) < 250]
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

        # d[REF] = public_id  # noqa: ERA001
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

        if phones := [x for x in (format_phonenumber(nd["phone"]),) if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["contact:email"] = nd["email"]
        if "lojasamanhecer" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};lojasamanhecer".strip(";")
        d["contact:instagram"] = "lojas.amanhecer"

        tags_to_reset.update({"phone", "mobile", "fax", "email", "contact:fax", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        postcode, city = re.sub(r"\s*-\s*", "-", nd["postalCode"]), (nd["city"] or nd["zone"])
        city = re.sub(
            r"\s+(agb|amt|arl|avs|bbr|bcl|brb|brg|cdv|cld|cmn|cnf|ctm|eps|fnd|gmr|lga|lmg|lnh|lrs|mbr|mfr|mgr|ovr|pmz|pnf|pvl|pvz|rgr|scr|sei|smp|snt|srp|str|tvd|tvr|vdg|vgs)$",
            "",
            city.split(",")[0],
            flags=re.IGNORECASE,
        )
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, re.sub(r"\bCôa\b", "Coa", titleize(city)))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["address"].strip("; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Amanhecer", REF, old_data)
