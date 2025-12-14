#!/usr/bin/env python3

import re

from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://wells.pt/lojas-wells"

REF = "ref"

BRANCH_ABBREVS = (
    (r"\bAlges\b", "Algés"),
    (r"\bAntonio\b", "António"),
    (r"\bAv\b\.?", "Avenida"),
    (r"\bAzeitao\b", "Azeitão"),
    (r"\bBd\b", "Bom Dia"),
    (r"\bCnt\b", "Continente"),
    (r"\bD'Aire\b", "Daire"),
    (r"\bDr\b", "Doutor"),
    (r"\bEstacao\b", "Estação"),
    (r"\bEvora\b", "Évora"),
    (r"\bFamalicao\b", "Famalicão"),
    (r"\bFanzeres\b", "Fânzeres"),
    (r"\bFig\. Foz\b", "Figueira da Foz"),
    (r"\bFrs\b", "Franquia"),
    (r"^(Franquia) (.+)$", r"\2 \1"),
    (r"\bGpl\b", "Gran Plaza"),
    (r"\bJoao\b", "João"),
    (r"\b(MDL|Mh)\b", "Modelo"),
    (r"\bMte\b", "Monte"),
    (r"\bOdiaxere\b", "Odiáxere"),
    (r"\bPdl\b", "Ponta Delgada"),
    (r"\bPonte de Sôr\b", "Ponte de Sor"),
    (r"\bQta?\b", "Quinta"),
    (r"\bS João\b", "São João"),
    (r"\bS\.\s?J\.", "São João"),
    (r"\bS\.Atº\b", "Santo António"),
    (r"\bS\. F\. Marinha\b", "São Félix da Marinha"),
    (r"\bS\.", "São"),
    (r"\bStª? Maria\b", "Santa Maria"),
    (r"\bSto\b", "Santo"),
    (r"\bUbbo\b", "UBBO"),
    (r"\bV\.\s?F\. Xira\b", "Vila Franca de Xira"),
    (r"\bV\. N\.", "Vila Nova"),
    (r"\bV\.", "Vila"),
    (r"\bVitoria\b", "Vitória"),
)
BRANCHES = {
    "Aqua Portimao": "Aqua Portimão",
    "Caldas Rainha Continente Bom Dia": "Caldas da Rainha Continente Bom Dia",
    "Caldas Rainha Continente Modelo": "Caldas da Rainha Continente Modelo",
    "Castelo Branco Parque Barrocal": "Castelo Branco Parque do Barrocal",
    "Pacos Ferreira Ferrara Plaza": "Paços de Ferreira Ferrara Plaza",
    "Portimao Cabeço do Mocho": "Portimão Cabeço do Mocho",
    "Portimao Continente Shopping": "Portimão Continente Shopping",
    "São João da Madeira 8 Avenida": "São João da Madeira 8ª Avenida",
}
SCHEDULE_DAYS_MAPPING = {
    r"^$|segunda a domingo-?|todos os dias?": "Mo-Su",
    r"seg\.? a sex\.?": "Mo-Fr",
    r"seg\.? a sáb\.?": "Mo-Sa",
    r"dom(ingo|\.?) a (qui(nta(-feira)?)?\.?|5ªf)": "Su-Th",
    r"s[aá]b\.": "Sa",
    r"dom\.?": "Su",
    r"sáb\.? [ea] dom\.": "Sa,Su",
    r"sex\.": "Fr",
    r"sex\. e sáb\.": "Fr,Sa",
    r"feriados": "PH",
    r"dom\.? e feriados": "Su,PH",
    r"sáb\.? dom\.? e feriados": "Sa,Su,PH",
    r"sex(tas|\.),? sáb(ados|\.) e v[eé]sp(era|\.)( de)? feriados?": "Fr,Sa,PH -1 day",
    r"véspera de feriado, sex e sáb": "Fr,Sa,PH -1 day",
    r"vésperas de feriados": "PH -1 day",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h\s*às\s*(\d{2})h": r"\1:00-\2:00",
    r"(\d{2})h\s*às\s*(\d{2})h(\d{2})": r"\1:00-\2:\3",
    r"(\d{1})[:h](\d{2})h?\s*(?:às|-)\s*(\d{2})[:h](\d{2})h?": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{2})[:h](\d{2})h?\s*(?:às|-)\s*(\d{2})[:h](\d{2})h?": r"\1:\2-\3:\4",
    r"encerrad[ao]|:h às :h": r"off",
}
POSTCODES = {
    "2853": "9500-465",
}
CITIES = {
    "1494-044": "Algés",
    "1495-241": "Algés",
    "1658-581": "Caneças",
    "2350-537": "Torres Novas",
    "2685-223": "Portela",
    "2725-397": "Mem Martins",
    "2725-537": "Mem Martins",
    "2825-004": "Caparica",
    "2975-333": "Quinta do Conde",
    "2590-041": "Sobral de Monte Agraço",
    "3080-228": "Figueira da Foz",
    "3720-256": "Oliveira de Azeméis",
    "4200-008": "São Mamede de Infesta",
    "4420-490": "Valbom",
    "4450-565": "Leça da Palmeira",
    "4460-384": "São Mamede de Infesta",
    "4505-374": "Fiães",
    "4525-117": "Canedo",
    "4535-211": "Mozelos",
    "4535-401": "Santa Maria de Lamas",
    "4764-501": "Vila Nova de Famalicão",
    "4820-273": "Fafe",
    "4920-260": "Vila Nova de Cerveira",
    "7500-200": "Vila Nova de Santo André",
    "8400-656": "Parchal",
    "8900-258": "Vila Real de Santo António",
    "9050-299": "São Gonçalo",
    "9500-376": "Ponta Delgada",
    "9500-465": "Ponta Delgada",
    "9560-414": "Lagoa",
    "9600-516": "Ribeira Grande",
    "9900-038": "Horta (Angústias)",
}


def fetch_data():
    def post_process(page):
        page_tree = etree.fromstring(page, etree.HTMLParser())
        return page_tree.xpath("//script[@id='locations-data']/text()")[0]

    result = fetch_json_data(DATA_URL, post_process=post_process)
    result = [
        {
            "info": etree.fromstring(x["infoWindowHtml"], etree.HTMLParser()),
            **{k: v for k, v in x.items() if k not in ("infoWindowHtml",)},
        }
        for x in result
    ]
    return [
        {
            "id": x["info"][0][0].attrib["data-store-id"],
            "phones": [
                re.sub(r"^\s*([';]\s*)?(([^0-9:]+)\s*:?)?\s*(\d+)/?$", r"\3:\4", y.lower())
                .replace("geral", "")
                .replace("saude", "saúde")
                .split(":")
                for y in x["info"].xpath("//a[contains(@class, 'w-store-locator-phone')]/text()")
            ],
            "services": [y.strip() for y in x["info"].xpath(".//p[contains(@class, 'w-store-service')]/text()")],
            **{k: v for k, v in x.items() if k not in ("info",)},
        }
        for x in result
    ]


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"well\'?s",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        branch = re.sub(r"^Wells\s+", "", titleize(nd["name"]))
        is_opt = re.search(r"\b(óp?tica|opt)\b", branch, flags=re.IGNORECASE)
        branch = re.sub(r"\b(óp?tica|opt)\b", " ", branch, flags=re.IGNORECASE).strip()
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None and nd["latitude"] is not None and nd["longitude"] is not None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["latitude"] or 38.306893
            d.data["lon"] = nd["longitude"] or -17.050891
            old_data.append(d)
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = "optician" if is_opt else "chemist"
        d["name"] = "Wells Ótica" if is_opt else "Wells"
        d["brand"] = "Wells"
        d["brand:wikidata"] = "Q115388598"
        d["brand:wikipedia"] = "pt:Wells (lojas)"
        for r in BRANCH_ABBREVS:
            branch = re.sub(r[0], r[1], branch)
        d["branch"] = BRANCHES.get(branch, branch)

        tags_to_reset.update({"amenity", "dispensing", "healthcare"})

        schedule = re.split(
            r"\s*<p>\s*",
            re.sub(r"(\d+[h:]\d+)\.", r"\1;", re.sub(r"horário:|^<p>|</p>", "", nd["storeHours"].lower())),
            flags=re.DOTALL,
        )
        schedule = [
            [y.strip() for y in re.sub(r"^([^0-9:]*?)\s*(?=\d(?!ª)|das |encerrad|:h)", r"\1: ", x).split(":", 1)]
            for x in schedule
        ]
        for s in schedule:
            if len(s) != 2:
                s[:] = [f"<ERR:{s}>"]
                continue

            sa = s[0]
            sb = f"<ERR:{sa}>"
            for sma, smb in SCHEDULE_DAYS_MAPPING.items():
                if re.fullmatch(sma, sa) is not None:
                    sb = re.sub(sma, smb, sa)
                    break
            s[0] = sb

            ss = []
            for sa in re.split(r"\s*(?:\be\b|/|,)\s*", s[1]):
                sb = f"<ERR:{sa}>"
                for sma, smb in SCHEDULE_HOURS_MAPPING.items():
                    if re.fullmatch(sma, sa) is not None:
                        sb = re.sub(sma, smb, sa)
                        break
                ss.append(sb)
            s[1] = ",".join(ss)
        if len(schedule) >= 2 and schedule[0][0] == "Mo-Sa" and schedule[1][0] == "Su,PH" and schedule[0][1] == schedule[1][1]:
            schedule = [["Mo-Su,PH", schedule[0][1]], *schedule[2:]]
        if len(schedule) >= 2 and schedule[0][0] == "Mo-Sa" and schedule[1][0] == "PH" and schedule[0][1] == schedule[1][1]:
            schedule = [["Mo-Sa,PH", schedule[0][1]], *schedule[2:]]
        if len(schedule) == 2 and schedule[0][0] == "Mo-Su" and schedule[1][0] == "Su,PH":
            schedule[0][0] = "Mo-Sa"
        schedule = "; ".join([" ".join(x) for x in schedule])
        d["opening_hours"] = schedule

        phones = []
        for comment, phone in nd["phones"]:
            if len(phone) == 9:
                phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
                if comment:
                    phone += f' "{comment}"'
                phones.append(phone)
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://wells.pt/"
        d["contact:facebook"] = "WellsPT"
        d["contact:youtube"] = "https://www.youtube.com/@Wells_oficial"
        d["contact:instagram"] = "wells_oficial"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        postcode = POSTCODES.get(public_id, nd["postalCode"])
        if postcode.endswith("-000") and postcode[:4] == d["addr:postcode"][:4]:
            postcode = d["addr:postcode"]
        if len(postcode) == 4:
            postcode += "-000"
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, titleize(nd["city"].strip()))

        if (
            not d["addr:street"]
            and not (d["addr:housenumber"] or d["addr:housename"] or d["nohousenumber"])
            and not d["addr:place"]
            and not d["addr:suburb"]
        ):
            d["x-dld-addr"] = "; ".join([nd["address1"], nd["address2"]])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Wells", REF, old_data)
