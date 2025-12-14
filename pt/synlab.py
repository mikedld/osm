#!/usr/bin/env python3

import json
import re
from itertools import batched, groupby
from urllib.parse import urljoin

from impl.common import DiffDict, distance, fetch_html_data, format_phonenumber, overpass_query, write_diff


DATA_URL = "https://www.synlab.pt/onde-estamos"

REF = "ref"

BRANCHES = {
    "CCES S. João de Ver": "CCES São João de Ver",
    "Lisboa (Av. Paris)": "Lisboa (Avenida de Paris)",
    "S. João de Ver": "São João de Ver",
    "Vila Nova S. Bento": "Vila Nova de São Bento",
}
SCHEDULE_DAYS_MAPPING = {
    "n/a": "",
    "2ª feira": "Mo",
    "2ª, 3ª feira": "Mo,Tu",
    "2ª, 4ª, 6ª feira": "Mo,We,Fr",
    "2ª, 4ª e 6ª feira": "Mo,We,Fr",
    "2ª, 4ª feira": "Mo,We",
    "2.ª, 4.ª feira e sábado": "Mo,We,Sa",
    "2ª, 5ª  feira": "Mo,Th",
    "2ª, 5ª e 6ª feira": "Mo,Th,Fr",
    "2ª a 5ª feira": "Mo-Th",
    "3ª feira": "Tu",
    "3ª, 4ª feira": "Tu,We",
    "3ª e 5ª feira": "Tu,Th",
    "3ª, 5ª feira": "Tu,Th",
    "3ª, 5ª feira e sábado": "Tu,Th,Sa",
    "3ª, 6ª feira": "Tu,Fr",
    "4ª feira": "We",
    "4ª, 5ª, 6ª feira": "We-Fr",
    "4ª, 6ª feira": "We,Fr",
    "4ª e 6ª feira": "We,Fr",
    "5ª feira": "Th",
    "5.ª feira": "Th",
    "6ª feira": "Fr",
    "6.ª feira": "Fr",
    "sábado": "Sa",
    "sábados": "Sa",
    "sábado e domingo": "Sa,Su",
    "sábados e domingos": "Sa,Su",
    "domingo": "Su",
    "domingo e feriados": "Su,PH",
    "sábado, domingo e feriados": "Sa,Su,PH",
    "dias úteis": "Mo-Fr",
    "todos os dias": "Mo-Su",
    "24 horas": "24/7",
    "por marcação": '"por marcação"',
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1}):(\d{2}) às (\d{2}):(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{1}):(\d{2}) às (\d{2}):(\d{2}) e das (\d{2}):(\d{2}) às (\d{2}):(\d{2})": r"0\1:\2-\3:\4,\5:\6-\7:\8",
    r"(\d{2})[:.](\d{2})\s*[àá]s\s*(\d{2})[:.](\d{2})": r"\1:\2-\3:\4",
    r"(\d{2}):(\d{2}) às (\d{2}):(\d{2}) e(?: das)? (\d{2}):(\d{2}) (?:a|às) (\d{2}):(\d{2})": r"\1:\2-\3:\4,\5:\6-\7:\8",
    r"(\d{2}):(\d{2}) às (\d{2}):(\d{2}) \(por marcação\)": r'\1:\2-\3:\4 "por marcação"',
}
CITIES = {
    "2840-009": "Seixal",
    "4700-068": "Braga",
    "4710-426": "Braga",
    "8200-856": "Guia",
}


def fetch_data():
    def parse_js_string(value):
        return (json.loads(value.strip("; ")) or "").strip('" ')

    result_tree = fetch_html_data(DATA_URL)
    result = [
        dict(re.findall(r"var (\w+) = (.+?);\n", x, flags=re.DOTALL))
        for x in re.findall(r"(var lat =.*?\n\n)", result_tree.xpath("//script[@nonce]/text()")[0], flags=re.DOTALL)
    ]
    result = [
        {
            # "id": str(uuid.uuid5(uuid.NAMESPACE_URL, "synlab:" + x["slug"].strip('" ').split("/")[2])),  # noqa: ERA001
            "lat": float(re.sub(r'.*"([^"]+)".*', r"\1", x["lat"]).replace(",", ".")),
            "lng": float(re.sub(r'.*"([^"]+)".*', r"\1", x["lng"]).replace(",", ".")),
            "name": parse_js_string(x["name"]),
            "phoneNumber": parse_js_string(x["phoneNumber"]),
            "email": parse_js_string(x["email"]),
            "atd": parse_js_string(x["atd"]).strip(". ").lower(),
            "atdClinics": parse_js_string(x["atdClinics"]).strip(". ").lower(),
            "address": parse_js_string(x["address"]),
            "postalCode": [parse_js_string(c) for c in x["postalCode"].split(' + " " + ')],
            "parking": parse_js_string(x["parking"]),
            "slug": parse_js_string(x["slug"]),
        }
        for x in result
    ]
    return result


def fix_branch(name):
    branch = re.sub(r"^SYNLAB\s+", "", name)
    branch = re.sub(r"\bClinica\b", "Clínica", branch)
    branch = re.sub(r"\bPoliClínica\b", "Policlínica", branch)
    branch = re.sub(r"\bHospital( da)? Luz( -)? ", "Hospital da Luz - ", branch)
    branch = re.sub(r"\b(Clínica (?:da Luz|Lusíadas)|Hospital Lusíadas)( -)? ", r"\1 - ", branch)
    branch = re.sub(r"\bDr\. ", "Doutor ", branch)
    branch = re.sub(r"\bSra\. ", "Senhora ", branch)
    branch = BRANCHES.get(branch, branch)
    return branch


def schedule_time(v):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def process_valid_schedule(schedule):
    return [f"{SCHEDULE_DAYS_MAPPING.get(x[0], f'<ERR:{x[0]}>')} {schedule_time(x[1])}" for x in schedule]


def process_schedule(schedule):
    schedule = re.sub(r"(dias úteis|sábado|feriados) (?=\d)", r"\1 das ", schedule)
    urgency_274 = False
    if schedule.endswith("(urgência 24/7)"):
        schedule = schedule[:-15].strip()
        urgency_274 = True
    schedule = re.split(
        r"\s*[;|]\s*|(?<!\bfeira)(?<!ª)(?<!\bsábado)(?<!\bsábados)(?<!\bdomingo)(?<!\bdomingos)\b\s+e\s+\b(?!das\b|\d)"
        r"|"
        r"(piso\s+\S+\s+\([^)]+\)|edifício\s+\S+|atendimento|levantamento de resultados)\s*(?:[:-]\s*)?",
        schedule,
    )
    schedule = [x.split(" das ", 1) for x in schedule if x]
    for x in schedule:
        if len(x) != 2:
            schedule = [next(g)[0] if k == 1 else list(g) for k, g in groupby(schedule, key=lambda x: len(x))]
            schedule = dict(batched(schedule, 2)) if len(schedule) > 1 else schedule[0]
            break
    if isinstance(schedule, str):
        schedule = SCHEDULE_DAYS_MAPPING.get(schedule, f"<ERR:{schedule}>")
    elif isinstance(schedule, dict):
        schedule = " || ".join(["; ".join([f'{x} "{k}"' for x in process_valid_schedule(v)]) for k, v in schedule.items()])
    else:
        schedule = "; ".join(process_valid_schedule(schedule))
    if urgency_274:
        schedule += ' || 24/7 "urgência"'
    return schedule


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[healthcare][~"^(name|brand)$"~"synlab",i](area.country);')]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = "<NONE>"  # nd["id"]
        branch = fix_branch(nd["name"])
        tags_to_reset = set()

        d = None  # next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["lat"], nd["lng"]]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
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
        d["healthcare"] = "laboratory"
        d["healthcare:speciality"] = "biology"
        d["name"] = "Synlab"
        d["brand"] = "Synlab"
        d["brand:wikidata"] = "Q2376015"
        d["brand:wikipedia"] = "en:Synlab Group"
        d["branch"] = branch

        tags_to_reset.add("not:brand:wikidata")

        d["opening_hours"] = process_schedule(nd["atd"])
        d["opening_hours:analysis"] = process_schedule(nd["atdClinics"])
        d["source:opening_hours"] = "website"

        phones = re.split(r"[/;]", re.sub(r"\D", "", nd["phoneNumber"]))
        phones = [[x[:9], x[9:]] if len(x) == 18 else [x] for x in phones]
        phones = [format_phonenumber(x) for y in phones for x in y]
        if phones := [x for x in phones if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["contact:email"] = ";".join(
            [x.strip() for x in re.sub(r"(@[^@]+?\.(?:com|pt))(?=\w.*@)", r"\1;", nd["email"]).split(";")]
        )
        d["website"] = urljoin(DATA_URL, nd["slug"])
        d["contact:facebook"] = "synlabPT"
        d["contact:instagram"] = "synlabpt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/synlabportugal/"

        tags_to_reset.update({"phone", "mobile", "fax", "email", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        d["addr:postcode"], d["addr:city"] = nd["postalCode"]
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["address"].replace("\n", "; ")

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        d.revert(REF)
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Synlab", REF, old_data)
