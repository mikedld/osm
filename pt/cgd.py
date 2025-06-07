#!/usr/bin/env python3

import json
import re
from multiprocessing import Pool
from pathlib import Path

import requests
from lxml import etree

from impl.common import DiffDict, cache_name, overpass_query, distance, titleize, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

DAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
CITIES = {
    "2425-617": "Monte Redondo",
    "2500-238": "Caldas da Rainha",
    "2660-310": "Santo António dos Cavaleiros",
    "2685-223": "Portela",
    "2700-292": "São Brás",
    "2710-573": "Sintra",
    "2825-355": "Costa da Caparica",
    "3450-123": "Mortágua",
    "3560-172": "Sátão",
    "4415-727": "Olival",
    "4475-615": "Castêlo da Maia",
    "4730-450": "Vila de Prado",
    "6430-183": "Mêda",
    "6440-092": "Figueira de Castelo Rodrigo",
    "7480-148": "Avis",
    "8125-410": "Vilamoura",
    "8375-109": "São Bartolomeu de Messines",
    "8900-231": "Vila Real de Santo António",
    "9900-144": "Horta (Angústias)",
    "9980-024": "Vila do Corvo",
}
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
DAYS_8_AND_19 = ",".join([f"{m} {d}" for m in MONTHS for d in ("08", "19")])
DAYS_OPEN = {
    None: ["Mo-Fr"],
    "2ª, 4ª e 6ª (3ª e 5ª, apenas se coincidirem com os dias 8 e 19 de cada mês)": [
        "Mo,We,Fr",
        f"{DAYS_8_AND_19} Tu,Th",
        # ["Mo,We,Fr", None],
        # ["Tu,Th", "on 8th and 19th of each month"],
    ],
    "3ª e 5ª (2ª, 4ª e 6ª apenas se coincidirem com os dias 8 e 19 de cada mês)": [
        "Tu,Th",
        f"{DAYS_8_AND_19} Mo,We,Fr",
        # ["Tu,Th", None],
        # ["Mo,We,Fr", "on 8th and 19th of each month"],
    ],
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h(\d{2})": r"\1:\2-",
    r"(?:De 2ª a 6ª feira, das )?(\d{2})h(\d{2})\s*(?:[-–]|às)\s*(\d{2})h(\d{2})\.?": r"\1:\2-\3:\4",
    r"(?:Todos os dias úteis: )?(\d{1})h(\d{2})\s*(?:[-–]|às)\s*(\d{2})h(\d{2})": r"0\1:\2-\3:\4",
}


def fetch_level1_data(url):
    cache_file = Path(f"{cache_name(url)}.html")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        r = requests.get(url, headers={"user-agent": "mikedld-osm/1.0"})
        r.raise_for_status()
        result = r.content.decode("utf-8")
        result_tree = etree.fromstring(result, etree.HTMLParser())
        etree.indent(result_tree)
        result = etree.tostring(result_tree, encoding="utf-8", pretty_print=True).decode("utf-8")
        if ENABLE_CACHE:
            cache_file.write_text(result)
    else:
        result = cache_file.read_text()
    result_tree = etree.fromstring(result)
    result = [
        f"{url.split('?')[0]}{href}"
        for href in result_tree.xpath("//a[contains(@class, 'agencias')]/@href")
    ]
    return result


def fetch_level2_data(url):
    cache_file = Path(f"{cache_name(url)}.html")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        r = requests.get(url, headers={"user-agent": "mikedld-osm/1.0"})
        r.raise_for_status()
        result = r.content.decode("utf-8")
        result_tree = etree.fromstring(result, etree.HTMLParser())
        etree.indent(result_tree)
        result = etree.tostring(result_tree, encoding="utf-8", pretty_print=True).decode("utf-8")
        if ENABLE_CACHE:
            cache_file.write_text(result)
    else:
        result = cache_file.read_text()
    result_tree = etree.fromstring(result)
    result = result_tree.xpath("//script[contains(text(), 'var agencias =')]/text()")[0]
    result = json.loads(re.sub(r".*var agencias =|;$", "", result, flags=re.S).replace("'", '"'))
    result = [
        {
            "branch": r[0],
            "lat": r[1],
            "lon": r[2],
            "id": str(r[3]),
            "url": "/".join(url.split("/", 3)[:3]) + result_tree.xpath(f"//a[@id='l{r[3]}']/@href")[0].split('?')[0],
            "subtitle": "".join(result_tree.xpath(f"//a[@id='l{r[3]}']//span[@class='subtitle-text-right']/text()")),
            "addr": [x.strip() for x in result_tree.xpath(f"//div[@id='addr{r[3]}']//text()") if x.strip()],
        }
        for r in result
    ]
    for r in result:
        a = r["addr"]
        k = "0Esta Agência"
        if i := next((i for i, s in enumerate(a) if k in s and not s.startswith(k)), None):
            s = a[i].split(k, 1)
            a[i:i + 1] = [s[0] + k[0], k[1:] + s[1]]
        for i in range(len(a) - 2, -1, -1):
            if a[i] in ("Telefone:", "Horário:", "Dias da semana:") or (a[i].endswith("ª") and a[i + 1].startswith("(")):
                a[i] += f" {a.pop(i + 1)}"
    return result


def schedule_time(v):
    sa = v
    sb = "<ERR>"
    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    agencias_data_url = "https://www.cgd.pt/Corporativo/Rede-CGD/Pages/Agencias.aspx"
    gabinetes_data_url = "https://www.cgd.pt/Corporativo/Rede-CGD/Pages/Gabinetes.aspx"
    new_data = fetch_level1_data(agencias_data_url) + fetch_level1_data(gabinetes_data_url)
    with Pool(4) as p:
        new_data = [d for ds in p.imap_unordered(fetch_level1_data, new_data) for d in ds]
    with Pool(4) as p:
        new_data = [d for ds in p.imap_unordered(fetch_level2_data, new_data) for d in ds]

    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[amenity=bank][name~"Caixa Geral"](area.p); );')["elements"]]

    for nd in new_data:
        public_id = nd["id"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["lat"], nd["lon"]]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 75], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["lat"]
            d.data["lon"] = nd["lon"]
            old_data.append(d)

        d[REF] = public_id
        d["amenity"] = "bank"
        d["name"] = "Caixa Geral de Depósitos"
        d["short_name"] = "CGD"
        d["brand"] = "Caixa Geral de Depósitos"
        d["brand:wikidata"] = "Q1026044"
        d["brand:wikipedia"] = "pt:Caixa Geral de Depósitos"
        d["branch"] = titleize(nd["branch"])
        d["branch:type"] = "business" if "Gabinete de Empresas" in d["branch"] else "retail"
        d["operator"] = "Caixa Geral de Depósitos"
        d["operator:wikidata"] = "Q1026044"
        d["operator:wikipedia"] = "pt:Caixa Geral de Depósitos"

        for s in (
            "Esta Agência só presta serviço de tesouraria através de equipamentos automáticos.",
            "Esta agência só presta serviço de tesouraria através de equipamentos automáticos.",
            "Área automática com levantamento e depósito de notas, moedas e cheques.",
            "A nova área automática está disponível 24h/ 7.",
            "-"
        ):
            if s in nd["addr"]:
              nd["addr"].remove(s)

        days_open = next((a for a in nd["addr"] if a.startswith("Dias da semana:")), None)
        if days_open:
            nd["addr"].remove(days_open)
            days_open = days_open.split(":", 1)[1].strip()
        days_open = DAYS_OPEN.get(days_open, ["<ERR>"])
        hours_open = next((a for a in nd["addr"] if a.startswith("Horário:")), None)
        if hours_open:
            x = hours_open
            nd["addr"].remove(hours_open)
            while hours_open.startswith("Horário:"):
                hours_open = hours_open.split(":", 1)[1].strip()
        hours_closed = next((a for a in nd["addr"] if a.startswith("Encerrada:")), None)
        if hours_closed:
            nd["addr"].remove(hours_closed)
            hours_closed = hours_closed.split(":", 1)[1].strip()
        treasury_hours_closes = next((a for a in nd["addr"] if re.match(r"^(Tesouraria [Ee]ncerrada|Encerramento tesouraria):.*", a)), None)
        if treasury_hours_closes:
            nd["addr"].remove(treasury_hours_closes)
            treasury_hours_closes = treasury_hours_closes.split(":", 1)[1].strip()
        temp_closed = next((a for a in nd["addr"] if re.match("Esta [Aa]gência está temporariamente encerrada", a)), None)
        if temp_closed:
            nd["addr"].remove(temp_closed)
            temp_closed = True
        elif hours_open == "Esta agência está temporariamente encerrada, em trabalhos de remodelação.":
            temp_closed = True
            hours_open = None
        elif re.search(r"temporariamente encerrad[ao]", nd["subtitle"]):
            temp_closed = True
        schedule = None
        if temp_closed:
            schedule = 'Mo-Su,PH off "closed temporarily"'
        elif hours_open:
            x = hours_open
            hours_open = schedule_time(hours_open)
            if hours_open != "<ERR>":
                if hours_closed:
                    o = hours_open.split("-")
                    c = schedule_time(hours_closed).split("-")
                    hours_open = f"{o[0]}-{c[0]},{c[1]}-{o[1]}"
            schedule = "; ".join([f"{d} {hours_open}" for d in days_open]) + "; Sa,Su,PH off"
        if schedule:
            d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = next((a for a in nd["addr"] if a.startswith("Telefone:")), None)
        if phone:
            nd["addr"].remove(phone)
            phone = phone.split(":", 1)[1].replace(" ", "")
            if not phone.startswith("+351"):
                phone = f"+351{phone}"
            if len(phone) == 13:
                phone = f"+351 {phone[4:7]} {phone[7:10]} {phone[10:13]}"
                if phone[5:6] == "9":
                    d["contact:mobile"] = phone
                else:
                    tags_to_reset.add("contact:mobile")
                if phone[5:6] != "9":
                    d["contact:phone"] = phone
                else:
                    tags_to_reset.add("contact:phone")
        d["contact:website"] = nd["url"]
        d["contact:facebook"] = "caixageraldedepositos"
        d["contact:youtube"] = "http://www.youtube.com/user/mediacgd"
        d["contact:instagram"] = "caixageraldedepositos"
        d["contact:linkedin"] = "https://www.linkedin.com/company/caixageraldedepositos/"

        tags_to_reset.update({"phone", "mobile", "fax", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        postcode = next((a for a in nd["addr"] if re.match(r"^\d{4}\s*-\s*\d{3}\s+.*", a)), None) or ""
        city = None
        if postcode:
            postcode, city = re.sub(r"^(\d{4}\s*-\s*\d{3})\s+(?:-\s*)?(.+)$", r"\1:\2", postcode).split(":")
            postcode = postcode.replace(" ", "")
            city = city.split("(")[0].strip().replace("  ", " ")
        if len(postcode) == 8:
            d["addr:postcode"] = postcode
        elif postcode:
            d["addr:postcode"] = "<ERR>"
        if city:
            d["addr:city"] = CITIES.get(postcode, titleize(city))
        if d.kind == "new":
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

    write_diff("Caixa Geral de Depósitos", REF, old_data, osm=True)
