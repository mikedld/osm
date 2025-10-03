#!/usr/bin/env python3

import itertools
import re
from multiprocessing import Pool
from urllib.parse import urljoin

from lxml import etree
from more_itertools import flatten

from impl.common import DiffDict, distance, fetch_html_data, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.turiscar.pt/pt/estacoes"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"segunda a sexta feira": "Mo-Fr",
    r"segunda-feira a sexta-feira": "Mo-Fr",
    r"segunda-feira a domingo": "Mo-Su",
    r"sábado": "Sa",
    r"sábados": "Sa",
    r"sábado, domingo e feriados": "Sa,Su,PH",
    r"sábados, domingos e  feriados": "Sa,Su,PH",
    r"sábados, domingos e feriados": "Sa,Su,PH",
    r"domingos": "Su",
    r"domingo e feriados": "Su,PH",
    r"domingos e feriados": "Su,PH",
    r"fins-de-semana e feriados": "Sa,Su,PH",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h\s*(?:às|-)\s*(\d{2})h": r"\1:00-\2:00",
    r"(?:das\s+)?(\d{2})[:h](\d{1})\s*(?:às|-)\s*(\d{2})[:h](\d{2})": r"\1:0\2-\3:\4",
    r"(?:das\s+)?(\d{2})[:h](\d{2})\s*(?:às|-)\s*(\d{2})[:h](\d{2})": r"\1:\2-\3:\4",
    r"encerrado": "off",
}


def fetch_level1_data():
    def post_process(page):
        page = re.sub(r"^.*var\s+aLocais\s*=\s*\[(.+?)\];.*$", r"[\1]", page, flags=re.DOTALL)
        page = page.replace('"', '\\"')
        page = page.replace("'", '"')
        return page

    result = fetch_json_data(DATA_URL, post_process=post_process)
    result = [
        {
            "id": x[0],
            "name": x[1],
            "lat": x[2],
            "lon": x[3],
            "addr": [
                y.strip()
                for y in etree.fromstring(x[4].split("</strong>")[1].split("<center>")[0], etree.HTMLParser()).xpath("//text()")
                if y.strip()
            ],
            "url": urljoin(DATA_URL, etree.fromstring(x[4], etree.HTMLParser()).xpath("//a/@href")[0]),
        }
        for x in result
    ]
    return result


def fetch_level2_data(data):
    result_tree = fetch_html_data(data["url"])
    return {
        **data,
        "contacts": [
            x.strip().lower()
            for x in result_tree.xpath("//div[contains(@class, 'map-detail-info') and ./h4/text() = 'Contactos']//text()")
            if x.strip() and x.strip() != "Contactos"
        ],
        "schedule": [
            x.strip().lower().replace("–", "-")
            for x in result_tree.xpath(
                "//div[contains(@class, 'map-detail-info') and ./h4/text() = 'Horário de Funcionamento']//text()"
            )
            if x.strip() and x.strip() != "Horário de Funcionamento"
        ],
    }


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[amenity][name~"Turiscar"](area.country);')]

    for nd in new_data:
        public_id = str(nd["id"])
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["lat"], nd["lon"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["lat"]
            d.data["lon"] = nd["lon"]
            old_data.append(d)

        tags_to_reset = set()

        d[REF] = public_id
        d["amenity"] = "car_rental"
        d["name"] = "Turiscar"
        d["branch"] = nd["name"]

        if schedule := nd["schedule"]:
            schedule = [
                re.sub(r"^(?:encerra(?:do:)?(?:\s+aos)?\s+)(.+?)$|^(.+?)(?:\s*-\s*encerrado)$", r"\1\2 : encerrado", x)
                for x in schedule
            ]
            schedule = [re.sub(r"^(sábado|.*?-feira):?\s*(?=\d)(.+?)$", r"\1 : \2", x) for x in schedule]
            for i in range(len(schedule) - 1, 0, -1):
                if re.match(r"\d", schedule[i]) and re.match(r"\d", schedule[i - 1]):
                    schedule[i - 1] += f" e {schedule.pop(i)}"
            schedule = list(flatten(x.split(" : ") for x in schedule))
            schedule = [list(x) for x in itertools.batched(schedule, 2)]
            for s in schedule:
                sa = s[0]
                sb = f"<ERR:{sa}>"
                for sma, smb in SCHEDULE_DAYS_MAPPING.items():
                    if re.fullmatch(sma, sa) is not None:
                        sb = re.sub(sma, smb, sa)
                        break
                s[0] = sb

                ss = []
                for sa in re.split(r"\s+e\s+|\s*/\s*", s[1]):
                    sb = f"<ERR:{sa}>"
                    for sma, smb in SCHEDULE_HOURS_MAPPING.items():
                        if re.fullmatch(sma, sa) is not None:
                            sb = re.sub(sma, smb, sa)
                            break
                    ss.append(sb)
                s[1] = ",".join(ss)
            schedule = [" ".join(x) for x in schedule]
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        if phones := [x[4:].split("chamada")[0].strip(" -").split(" ", 1) for x in nd["contacts"] if x.startswith("tel.")]:
            phones = list(filter(lambda x: len(x) == 2, phones))
            for i, phone in enumerate(phones):
                comment = phone[0]
                phone = re.sub(r"\s", "", phone[1])
                if len(phone) == 13:
                    phone = f"+351 {phone[4:7]} {phone[7:10]} {phone[10:13]}"
                    phones[i] = f'{phone} "{comment}"'
                else:
                    phones[i] = ""
        if phones := list(filter(lambda x: x, phones)):
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if emails := [x.split(":", 1)[1].strip() for x in nd["contacts"] if x.startswith("e-mail:")]:
            d["contact:email"] = ";".join(emails)
        else:
            tags_to_reset.add("contact:email")
        d["website"] = nd["url"]
        d["contact:facebook"] = "turiscar"
        d["contact:instagram"] = "turiscarrentacar"
        d["contact:twitter"] = "turiscarpt"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        if len(nd["addr"]) == 1:
            nd["addr"].append(" ")
        postcode, city = nd["addr"].pop(-1).split(" ", 1)
        if postcode:
            d["addr:postcode"] = postcode
        if city:
            d["addr:city"] = titleize(city.strip(" -"))
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(nd["addr"])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == str(nd["id"])):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Turiscar", REF, old_data, osm=True)
