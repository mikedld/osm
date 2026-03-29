#!/usr/bin/env python3

import re
from itertools import batched, count, groupby

from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, format_phonenumber, overpass_query, write_diff


DATA_URL = "https://www.roady.pt/amlocator/index/ajax/"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"segunda-feira a quinta-feira": "Mo-Th",
    r"segunda(- ?feira)? a sexta(-feira)?": "Mo-Fr",
    r"(de )?(2ª|segunda-feira) (a|até) sábado": "Mo-Sa",
    r"segunda-feira a sábado e feriados": "Mo-Sa,PH",
    r"segunda-feira a domingo e feriados": "Mo-Su,PH",
    r"sexta-feira e sábado": "Fr,Sa",
    r"sábado": "Sa",
    r"sábado, domingo e feriados": "Sa,Su,PH",
    r"sábado e feriados": "Sa,PH",
    r"domingos?": "Su",
    r"domingos? e feriados": "Su,PH",
    r"feriados?": "PH",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h(\d{2})\s*(?:às|-)\s*(\d{2})h(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})[:h](\d{2})\s*(?:às|-)\s*(\d{2})[:h](\d{2})": r"\1:\2-\3:\4",
    r"(\d{2})h-(\d{2})h": r"\1:00-\2:00",
    r"encerrado": r"off",
}


def fetch_data():
    result = []
    for page_idx in count(start=1):
        params = {
            "p": page_idx,
        }
        page = fetch_json_data(DATA_URL, params=params)["items"]
        result_ids = {x["id"] for x in result}
        if not {x["id"] for x in page} - result_ids:
            break
        result.extend(
            [
                {
                    **x,
                    "name": el.xpath("//header/text()")[0],
                    "schedule": [
                        x.strip()
                        for x in el.xpath("//div[@class='roady-scheadule-list--item'][1]//text()")
                        if x.strip() not in ("", "Horário:")
                    ],
                    "phones": [
                        y.strip()
                        for x in el.xpath("//a[starts-with(@href, 'tel:')]/@href")
                        for y in x.removeprefix("tel:").split("/")
                    ],
                    "emails": [x.removeprefix("mailto:") for x in el.xpath("//a[starts-with(@href, 'mailto:')]/@href")],
                    "address": [x.strip() for x in el.xpath("//div[contains(@class, 'tx-address')][1]//text()") if x.strip()],
                }
                for x in page
                for el in [etree.fromstring(x["popup_html"], etree.HTMLParser())]
                if x["id"] not in result_ids
            ]
        )
    return result


def schedule_time(v, mapping):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def schedule_time_for(schedule, kind):
    result = []
    if isinstance(schedule, dict):
        for k, v in schedule.items():
            if kind in k:
                result.extend(batched(v, 2))
    else:
        for s in batched(schedule, 2):
            if isinstance(s[1], dict):
                for k, v in s[1].items():
                    if kind in k:
                        result.append((s[0], v))
            else:
                result.append(s)

    result = [
        [
            schedule_time(x[0], SCHEDULE_DAYS_MAPPING),
            ",".join(
                [
                    schedule_time(t, SCHEDULE_HOURS_MAPPING)
                    for t in re.split(r"\s*[|/]\s*", re.sub(r"-([^-]+)-([^-]+)-", r"-\1|\2-", x[1]))
                ]
            ),
        ]
        for x in result
    ]

    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"roady",i](area.country);')]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = str(nd["id"])
        branch = nd["name"].removeprefix("Roady ")
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"]), float(nd["lng"])]
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
        d["shop"] = "car_repair"
        d["name"] = "Roady"
        d["brand"] = "Roady"
        d["brand:wikidata"] = "Q3434112"
        d["brand:wikipedia"] = "fr:Roady"
        d["branch"] = branch

        schedule = re.sub(r"(\s*(<br\s*/?>|;)\s*)+", "; ", "; ".join(nd["schedule"]).lower()).replace("–", "-").strip()
        schedule = re.sub(r"\|\s*(dom)", r"; \1", schedule)
        if "loja" in schedule or "oficina" in schedule:
            schedule = re.split(r"[:;(|\s]*(loja e oficina|loja|oficina)[:;)|\s]*", schedule)
            if not schedule[0]:
                schedule = {k: re.split(r"\s*[:;]\s+", v) for k, v in batched(schedule[1:], 2)}
            else:
                schedule = [x for s in schedule for x in re.split(r"\s*[:;]\s+", s)]
                for i in range(len(schedule) - 1, -1, -1):
                    if schedule[i] in ("loja e oficina", "loja", "oficina"):
                        schedule[i] = (schedule[i], schedule[i + 1])
                        schedule.pop(i + 1)
                schedule = [[dict(g)] if k else list(g) for k, g in groupby(schedule, lambda x: isinstance(x, tuple))]
                schedule = [x for s in schedule for x in s]
        else:
            schedule = re.split(r"\s*[:;]\s+", schedule)
        if main_schedule := schedule_time_for(schedule, "oficina"):
            d["opening_hours"] = "; ".join([" ".join(x) for x in main_schedule])
        if store_schedule := schedule_time_for(schedule, "loja"):
            if store_schedule != main_schedule:
                d["opening_hours:store"] = "; ".join([" ".join(x) for x in store_schedule])
            else:
                tags_to_reset.add("opening_hours:store")
        if main_schedule or store_schedule:
            d["source:opening_hours"] = "website"

        phones = [format_phonenumber(x) for x in nd["phones"]]
        if phones := [x for x in phones if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        faxes = [str(x) for x in (nd.get("fax"),) if x]
        faxes = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in faxes if len(x) == 9]
        if faxes:
            d["contact:fax"] = ";".join(faxes)
        else:
            tags_to_reset.add("contact:fax")
        if emails := nd["emails"]:
            for email in set(emails) - set(d["contact:email"].split(";")):
                d["contact:email"] = f"{d['contact:email']};{email}".strip("; ")
        else:
            tags_to_reset.add("contact:email")
        d["website"] = "https://www.roady.pt/"
        if "Roady.Centro.Auto" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};Roady.Centro.Auto".strip(";")
        d["contact:instagram"] = "Roady.Centro.Auto"

        tags_to_reset.update({"phone", "mobile", "fax", "email", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        postcode, city = nd["address"][-1].split(" ", maxsplit=1)
        if city:
            d["addr:city"] = d["addr:city"] or city
        if postcode := postcode.removesuffix("-000"):
            if len(postcode) == 4 and d["addr:postcode"].startswith(f"{postcode}-"):
                postcode = d["addr:postcode"]
            if len(postcode) == 4:
                postcode += "-000"
            d["addr:postcode"] = postcode
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(nd["address"][0:-1])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Roady", REF, old_data, osm=True)
