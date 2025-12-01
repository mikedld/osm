#!/usr/bin/env python3

import itertools
import re

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.roady.pt/lojas"

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
    def post_process(page):
        return re.sub(r"^.*var\s+handover\s*=\s*\{(.+?)\};.*$", r"{\1}", page, flags=re.DOTALL)

    return fetch_json_data(DATA_URL, post_process=post_process)["resources"]["stores"]


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
                result.extend(itertools.batched(v, 2))
    else:
        for s in itertools.batched(schedule, 2):
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

        d[REF] = public_id
        d["shop"] = "car_repair"
        d["name"] = "Roady"
        d["brand"] = "Roady"
        d["brand:wikidata"] = "Q3434112"
        d["brand:wikipedia"] = "fr:Roady"
        d["branch"] = branch

        schedule = re.sub(r"(\s*(<br\s*/?>|;)\s*)+", "; ", nd["schedule"].lower()).replace("–", "-").strip()
        schedule = re.sub(r"\|\s*(dom)", r"; \1", schedule)
        if "loja" in schedule or "oficina" in schedule:
            schedule = re.split(r"[:;(|\s]*(loja e oficina|loja|oficina)[:;)|\s]*", schedule)
            if not schedule[0]:
                schedule = {k: re.split(r"\s*[:;]\s+", v) for k, v in itertools.batched(schedule[1:], 2)}
            else:
                schedule = [x for s in schedule for x in re.split(r"\s*[:;]\s+", s)]
                for i in range(len(schedule) - 1, -1, -1):
                    if schedule[i] in ("loja e oficina", "loja", "oficina"):
                        schedule[i] = (schedule[i], schedule[i + 1])
                        schedule.pop(i + 1)
                schedule = [[dict(g)] if k else list(g) for k, g in itertools.groupby(schedule, lambda x: isinstance(x, tuple))]
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

        phones = [str(x) for x in (nd["phone_number"], nd["cell_phone"]) if x]
        phones = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in phones if len(x) == 9]
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        faxes = [str(x) for x in (nd.get("fax"),) if x]
        faxes = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in faxes if len(x) == 9]
        if faxes:
            d["contact:fax"] = ";".join(faxes)
        else:
            tags_to_reset.add("contact:fax")
        if emails := [x for x in (nd["main_email"], nd["secundary_email"]) if x]:
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

        if city := titleize(nd["locality"]):
            d["addr:city"] = d["addr:city"] or city
        if postcode := nd["postal_code"].removesuffix("-000"):
            if len(postcode) == 4 and d["addr:postcode"].startswith(f"{postcode}-"):
                postcode = d["addr:postcode"]
            if len(postcode) == 4:
                postcode += "-000"
            d["addr:postcode"] = postcode
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = re.sub(r"(<br\s*/?>\s*)+", "; ", nd["address"]).strip()

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

    write_diff("Roady", REF, old_data, osm=True)
