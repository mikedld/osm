#!/usr/bin/env python3

import itertools
import re

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.bricomarche.pt/apoio-ao-cliente/horarios-de-loja/"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"seg(unda)?( a |-)sex(ta?)?": "Mo-Fr",
    r"seg(unda)?( [-a] |-)s[aá]b(ado)?": "Mo-Sa",
    r"(de )?seg(unda)?(-feira)?( [-a] |-)dom(ingo)?|todos os dias": "Mo-Su",
    r"s[aá]b(ado)?": "Sa",
    r"sab-dom": "Sa,Su",
    r"dom(ingo)?": "Su",
    r"dom(ingo)? e feriados?": "Su,PH",
    r"páscoa, natal e ano novo": "easter,Dec 25,Jan 01",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h?[:.h](\d{2})[hm]?(?: (?:-|[aà]s) |-)(\d{2})h?[:.h](\d{2})[hm]?": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{1})h(?: (?:-|às) |-)(\d{2})h": r"0\1:00-\2:00",
    r"(\d{1})h(?: - |-)(\d{2})[:.h](\d{2})h?": r"0\1:00-\2:\3",
    r"(\d{1})h(\d{2})-(\d{1})h": r"0\1:\2-0\3:00",
    r"(\d{1})h(\d{2})-(\d{2})h": r"0\1:\2-\3:00",
    r"(?:das )?(\d{2})[:h](\d{2})[hm]?(?: (?:-|as) |-)(\d{2})[:h](\d{2})[hm.]?": r"\1:\2-\3:\4",
    r"(?:das )?(\d{2})[:h](\d{2})h?(?: às |-)(\d{2})h": r"\1:\2-\3:00",
    r"(\d{2})h(?: (?:-|às) |-)(\d{2})h": r"\1:00-\2:00",
    r"(?:das )?(\d{2})h-(\d{2})[:h](\d{2})h?": r"\1:00-\2:\3",
    r".*\bfechados?\b.*": "off",
}
BRANCHES = {
    "Charneca da Caparica": "Charneca de Caparica",
}


def fetch_data():
    def post_process(page):
        return re.sub(r"^.*var\s+lojas\s*=\s*\[(.+?)\];.*$", r"[\1]", page, flags=re.DOTALL)

    return fetch_json_data(DATA_URL, post_process=post_process)


def schedule_time(v, mapping):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


def fixup_schedule_time(v):
    if m := re.fullmatch(r"(\d+):(\d+)-(\d+):((\d+),(\d+):(\d+)-(\d+):(\d+))", v):
        t2 = int(m[3])
        t3 = int(m[6])
        if t2 < 12 and t3 >= 12 and t2 < int(m[1]) and t3 > t2:
            v = f"{m[1]}:{m[2]}-{t2 + 12:02}:{m[4]}"
    return v


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"Brico[Mm]arch[eé]"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = str(nd["id"])
        branch = nd["name"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"] or 38.306893), float(nd["lng"] or -17.050891)]
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
        d["shop"] = "doityourself"
        d["name"] = "Bricomarché"
        d["brand"] = "Bricomarché"
        d["brand:wikidata"] = "Q2925147"
        d["brand:wikipedia"] = "en:Bricomarché"
        d["branch"] = BRANCHES.get(branch, branch)

        schedule = re.sub(r"\s{2,}", " ", re.sub(r"(sábado|domingo) - ", r"\1: ", (nd["schedule"] or "").lower().strip()))
        if (parts := list(filter(lambda x: x, re.split(r"\s*\|?\s*\b(loja|bâtidrive)\b:?\s*", schedule)))) and len(parts) > 1:
            parts = dict(itertools.batched(parts, 2))
            schedule = parts.get("loja", "")
        schedule = re.split(r"\s*[/|\r\n]+\s*", re.sub(r"(\dh|[:h]\d\d)\s*[-,]?\s*(s[aá]b|dom|fechados)", r"\1|\2", schedule))
        schedule = [re.split(r"\s*[:,]\s+|\s+das\s+", x, maxsplit=1) for x in schedule]
        schedule = [["seg-dom", x[0]] if len(x) == 1 else x for x in schedule]
        schedule = [[x[1], x[0]] if "fechado" in x[0] else x for x in schedule]
        schedule = [
            [
                schedule_time(x[0].strip("."), SCHEDULE_DAYS_MAPPING),
                ",".join([schedule_time(t, SCHEDULE_HOURS_MAPPING) for t in re.split(r"\s+e\s+", x[1])]),
            ]
            for x in schedule
        ]
        schedule = [[x[0], fixup_schedule_time(x[1])] for x in schedule]
        if len(schedule) == 2 and schedule[0][1] == schedule[1][1]:
            days = f"{schedule[0][0]},{schedule[1][0]}"
            if days == "Mo-Fr,Sa,Su":
                days = "Mo-Su"
            elif days == "Mo-Sa,Su,PH":
                days = "Mo-Su,PH"
            schedule = [[days, schedule[0][1]]]
        d["opening_hours"] = "; ".join([" ".join(x) for x in schedule])
        d["source:opening_hours"] = "website"

        phones = [
            re.sub(r"^([0-9 ]+).*", r"\1", x).replace(" ", "")
            for x in re.split(r"\s*[/,]\s*", re.sub(r"\(.+?\)", "", nd["phone_number"] or ""))
        ]
        phones = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in phones if len(x) == 9]
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if emails := list(filter(lambda x: x, re.split(r"\s*,\s*", (nd["main_email"] or "").strip()))):
            for email in set(emails) - set(d["contact:email"].split(";")):
                d["contact:email"] = f"{d['contact:email']};{email}".strip("; ")
        else:
            tags_to_reset.add("contact:email")
        d["website"] = f"https://www.bricomarche.pt/lojas/{nd['slug']}/"
        if "BricomarchePortugal" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};BricomarchePortugal".strip("; ")
        d["contact:youtube"] = "https://www.youtube.com/@BricomarchePt"
        d["contact:instagram"] = "bricomarche.portugal"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        if city := titleize(nd["locality"] or ""):
            d["addr:city"] = d["addr:city"] or city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = re.sub(r"(<br\s*/?>\s*)+", "; ", nd["address"] or "").strip()

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

    write_diff("Bricomarché", REF, old_data, osm=True)
