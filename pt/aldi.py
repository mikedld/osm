#!/usr/bin/env python3

import itertools
import re

from unidecode import unidecode

from impl.common import DiffDict, fetch_json_data, overpass_query, titleize, distance, opening_weekdays, write_diff


DATA_URL = "https://locator.uberall.com/api/storefinders/ALDINORDPT_YTvsWfhEG5TCPruM6ab6sZIi0Xodyx/locations/all"

REF = "ref"

CITIES = {
    "1350-321": "Lisboa",
    "2710-694": "Quinta da Beloura I",
    "4405-520": "Vilar do Paraíso",
    "4420-356": "Gondomar",
    "8700-224": "Olhão",
}
STREET_ABBREVS = [
    [r"\bav\.? ", "avenida "],
    [r"\best\. ", "estrada "],
    [r"\bestrada nacional ", "EN "],
    [r"\bdr\. ", "doutor "],
    [r"\be?n(\d+)\b", r"en \1"],
    [r"\br\. ", "rua "],
]


def fetch_data():
    return fetch_json_data(DATA_URL)["response"]["locations"]


def get_url_part(value):
    e = unidecode(value)
    e = re.sub(r"[\\.,/#!$%^&*|;:{}=\-_`'~()\[\]]", " ", e)
    e = re.sub(r"\s{1,}", "-", e)
    return e.lower()


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"(^| )[Aa][Ll][Dd][Ii]( |$)"](area.country);')]

    for nd in new_data:
        public_id = nd["identifier"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["lat"]), float(nd["lng"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{int(public_id[2:])}"
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["lng"])
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["name"] = "Aldi"
        d["brand"] = "Aldi"
        d["brand:wikidata"] = "Q41171373"
        d["brand:wikipedia"] = "pt:Aldi"
        if branch := re.sub(r"^ALDI ", "", nd["name"]).strip():
            d["branch"] = branch

        schedule = [
            {
                "d": x["dayOfWeek"] - 1,
                "t": f"{x['from1']}-{x['to1'][:5]}"
            }
            for x in nd["openingHours"]
        ]
        schedule = [
            {
                "d": sorted([x["d"] for x in g]),
                "t": k
            }
            for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
        ]
        schedule = [
            f"{opening_weekdays(x['d'])} {x['t']}"
            for x in sorted(schedule, key=lambda x: x["d"][0])
        ]
        if schedule:
            d["opening_hours"] = "; ".join(schedule)
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        d["contact:website"] = f"https://www.aldi.pt/tools/lojas-e-horarios-de-funcionamento.html/l/{get_url_part(nd['city'])}/{get_url_part(nd['streetAndNumber'])}/{nd['id']}"
        d["contact:email"] = "portugal@aldi.pt"
        d["contact:facebook"] = "AldiSupermercados.pt"
        d["contact:youtube"] = "https://www.youtube.com/@aldi.portugal"
        d["contact:instagram"] = "aldi.portugal"
        d["contact:linkedin"] = "https://linkedin.com/company/aldi-portugal"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        d["addr:postcode"] = nd["zip"].strip()
        d["addr:city"] = CITIES.get(d["addr:postcode"], nd["city"].strip())
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            street = nd["streetAndNumber"].replace("  ", " ")
            for r in STREET_ABBREVS:
                street = re.sub(r[0], r[1], street.lower())
            if m := re.fullmatch(r"^(?!en\s+)(.+?),?\s+(?:n\.º\s*)?(\d+[a-z]?(?:-\d+)?)", street):
                d["addr:street"] = titleize(m[1])
                d["addr:housenumber"] = m[2].upper()
            elif re.match(r"^bairro ", street):
                d["addr:place"] = titleize(street)
            elif re.match(r"^quinta ", street):
                d["addr:suburb"] = titleize(street)
            else:
                m = street.split(",", 1)
                d["addr:street"] = titleize(m[0])
                if len(m) > 1:
                    d["addr:place"] = titleize(m[1].strip())

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["identifier"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Aldi", REF, old_data, osm=True)
