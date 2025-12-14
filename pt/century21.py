#!/usr/bin/env python3

import re

from impl.common import DiffDict, distance, fetch_json_data, format_phonenumber, overpass_query, write_diff


DATA_URL = "https://century21.pt/api/agencies"

REF = "ref"

CITIES = {
    "1600-864": "Lisboa",
    "2560-498": "Silveira",
    "2605-652": "Belas",
    "2610-161": "Amadora",
    "2725-326": "Mem Martins",
    "2785-629": "São Domingos de Rana",
    "2820-156": "Charneca de Caparica",
    "2820-186": "Charneca de Caparica",
    "2825-294": "Costa da Caparica",
    "2830-998": "Barreiro",
    "2840-400": "Seixal",
    "2845-111": "Amora",
    "2845-483": "Amora",
    "4400-053": "Vila Nova de Gaia",
    "8125-410": "Quarteira",
    "9000-042": "Funchal",
    "9000-089": "Funchal",
    "9000-268": "Funchal",
    "9600-513": "Ribeira Grande",
}


def fetch_data():
    return fetch_json_data(DATA_URL, params={"limit": 1000})["data"]


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "("
            'nwr[office][~"^(name|brand)$"~"century[ ]?21",i](area.country);'
            'nwr[shop][~"^(name|brand)$"~"century[ ]?21",i](area.country);'
            ");"
        )
    ]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["code"]
        branch = nd["name"].removeprefix("CENTURY 21 ").replace("´", "'").strip()
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["latitude"], nd["longitude"]]
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
        d["office"] = "estate_agent"
        d["name"] = f"Century 21 {branch}"
        d["brand"] = "Century 21"
        d["brand:wikidata"] = "Q1054480"
        d["brand:wikipedia"] = "en:Century 21 Real Estate"
        d["branch"] = branch

        tags_to_reset.add("shop")

        phones = nd["phone"].split("  ")
        if phones := [x for x in (format_phonenumber(x) for x in phones) if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.add("contact:email")
        d["website"] = f"https://www.century21.pt/agencias/{nd['handler']}"
        d["contact:facebook"] = "C21Portugal"
        d["contact:youtube"] = "@C21Portugal"
        d["contact:instagram"] = "c21portugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/century21portugal"
        d["contact:twitter"] = "C21Portugal"

        tags_to_reset.update({"phone", "mobile", "url", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        address = nd["address"].removesuffix(", Portugal")
        if m := re.fullmatch(r"(.+),?\s*(\d{4}\s*[-–]\s*\d{3})(?:,?\s*(.+))?", address):
            address = m[1].strip(", ")
            d["addr:postcode"] = re.sub(r"\s*[-–]\s*", "-", m[2])
            d["addr:city"] = CITIES.get(d["addr:postcode"], m[3]) or d["addr:city"]
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Century 21", REF, old_data, osm=True)
