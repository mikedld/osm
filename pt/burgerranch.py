#!/usr/bin/env python3

import re

from impl.common import DiffDict, fetch_json_data, overpass_query, distance, titleize, write_diff


DATA_URL = "https://www.burgerranch.com/localizacoes/"

REF = "ref"


def fetch_data():
    def post_process(page):
        return re.sub(r"^.*var restaurants\s*=\s*\[(.+?)\];.*$", r"[\1]", page, flags=re.S)

    return fetch_json_data(DATA_URL, post_process=post_process)


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[amenity][name~"Burgu?er Ranch|Ranch Burgu?er"](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = "<NONE>" # nd["id"]
        tags_to_reset = set()

        d = None # next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["latitude"]), float(nd["longitude"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = float(nd["latitude"])
            d.data["lon"] = float(nd["longitude"])
            old_data.append(d)
            new_node_id -= 1

        d[REF] = public_id
        d["amenity"] = "fast_food"
        d["cuisine"] = "burger"
        d["name"] = "Burger Ranch"
        d["brand"] = "Burger Ranch"
        d["brand:wikidata"] = "Q1014891"
        # d["brand:wikipedia"] = ""
        d["branch"] = titleize(nd["title"])

        d["contact:phone"] = "+351 282 422 274"
        d["contact:website"] = "https://www.burgerranch.com/"
        d["contact:facebook"] = "BurgerRanch"
        d["contact:instagram"] = "burgerranch"
        d["contact:linkedin"] = "https://www.linkedin.com/company/burger-ranch/"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = re.sub(r"(?<!\s)(?<!,)\s+(?=\d{4}-\d{3}\b)", ", ", nd["text"]).rsplit(",", 1)
        if m := re.fullmatch(r"(\d{4}(?:-\d{3})?\b)\s*(\b(?!-).+)?", address[1].strip()):
            postcode, city = m[1], m[2]
            if len(postcode) == 4:
                if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                    postcode = d["addr:postcode"]
                else:
                    postcode += "-000"
            d["addr:postcode"] = postcode
            if city:
                d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address[0]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            d.revert(REF)
            continue
        # ref = d[REF]
        # if ref and any(nd for nd in new_data if ref == nd["id"]):
        #     continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Burger Ranch", REF, old_data, osm=True)
