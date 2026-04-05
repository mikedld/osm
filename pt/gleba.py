#!/usr/bin/env python3

import re

from more_itertools import one

from impl.common import DiffDict, distance, fetch_html_data, format_phonenumber, lookup_gmaps_coords, overpass_query, write_diff


DATA_URL = "https://mygleba.com/pt/lojas-gleba"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"seg a quin": "Mo-Th",
    r"seg a dom": "Mo-Su",
    r"sex a dom": "Fr-Su",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})[:h](\d{2})\s*-\s*(\d{2})[:h](\d{2})": r"\1:\2-\3:\4",
}


def fetch_data():
    result_tree = fetch_html_data(DATA_URL)
    result = [
        {
            "id": el.attrib["id"].removeprefix("gleba-"),
            "title": one(el.xpath(".//*[@class='lojas_gleba_title']//text()")),
            "address": el.xpath(".//*[@class='lojas_gleba_text']//text()"),
            "schedule": [
                x.strip() for x in el.xpath(".//*[contains(@class, 'lojas_gleba_info_schedule')]//text()") if x.strip()
            ],
            "phone": one(
                y.strip() for x in el.xpath(".//a[starts-with(@href, 'tel:')]/@href") for y in x.removeprefix("tel:").split("/")
            ),
            "coords": lookup_gmaps_coords(el.xpath(".//a[contains(./span/text(), 'Ver mapa')]/@href")[0]),
            "menu_urls": el.xpath(".//a[contains(./span/text(), 'Cafetaria')]/@href"),
        }
        for el in result_tree.xpath("//div[@class='lojas_gleba']//div[@class='lojas_gleba_caroussel_outter_wrap']")
    ]
    return result


def schedule_time(v, mapping):
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"gleba",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = nd["coords"]
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
        d["shop"] = "bakery"
        d["name"] = "Gleba"
        d["branch"] = nd["title"].removeprefix("Gleba ").split(" - ")[0]

        schedule = [re.split(r"\s*:\s*", x.lower(), maxsplit=1) for x in nd["schedule"]]
        schedule = [[schedule_time(x[0], SCHEDULE_DAYS_MAPPING), schedule_time(x[1], SCHEDULE_HOURS_MAPPING)] for x in schedule]
        schedule = "; ".join([" ".join(x) for x in schedule])
        d["opening_hours"] = schedule
        d["source:opening_hours"] = "website"

        if phone := format_phonenumber(nd["phone"]):
            d["contact:phone"] = phone
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://mygleba.com/"
        d["website:menu"] = nd["menu_urls"][0] if nd["menu_urls"] else ""
        d["contact:facebook"] = "glebapadaria"
        d["contact:youtube"] = "@gleba-moagempadaria3643"
        d["contact:instagram"] = "gleba_padaria"
        d["contact:linkedin"] = "https://www.linkedin.com/company/gleba-moagem-padaria"
        d["contact:pinterest"] = "gleba_padaria"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        postcode, city = nd["address"][-1].split(" ", maxsplit=1)
        d["addr:postcode"] = postcode
        d["addr:city"] = city
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(nd["address"][0:-1])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Gleba", REF, old_data, osm=True)
