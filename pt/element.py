#!/usr/bin/env python3

import json
import re
from itertools import count
from multiprocessing import Pool

from impl.common import DiffDict, distance, fetch_html_data, overpass_query, titleize, write_diff


DATA_URL = "https://elementgyms.pt/ginasio/"

REF = "ref"

SCHEDULE_DAYS = {
    "2ª a 6ª": "Mo-Fr",
    "Sábados, domingos e feriados": "Sa,Su,PH",
}
SCHEDULE_TIMES = {
    "06h30 às 22h30": "06:30-22:30",
    "07h00 às 22h00": "07:00-22:00",
    "09h00 às 18h00": "09:00-18:00",
    "20h30 - 22h30": "20:30-22:30",
    "6h30 - 9h30": "06:30-09:30",
    "6h30 às 22h30": "06:30-22:30",
}


def fetch_level1_data():
    extra_title_mapping = {
        "braga": "braga centro",
        "coimbra": "coimbra solum",
        "covilha": "covilhã",
        "lisboa - campo pequeno": "campo pequeno",
        "gaia": "gaia centro",
        "s. mamede": "s.mamede",
        "tagus park": "taguspark",
    }

    def match_title(x, e):
        xt = x["title"].strip().lower()
        et = e["title"].strip().lower()
        return xt == extra_title_mapping.get(et, et)

    result = []
    extras = []
    for page_idx in count(start=1):
        params = {
            "sf_paged": page_idx,
        }
        result_tree = fetch_html_data(DATA_URL, params=params)
        if not extras:
            extras = result_tree.xpath("//script[@id='solinca-element-js-extra']/text()")[0]
            extras = re.sub(r"^.*var\s+gymsData\s*=\s*\{(.+)\};.*$", r"{\1}", extras, flags=re.DOTALL)
            extras = json.loads(extras)["gyms"]
        result.extend(
            [
                {
                    "url": el.xpath(".//a[@class='elementor-post__read-more']/@href")[0],
                    "title": el.xpath(".//div[@class='title-ginasio']/text()")[0].strip(),
                }
                for el in result_tree.xpath("//div[@class='container-ginasio-home']")
            ]
        )
        if not result_tree.xpath("//nav[@class='elementor-pagination']/a[contains(@class, 'next')]/@href"):
            break
    result = [
        {
            **x,
            "extra": next(
                (e for e in extras if match_title(x, e)),
                {"step_url": "=", "latitude": 38.306893, "longitude": -17.050891, "address": "?"},
            ),
        }
        for x in result
    ]
    return result


def fetch_level2_data(data):
    result_tree = fetch_html_data(data["url"])
    return {
        **data,
        "id": data["extra"]["step_url"].split("=")[1],
        "schedule": [
            x.strip().replace("–", "-")
            for x in (
                result_tree.xpath("//div[contains(./p/text(), 'Horário:')]/div//text()")
                or result_tree.xpath(
                    "//div[contains(./span/i/@class, 'icomoon-the7-font-the7-clock-03')]/following-sibling::div//text()"
                )
            )
            if x.strip()
        ],
    }


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[leisure][name~"element( |$)",i](area.country);')]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"].lstrip("0")
        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["extra"]["latitude"]), float(nd["extra"]["longitude"])]
        if coord[1] > 0:
            coord[1] = -coord[1]
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

        tags_to_reset = set()

        d[REF] = public_id
        d["leisure"] = "fitness_centre"
        d["name"] = "Element"
        d["branch"] = titleize(nd["title"])

        schedule = nd["schedule"]
        schedule = [re.split(r"\s*-\s*", x, maxsplit=1) for x in schedule]
        schedule = [[SCHEDULE_DAYS[x[0]], [SCHEDULE_TIMES[y] for y in re.split(r"\s*;\s*", x[1])]] for x in schedule]
        schedule = [f"{days} {','.join(times)}" for days, times in schedule]
        d["opening_hours"] = "; ".join(schedule)
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        d["website"] = nd["url"]
        d["contact:facebook"] = "elementgyms.pt"
        d["contact:instagram"] = "elementgyms.pt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/ginasioselement"
        d["contact:tiktok"] = "element-gym"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["extra"]["address"]
        if m := re.fullmatch(r"(.+?)\s*,\s*(\d{4}-\d{3})\s*,\s*(.+)", address, flags=re.DOTALL):
            d["addr:postcode"] = m[2]
            d["addr:city"] = titleize(m[3])
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"]:
            d["x-dld-addr"] = address

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

    write_diff("Element", REF, old_data, osm=True)
