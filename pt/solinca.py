#!/usr/bin/env python3

import re
from multiprocessing import Pool
from pathlib import Path

import requests
from lxml import etree

from impl.common import DiffDict, cache_name, overpass_query, distance, titleize, lookup_postcode, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

DAYS = {
    "2ª a 6ª Feira:": "Mo-Fr",
    "Sábados": "Sa",
    "Domingos e Feriados": "Su,PH",
}


def fetch_level1_data(url):
    page = 1
    results = []
    while True:
        cache_file = Path(f"{cache_name(url + str(page))}.html")
        if not ENABLE_CACHE or not cache_file.exists():
            # print(f"Querying URL: {url}")
            r = requests.get(url, params={"sf_paged": page})
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
        results.extend([
            {
                "id": re.sub(r"^.*\bpost-(\d+)\b.*$", r"\1", el.xpath("./@class")[0]),
                "url": el.xpath("./a[1]/@href")[0],
                "title": el.xpath(".//h3[@class='elementor-post__title']/text()")[0].strip().replace("–", "-"),
                "type": el.xpath(".//p[@class='elementor-post-cat']/text()")[0].strip(),
            }
            for el in result_tree.xpath("//article")
        ])
        if not result_tree.xpath("//nav[@class='elementor-pagination']/a[contains(@class, 'next')]/@href"):
            break
        page += 1
    return results


def fetch_level2_data(data):
    url = data["url"]
    cache_file = Path(f"{cache_name(url)}.html")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {url}")
        r = requests.get(url)
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
    info = [x.strip() for x in result_tree.xpath(f"//section[.//*[contains(text(), 'Horário')]]//text()") if x.strip()]
    address = re.sub(r"[–—]", "-", re.sub(r"\s+", " ", " ".join(info[info.index("Morada") + 1:info.index("Ver no mapa >")])))
    location = None
    if m := re.search(r".+?\b(\d{4}(\s*-\s*\d{3})?)\b,?(\s+\D.*|$)", address):
        postcode = m[1].replace("–", "-").replace(" ", "")
        if len(postcode) == 4:
            postcode += "-000"
        location = lookup_postcode(postcode)
        if not location and "-" in postcode:
            location = lookup_postcode(postcode.split("-", 1)[0])
        if location:
            location.append(postcode)
    return {
        **data,
        "location": location,
        "schedule": [re.sub(r"\s+", " ", x) for x in info[info.index("Horário") + 1:info.index("Contacto")]],
        "contacts": [re.sub(r"\s+", "", x) for x in info[info.index("Contacto") + 1:info.index("Localização")]],
        "address": address,
    }


if __name__ == "__main__":
    data_url = "https://www.solinca.pt/solinca-ginasios/"
    new_data = fetch_level1_data(data_url)
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[leisure][name~"Solinca"](area.p); );')["elements"]]

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None and nd["location"]:
            coord = nd["location"][0]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 250], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["location"][0][0]
            d.data["lon"] = nd["location"][0][1]
            old_data.append(d)

        tags_to_reset = set()

        d[REF] = public_id
        d["leisure"] = "fitness_centre"
        d["name"] = nd["type"]
        d["branch"] = titleize(nd["title"])

        schedule = []
        for s in nd["schedule"]:
            if days := DAYS.get(s):
                schedule.append([days, []])
            else:
                schedule[-1][1].append(re.sub(r"(\d{2}:\d{2})\s*(?:-|às)\s*(\d{2}:\d{2})", r"\1-\2", s))
        schedule = [
            f"{days} {','.join(times)}"
            for days, times in schedule
        ]
        d["opening_hours"] = "; ".join(schedule)
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        contacts = []
        for c in nd["contacts"]:
            if c.lower() in ("email", "fax", "tel"):
                contacts.append([c.lower(), []])
            else:
                if len(c) == 3 and contacts[-1][0] == "tel" and contacts[-1][1] and len(contacts[-1][1][-1]) == 6:
                    contacts[-1][1][-1] += c
                else:
                    contacts[-1][1].append(c)
        contacts = dict(contacts)
        mobiles = []
        phones = []
        for phone in contacts["tel"]:
            if len(phone) == 9:
                phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
                if phone[5:6] == "9":
                    mobiles.append(phone)
                else:
                    phones.append(phone)
        if mobiles:
            d["contact:mobile"] = ";".join(mobiles)
        else:
            tags_to_reset.add("contact:mobile")
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        faxes = []
        for phone in contacts.get("fax", []):
            if len(phone) == 9:
                faxes.append(f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}")
        if faxes:
            d["contact:fax"] = ";".join(faxes)
        else:
            tags_to_reset.add("contact:fax")
        d["contact:website"] = nd["url"]
        d["contact:facebook"] = "Solinca"
        d["contact:youtube"] = "https://www.youtube.com/@SolincaFitnessChannel"
        d["contact:instagram"] = "solincahf"
        d["contact:linkedin"] = "https://www.linkedin.com/company/solinca-health-&-fitness"
        d["contact:email"] = ";".join(contacts["email"])

        tags_to_reset.update({"phone", "mobile", "fax", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        d["addr:city"] = titleize(nd["location"][1])
        postcode = nd["location"][2]
        if len(postcode) == 8 and postcode.endswith("-000"):
            postcode = postcode[:4]
        if len(postcode) == 4:
            postcode += d["addr:postcode"][4:] if len(d["addr:postcode"]) == 8 else "-000"
        if len(postcode) == 8:
            d["addr:postcode"] = postcode
        elif postcode:
            d["addr:postcode"] = "<ERR>"
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"]:
            d["x-dld-addr"] = nd["address"]

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

    write_diff("Solinca", REF, old_data, osm=True)
