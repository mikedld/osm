#!/usr/bin/env python3

import datetime
import html
import json
import itertools
import re
from html.parser import HTMLParser
from multiprocessing import Pool
from pathlib import Path

import requests

from impl.common import DiffDict, cache_name, overpass_query, titleize, opening_weekdays, distance, write_diff
from impl.config import ENABLE_CACHE


REF = "ref"

CITIES = {
    "lisbon": "lisboa",
    "oporto": "porto",
    "portimao": "portimão",
    "vila nova de gaia porto": "vila nova de gaia",
}


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stores = None
        self.store = None
        self.store_events = ""
        self._in_store = False
        self._in_store_events = False

    def handle_starttag(self, tag, attrs):
        locs = next((a for a in attrs if a[0] == "data-locations"), None)
        if locs:
            self.stores = json.loads(html.unescape(locs[1]))

        if tag == "script" and ("type", "application/ld+json") in attrs:
            self._in_store = True

        if tag == "div" and next((a for a in attrs if a[0] == "class" and "store-events" in a[1]), None):
            self._in_store_events = True

    def handle_endtag(self, tag):
        if tag == "div" and self._in_store_events:
            self._in_store_events = False

    def handle_data(self, data):
        if self._in_store:
            self.store = json.loads(data)
            self._in_store = False
        if self._in_store_events:
            self.store_events += data


def fetch_stores_data(url):
    cache_file = Path(f"{cache_name(url)}.html")
    if not cache_file.exists():
        # print(f"Querying URL: {url}")
        result = requests.get(url).content.decode("utf-8")
        if ENABLE_CACHE:
            cache_file.write_text(result)
    else:
        result = cache_file.read_text()
    parser = PageParser()
    parser.feed(result)
    return parser.stores


def fetch_store_data(store):
    store_id = re.sub(r'.*data-store-id="([^"]+)".*', r"\1", store["infoWindowHtml"], flags=re.S)
    url = f"https://www.auchan.pt/pt/loja?StoreID={store_id}"
    cache_file = Path(f"{cache_name(url)}.html")
    if not cache_file.exists():
        # print(f"Querying URL: {url}")
        result = requests.get(url).content.decode("utf-8")
        if ENABLE_CACHE:
            cache_file.write_text(result)
    else:
        result = cache_file.read_text()
    parser = PageParser()
    parser.feed(result)
    return {"id": store_id, "events": [x.strip() for x in parser.store_events.split("\n") if x.strip()], **store, **parser.store}


if __name__ == "__main__":
    old_data = [DiffDict(e) for e in overpass_query(f'area[admin_level=2][name=Portugal] -> .p; ( nwr[shop][shop!=electronics][shop!=houseware][shop!=pet][name~"Auchan"](area.p); ' +
        f'nwr[amenity][amenity!=fuel][amenity!=charging_station][amenity!=parking][name~"Auchan"](area.p); nwr[shop][name~"Minipreço"](area.p); );')["elements"]]

    data_url = "https://www.auchan.pt/pt/lojas"
    new_data = fetch_stores_data(data_url)
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_store_data, (nd for nd in new_data if nd["type"] == "Auchan")))

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 100], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["latitude"]
            d.data["lon"] = nd["longitude"]
            old_data.append(d)
            new_node_id -= 1

        name = re.sub(r"^(Auchan( Supermercado)?|My Auchan( Saúde e Bem-Estar)?|Auchan).+", r"\1", nd["name"])
        branch = re.sub(r"[ ]{2,}", " ", nd["name"][len(name):]).strip()
        is_super = name == "Auchan Supermercado"
        is_my = name == "My Auchan"
        is_my_saude = name == "My Auchan Saúde e Bem-Estar"
        tags_to_reset = set()

        d[REF] = public_id
        if is_my_saude:
            d["amenity"] = "pharmacy"
        else:
            d["shop"] = "convenience" if is_my else "supermarket"
        d["name"] = name.replace("My Auchan", "MyAuchan")
        d["branch"] = branch
        d["brand"] = "MyAuchan" if is_my or is_my_saude else "Auchan"
        d["brand:wikidata"] = "Q115800307" if is_my or is_my_saude else "Q758603"
        d["brand:wikipedia"] = "pt:Auchan"

        if old_name := d.old_tags.get("name"):
            if "Auchan" not in old_name:
                d["old_name"] = old_name

        if d["operator"] not in (None, "Auchan"):
            tags_to_reset.add("operator")

        EVENTS_MAPPING = {
            r"Horário feriados: (\d{2}:\d{2}) - (\d{2}:\d{2})": r"PH \1-\2",
            r"Horário feriados: (\d{1}:\d{2}) - (\d{2}:\d{2})": r"PH 0\1-\2",
            r"Horário vésperas de feriado: (\d{2}:\d{2}) - (\d{2}:\d{2})": r"PH -1 days \1-\2",
            r"Encerramento: domingo de Páscoa, 25 de dezembro e 1 de janeiro": r"easter,Dec 25,Jan 01 off",
            r"Encerramento véspera de Ano Novo: (\d{2}:\d{2})": r"Dec 31 {opens-}\1",
            r"Encerramento véspera de Natal: (\d{2}:\d{2})": r"Dec 24 {opens-}\1",
        }
        if schedule := nd["openingHoursSpecification"]:
            DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            opens = set(x["opens"] for x in schedule)
            schedule = [
                {
                    "d": DAYS.index(x["dayOfWeek"]),
                    "t": f"{x['opens']}-{x['closes']}",
                }
                for x in schedule
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
            if events := nd["events"]:
                events.sort(key=lambda x: -ord(x[0]))
                if len(opens) == 1:
                    opens = list(opens)[0]
                    for ea in events:
                        eb = "???"
                        for ema, emb in EVENTS_MAPPING.items():
                            if re.fullmatch(ema, ea) is not None:
                                eb = re.sub(ema, emb, ea)
                                break
                        schedule.append(eb.replace("{opens-}", f"{opens}-"))
                else:
                    schedule.append("???")
            schedule = "; ".join(schedule)
            if d["opening_hours"].replace(" ", "") != schedule.replace(" ", ""):
                d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = nd["telephone"][:16]
        if phone:
            if phone[5:6] == "9":
                d["contact:mobile"] = phone
                tags_to_reset.add("contact:phone")
            else:
                d["contact:phone"] = phone
                tags_to_reset.add("contact:mobile")
        d["contact:website"] = f"https://www.auchan.pt/pt/loja?StoreID={public_id}"
        d["contact:facebook"] = "AuchanPortugal"
        d["contact:youtube"] = "https://www.youtube.com/channel/UC6FSI7tYO9ISV11U2PHBBYQ"
        d["contact:instagram"] = "auchan_pt"
        d["contact:tiktok"] = "auchan_pt"
        d["contact:email"] = "apoiocliente@auchan.pt"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["address"]

        if not d["addr:city"]:
            d["addr:city"] = address["addressLocality"]
        d["addr:postcode"] = address["postalCode"]

        # street = [x.strip() for x in address["streetAddress"].split(f", {address['addressLocality']}", 1)[0].split(",", 1)]
        # if len(street) == 1:
        #     street = [x.strip() for x in street[0].split("nº", 1)]
        # if len(street) == 2:
        #     street[1] = [re.sub(r"Lote[ ]+", "LT ", x, flags=re.I).strip() for x in re.split(r"[nN]\.?º|,|\be\b", street[1]) if x.strip()]
        #     d["addr:street"] = street[0]
        #     d["addr:housenumber"] = ";".join(street[1])

        if d.kind == "new" and not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"] or d["addr:housename"]):
            d["x-dld-addr"] = address["streetAddress"]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind == "new":
            continue
        ref = d[REF]
        if any(nd for nd in new_data if ref == nd["id"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Auchan", REF, old_data)
