#!/usr/bin/env python3

import re
from multiprocessing import Pool
from urllib.parse import urljoin

from continente import fetch_level2_data, process_address, process_contacts, process_schedule
from lxml import etree
from playwright.sync_api import sync_playwright

from impl.common import (
    DiffDict,
    cache_name,
    cookie_jar_name,
    distance,
    overpass_query,
    save_cookies,
    write_diff,
)
from impl.config import ENABLE_CACHE, PLAYWRIGHT_CDP_URL, PLAYWRIGHT_CONTEXT_OPTS


DATA_URL = "https://feed.continente.pt/lavandaria-self-service-washy"

REF = "ref"


def fetch_level1_data():
    def filter_requests(route, request):
        if (
            request.resource_type in ("stylesheet", "image", "media", "font", "script")
            and "kramericaindustries" not in request.url
        ):
            # print(f"Aborting request: {request.url}")  # noqa: ERA001
            route.abort()
            return
        # print(f"Making request: {request.url}")  # noqa: ERA001
        route.continue_()

    cache_file = cache_name(DATA_URL).with_suffix(".cache.html")
    if not ENABLE_CACHE or not (cache_file.exists() and cookie_jar_name(DATA_URL).exists()):
        # print(f"Querying URL: {DATA_URL}")  # noqa: ERA001
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(PLAYWRIGHT_CDP_URL) if PLAYWRIGHT_CDP_URL else p.firefox.launch()
            context = browser.new_context(**PLAYWRIGHT_CONTEXT_OPTS)
            page = context.new_page()
            page.route("**/*", filter_requests)
            page.goto(DATA_URL)
            page.locator("(//article[contains(@class, 'storeCard')])[1]").wait_for(timeout=60000)
            page.wait_for_load_state(timeout=60000)
            result = page.content()
            save_cookies(DATA_URL, {x["name"]: x["value"] for x in context.cookies()})
            browser.close()
        if ENABLE_CACHE:
            cache_file.write_bytes(result.encode("utf-8"))
    else:
        result = cache_file.read_bytes().decode("utf-8")
    result_tree = etree.fromstring(result, etree.HTMLParser())
    result = [
        {
            "lat": float(el.attrib["data-lat"]),
            "lon": float(el.attrib["data-lng"]),
            "name": el.attrib["data-name"],
            "city": el.attrib["data-location"],
            "addr": el.xpath(".//p[contains(@class, 'storeCard__address')]/text()")[0].strip(),
            "url": urljoin(DATA_URL, el.xpath(".//a[contains(@class, 'storeCard__link')]/@href")[0]),
        }
        for el in result_tree.xpath("//article[contains(@class, 'storeCard')]")
    ]
    return result


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"washy",i](area.country);')]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        name = re.sub(r"^(Continente( Bom Dia| Modelo)?).*", r"\1", nd["name"])
        branch = nd["name"][len(name) :].strip()
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"]), float(nd["lon"])]
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
        d["shop"] = "laundry"
        d["name"] = "Washy"
        d["brand"] = "Washy"
        d["brand:wikidata"] = "Q129416138"
        d["branch"] = branch
        d["self_service"] = "yes"

        process_schedule(d, nd)

        process_contacts(d, nd)

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        process_address(d, nd)

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Washy", REF, old_data, osm=True)
