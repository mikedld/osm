#!/usr/bin/env python3

import itertools
import json
import re

from lxml import etree
from playwright.sync_api import sync_playwright

from impl.common import DiffDict, cache_name, distance, format_phonenumber, overpass_query, titleize, write_diff
from impl.config import ENABLE_CACHE, PLAYWRIGHT_CDP_URL, PLAYWRIGHT_CONTEXT_OPTS


DATA_URL = "https://chip7.pt/contact"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"segunda\s*,\s*quarta\s*,\s*sexta": "Mo,We,Fr",
    r"segunda\s+a\s+sexta": "Mo-Fr",
    r"segunda\s+a\s+domingo": "Mo-Su",
    r"terça\s+e\s+quinta": "Tu,Fr",
    r"sábados?": "Sa",
    r"sábados?\s*[,;]\s*domingos?\s+e\s+feriados": "Sa,Su,PH",
    r"domingos?\s+e\s+feriados?": "Su,PH",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})[h:]+(\d{2})\s*:\s*(\d{2})[h:]+(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})[h:]+(\d{2})\s*:\s*(\d{2})[h:]+(\d{2})": r"\1:\2-\3:\4",
    r"encerrad[ao]s?": r"off",
}
BRANCHES = {
    "Alges": "Algés",
    "Montemor o Novo": "Montemor-o-Novo",
}
CITIES = {
    "1495-027": "Algés",
    "2725-237": "Mem Martins",
    "3040-381": "Coimbra",
    "3860-672": "Avanca",
    "4435-208": "Rio Tinto",
    "4730-473": "Vila de Prado",
    "6230-372": "Fundão",
}


def fetch_data():
    def filter_requests(route, request):
        if (
            request.resource_type in ("stylesheet", "image", "media", "font", "script")
            and "cloudflare.com" not in request.url
            and "challenge-platform" not in request.url
        ):
            # print(f"Aborting request: {request.url}")  # noqa: ERA001
            route.abort()
            return
        if re.search(r"(cookiebot|google(tagmanager)?|gstatic)\.com/", request.url):
            # print(f"Aborting request: {request.url}")  # noqa: ERA001
            route.abort()
            return
        # print(f"Making request: {request.url}")  # noqa: ERA001
        route.continue_()

    cache_file = cache_name(DATA_URL).with_suffix(".cache.html")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {DATA_URL}")  # noqa: ERA001
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(PLAYWRIGHT_CDP_URL) if PLAYWRIGHT_CDP_URL else p.firefox.launch()
            context = browser.new_context(**PLAYWRIGHT_CONTEXT_OPTS)
            page = context.new_page()
            page.route("**/*", filter_requests)
            page.goto(DATA_URL)
            result = page.content()
            browser.close()
        if ENABLE_CACHE:
            cache_file.write_bytes(result.encode("utf-8"))
    else:
        result = cache_file.read_bytes().decode("utf-8")
    result = etree.fromstring(result.replace(" wire:", " wire-"), etree.HTMLParser()).xpath(
        "//div[@id='content']/div[@wire-initial-data]/@wire-initial-data"
    )[0]
    result = json.loads(result)["serverMemo"]["data"]["stores"]
    return result


def schedule_time(v, mapping):
    if not isinstance(v, str):
        return ",".join(schedule_time(x, mapping) for x in v)
    sa = v
    sb = f"<ERR:{v}>"
    for sma, smb in mapping.items():
        if re.fullmatch(sma, sa) is not None:
            sb = re.sub(sma, smb, sa)
            break
    return sb


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"chip ?7",i](area.country);')]

    for nd in new_data:
        public_id = str(nd["id"])
        branch = titleize(re.sub(r"^CHIP7\s+", "", nd["name"]))
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["lat"], nd["lng"]]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "computer"
        d["name"] = "Chip7"
        d["brand"] = "Chip7"
        # d["brand:wikidata"] = ""  # noqa: ERA001
        d["branch"] = BRANCHES.get(branch, branch)
        d["ref:vatin"] = f"PT{nd['vat']}"

        schedule = [re.sub(r"^(?:encerrad[ao]\s+aos|loja fechada)\s+(.+)$", r"\1 encerrada", x.lower()) for x in nd["schedule"]]
        schedule = [re.split(r"\s*:\s*", x, maxsplit=1) if re.match(r"[^:]*(sábado)\s*:", x) else [x] for x in schedule]
        schedule = [re.split(r"\s+e\s+", x[0], maxsplit=1) if len(x) == 1 and ":" in x[0] else x for x in schedule]
        schedule = [
            re.split(r"(?:\s+-)?\s+(?=encerrad)", x[0], maxsplit=1) if len(x) == 1 and "encerrad" in x[0] else x
            for x in schedule
        ]
        schedule = [
            x
            for y in schedule
            for x in y
            if not re.search(r"loja aberta durante o periodo de almoço|werepair guarda|dezembro 2025", x)
        ]
        schedule = [
            list(g) if k else next(g) for k, g in itertools.groupby(schedule, lambda x: re.search(r":|encerrad", x) is not None)
        ]
        schedule = [
            [schedule_time(x[0].strip(), SCHEDULE_DAYS_MAPPING), schedule_time(x[1], SCHEDULE_HOURS_MAPPING)]
            for x in itertools.batched(schedule, 2)
        ]
        schedule = "; ".join([" ".join(x) for x in schedule])
        d["opening_hours"] = schedule
        d["source:opening_hours"] = "website"

        if phones := [format_phonenumber(x) for x in (nd["phone"], nd["mobile"]) if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.add("contact:email")
        d["website"] = f"https://chip7.pt/contact/{nd['slug']}"
        if m := re.fullmatch(r".*facebook\.com/([^/?#]+).*", nd["facebook"] or ""):
            d["contact:facebook"] = m[1]
        else:
            tags_to_reset.add("contact:facebook")

        tags_to_reset.update({"phone", "mobile", "email", "facebook", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        if m := re.match(r"(\d{4}\s*-\s*\d{3})\b", nd["zip"]):
            d["addr:postcode"] = m[1].replace(" ", "")
        d["addr:city"] = CITIES.get(d["addr:postcode"]) or titleize(nd["city"])
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["address"]

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

    write_diff("Chip7", REF, old_data, osm=True)
