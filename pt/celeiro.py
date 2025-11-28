#!/usr/bin/env python3

import json
import re

from playwright.sync_api import sync_playwright

from impl.common import DiffDict, cache_name, distance, overpass_query, write_diff
from impl.config import ENABLE_CACHE, PLAYWRIGHT_CDP_URL, PLAYWRIGHT_CONTEXT_OPTS


DATA_URL = "https://www.celeiro.pt/lojas"

REF = "ref"

BRANCHES = {
    "Av. António Augusto de Aguiar": "Avenida António Augusto de Aguiar",
    "Av. da República": "Avenida da República",
    "Av. Roma": "Avenida de Roma",
}
SCHEDULE_DAYS_MAPPING = {
    r"24 de dezembro": "Dec 24",
    r"2ª a 5ª": "Mo-Th",
    r"2ª a 6ª": "Mo-Fr",
    r"2ª a dom": "Mo-Su",
    r"2ª a dom.": "Mo-Su",
    r"2ª a domingo": "Mo-Su",
    r"2ª a sábado": "Mo-Sa",
    r"2ª a sexta": "Mo-Fr",
    r"31 de dezembro": "Dec 31",
    r"6ª a sábado": "Fr,Sa",
    r"6ª e sábado": "Fr,Sa",
    r"6ª feira, sábado e vésperas de feriado": "Fr,Sa,PH -1 day",
    r"6ª feira santa": "easter -2 days",
    r"6ª feira santa e domingo páscoa": "easter -2 days,easter",
    r"6ª-feira, sábado e vésperas de feriado": "Fr,Sa,PH -1 day",
    r"domingo": "Su",
    r"domingo a 5ª": "Su-Th",
    r"domingo a 5ª feira": "Su-Th",
    r"domingo a 5ª-feira": "Su-Th",
    r"domingo e feriado": "Su,PH",
    r"domingo e feriados": "Su,PH",
    r"domingo páscoa": "easter",
    r"feriados": "PH",
    r"sábado": "Sa",
    r"segunda-feira a sábado": "Mo-Sa",
    r"sexta e sábado": "Fr,Sa",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h às (\d{2})h": r"0\1:00-\2:00",
    r"(\d{1})h(\d{2}) às (\d{2})h": r"0\1:\2-\3:00",
    r"(\d{2}):(\d{2}) – (\d{2}):(\d{2})h": r"\1:\2-\3:\4",
    r"(\d{2}):(\d{2}) às (\d{2}):(\d{2})h": r"\1:\2-\3:\4",
    r"(\d{2}):(\d{2})h às (\d{2})h": r"\1:\2-\3:00",
    r"(\d{2})h às (\d{2}):(\d{2})h": r"\1:00-\2:\3",
    r"(\d{2})h às (\d{2})h": r"\1:00-\2:00",
    r"(\d{2})h(\d{2}) às (\d{2})h": r"\1:\2-\3:00",
    r"encerrado": "off",
}
CITIES = {
    "8200-425": "Guia",
}


def fetch_data():
    def filter_requests(route, request):
        if request.resource_type in ("stylesheet", "image", "media", "font", "script") and "cloudflare.com" not in request.url:
            # print(f"Aborting request: {request.url}")  # noqa: ERA001
            route.abort()
            return
        if re.search(r"(cookiebot|google(tagmanager)?|gstatic)\.com/", request.url):
            # print(f"Aborting request: {request.url}")  # noqa: ERA001
            route.abort()
            return
        # print(f"Making request: {request.url}")  # noqa: ERA001
        route.continue_()

    cache_file = cache_name(DATA_URL).with_suffix(".cache.json")
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
    result = re.sub(r"^.*var\s+stores_obj\s*=\s*JSON.parse\('([^']*)'\);.*$", r"\1", result, flags=re.DOTALL)
    result = [
        {
            "id": k,
            **v,
        }
        for k, v in json.loads(result).items()
    ]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name="Celeiro"](area.country);')]

    for nd in new_data:
        public_id = nd["id"]
        branch = re.sub(r"^Celeiro\s+", "", nd["title"])
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
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["lat"])
            d.data["lon"] = float(nd["lng"])
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["organic"] = "only"
        d["name"] = "Celeiro"
        d["brand"] = "Celeiro"
        d["brand:wikidata"] = "Q114102189"
        d["branch"] = BRANCHES.get(branch, branch)

        schedule = [
            re.split(r"\s*(?:[-–:](?!feira)|\bdas\b)\s*", x.strip().lower(), maxsplit=1)
            for x in nd["schedule"].split("</br>")
            if not re.match(
                r"($|chamada para a rede fixa nacional|o horário pode diferir durante feriados|loja temporariamente encerrada)",
                x.strip(" *"),
                re.IGNORECASE,
            )
        ]
        for s in schedule:
            if len(s) != 2:
                s[:] = [f"<ERR:{s}>"]
                continue

            sa = s[0]
            sb = f"<ERR:{s}>"
            for sma, smb in SCHEDULE_DAYS_MAPPING.items():
                if re.fullmatch(sma, sa) is not None:
                    sb = re.sub(sma, smb, sa)
                    break
            s[0] = sb

            sa = s[1]
            sb = f"<ERR:{s}>"
            for sma, smb in SCHEDULE_HOURS_MAPPING.items():
                if re.fullmatch(sma, sa) is not None:
                    sb = re.sub(sma, smb, sa).replace("23:59", "00:00")
                    break
            s[1] = sb
        for i in range(len(schedule) - 1, 0, -1):
            if len(schedule) >= 2 and schedule[i - 1][1] == schedule[i][1]:
                schedule = [*schedule[: i - 1], [f"{schedule[i - 1][0]},{schedule[i][0]}", schedule[i][1]], *schedule[i + 1 :]]
        schedule = "; ".join([" ".join(x) for x in schedule])
        d["opening_hours"] = schedule
        d["source:opening_hours"] = "website"

        if phone := nd["phone"]:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = nd["link"]
        d["contact:facebook"] = "celeiro.pt"
        d["contact:instagram"] = "celeiro_viverdecorpoealma"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        address = [x.strip() for x in nd["address"].split("</br>") if x.strip()]
        if m := re.fullmatch(r"(\d{4})\s*[-–]\s*(\d{3})\s+(.+)", address[-1]):
            d["addr:postcode"] = f"{m[1]}-{m[2]}"
            d["addr:city"] = CITIES.get(d["addr:postcode"], m[3])
            address.pop()
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(address)

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

    write_diff("Celeiro", REF, old_data, osm=True)
