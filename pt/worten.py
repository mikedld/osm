#!/usr/bin/env python3

import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

from impl.common import DiffDict, cache_name, overpass_query, titleize, distance, write_diff
from impl.config import ENABLE_CACHE, PLAYWRIGHT_CDP_URL, PLAYWRIGHT_CONTEXT_OPTS


REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"2ª a (6ª|sexta)": r"Mo-Fr",
    r"(de )?(2ª|segunda)( a)? sábado": r"Mo-Sa",
    r"2ª a domingo|todos os dias": r"Mo-Su",
    r"(\d{2}), (\d{2}) de novembro": r"Nov \1, Nov \2",
    r"(6ª|sexta), sábado": r"Fr,Sa",
    r"(6ª|sexta), sábado, vésperas? de feriados?": r"Fr,Sa,PH -1 day",
    r"de (\d{2}) a (\d{2}) de dezembro": r"Dec \1-\2",
    r"dia (\d{2}) de dezembro": r"Dec \1",
    r"domingo": r"Su",
    r"domingo a 5ª": r"Su-Th",
    r"domingo a sexta": r"Su-Fr",
    r"domingos?, feriados": r"Su,PH",
    r"domingo, véspera de feriado": r"Su,PH -1 day",
    r"feriados": r"PH",
    r"sábado": r"Sa",
    r"sábado, domingo": r"Sa,Su",
    r"sábado, domingo, feriados": r"Sa,Su,PH",
    r"segunda a 5ª, domingo": r"Mo-Th,Su",
}
SCHEDULE_HOURS_MAPPING = {
    r"(?:das )?(\d{2})[:h](\d{2})h?\s*(?:-|[áà]s)\s*(\d{2})[:h](\d{2})h?": r"\1:\2-\3:\4",
    r"(?:das )?(\d{1})[:h](\d{2})h?\s*(?:-|[áà]s)\s*(\d{2})[:h](\d{2})h?": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{2})h\s*(?:-|[áà]s)\s*(\d{2})h": r"\1:00-\2:00",
    r"(?:das )?(\d{1})h\s*(?:-|[áà]s)\s*(\d{2})h": r"0\1:00-\2:00",
    r"encerrados": r"off",
}
CITIES = {
    "2040-413": "Rio Maior",
    "2135-114": "Samora Correia",
    "2695-877": "São João da Talha",
    "4430-826": "Avintes",
    "4535-211": "Mozelos",
    "4700-154": "Frossos",
    "4760-727": "Ribeirão",
    "9760-400": "Praia da Vitória",
    "9900-038": "Angústias",
}


def fetch_data(page_url, data_url):
    def filter_requests(route, request):
        if request.resource_type in ("stylesheet", "image", "media", "font") and "cloudflare.com" not in request.url:
            # print(f"Aborting request: {request.url}")
            route.abort()
            return
        if re.search(r"(cookiebot|google(tagmanager)?|gstatic)\.com/", request.url):
            # print(f"Aborting request: {request.url}")
            route.abort()
            return
        # print(f"Making request: {request.url}")
        route.continue_()

    cache_file = Path(f"{cache_name(data_url)}.json")
    if not ENABLE_CACHE or not cache_file.exists():
        # print(f"Querying URL: {data_url}")
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(PLAYWRIGHT_CDP_URL) if PLAYWRIGHT_CDP_URL else p.firefox.launch()
            context = browser.new_context(**PLAYWRIGHT_CONTEXT_OPTS)
            page = context.new_page()
            page.route("**/*", filter_requests)
            with page.expect_response(data_url, timeout=60000) as response:
                page.goto(page_url)
            result = response.value.body().decode("utf-8")
            browser.close()
        result = json.loads(result)
        if ENABLE_CACHE:
            cache_file.write_text(json.dumps(result))
    else:
        result = json.loads(cache_file.read_text())
    return result


if __name__ == "__main__":
    page_url = "https://www.worten.pt/lojas-worten"
    data_url = "https://www.worten.pt/_/api/graphql?wOperationName=getStores"
    new_data = fetch_data(page_url, data_url)[0]["data"]["stores"]["stores"]

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"Worten"](area.country);')]

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [float(nd["latitude"]), float(nd["longitude"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = float(nd["latitude"])
            d.data["lon"] = float(nd["longitude"])
            old_data.append(d)

        name = re.sub(r"^(Worten( Mobile)?).*", r"\1", nd["title"].replace("WRT", "Worten"))
        branch = nd["title"].replace("WRT", "Worten")[len(name):].strip()
        is_mobile = "Mobile" in name
        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "mobile_phone" if is_mobile else "electronics"
        d["name"] = name
        d["brand"] = "Worten"
        d["brand:wikidata"] = "Q10394039"
        d["brand:wikipedia"] = "pt:Worten"
        d["branch"] = branch

        schedule = [x.replace("  ", " ").replace("–", "-").strip().lower() for x in re.split(r"[/|()\n]", nd["openingHours"]) if x.strip()]
        schedule = [re.sub(r"([-,])? das ", ": das ", x).replace(" e ", ", ") for x in schedule]
        schedule = [re.sub(r"(?<!:)(?:-? )(encerrados|\d+h-\d+h)", r": \1", x) for x in schedule]
        schedule = [[y.strip() for y in x.split(":", 1)] for x in schedule]
        for s in schedule:
            if len(s) != 2:
                s[:] = ["<ERR>"]
                continue

            sa = s[0]
            sb = f"<ERR>"
            for sma, smb in SCHEDULE_DAYS_MAPPING.items():
                if re.fullmatch(sma, sa) is not None:
                    sb = re.sub(sma, smb, sa)
                    break
            s[0] = sb

            sa = s[1]
            sb = "<ERR>"
            for sma, smb in SCHEDULE_HOURS_MAPPING.items():
                if re.fullmatch(sma, sa) is not None:
                    sb = re.sub(sma, smb, sa).replace("23:59", "00:00")
                    break
            s[1] = sb
        if len(schedule) == 2 and schedule[0][0] == "Mo-Fr" and schedule[1][0] == "Sa,Su" and schedule[0][1] == schedule[1][1]:
            schedule = [["Mo-Su", schedule[0][1]]]
        schedule = "; ".join([" ".join(x) for x in schedule])
        if schedule.replace(" ", "") != d["opening_hours"].replace(" ", ""):
            d["opening_hours"] = schedule

        phone = nd["phoneNumber"]
        if phone:
            phone = re.sub(r"\s+", "", phone).split("(")[0].split(",")[0].split("/")
            for i in range(1, len(phone)):
                if len(phone[i]) < 9 and len(phone[i - 1]) == 9:
                    phone[i] = f"{phone[i - 1][:9 - len(phone[i])]}{phone[i]}"
            phone = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in phone if len(x) == 9]
            if tf := [x for x in phone if x[5:6] == "9"]:
                d["contact:mobile"] = ";".join(tf)
            else:
                tags_to_reset.add("contact:mobile")
            if tf := [x for x in phone if x[5:6] != "9"]:
                d["contact:phone"] = ";".join(tf)
            else:
                tags_to_reset.add("contact:phone")
        d["contact:website"] = f"https://www.worten.pt{nd['url']}"
        d["contact:facebook"] = "wortenpt"
        d["contact:twitter"] = "WortenPT"
        d["contact:youtube"] = "https://www.youtube.com/c/worten"
        d["contact:instagram"] = "wortenpt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/worten/"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        address = nd["address"]
        d["addr:city"] = CITIES.get(address["postalCode"], titleize(re.split(r"\s+[-–]\s+|,\s+", address["city"])[0].strip()))
        d["addr:postcode"] = address["postalCode"]
        if not d["addr:street"] and not (d["addr:housenumber"] or d["addr:housename"] or d["nohousenumber"]) and not d["addr:place"] and not d["addr:suburb"]:
            d["x-dld-addr"] = "; ".join(address["address"])

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

    write_diff("Worten", REF, old_data, osm=True)
