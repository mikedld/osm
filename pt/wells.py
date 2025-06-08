#!/usr/bin/env python3

import re

from lxml import etree

from impl.common import DiffDict, fetch_json_data, overpass_query, titleize, distance, write_diff


DATA_URL = "https://wells.pt/lojas-wells"

REF = "ref"

BRANCHES = {
}
SCHEDULE_DAYS_MAPPING = {
    r"^$|segunda a domingo-?|todos os dias?": "Mo-Su",
    r"seg\.? a sex\.?": "Mo-Fr",
    r"seg\.? a sáb\.?": "Mo-Sa",
    r"dom(ingo|\.?) a qui(nta(-feira)?)?\.?": "Su-Th",
    r"s[aá]b\.": "Sa",
    r"dom\.?": "Su",
    r"sáb e dom\.": "Sa,Su",
    r"sex\. e sáb\.": "Fr,Sa",
    r"feriados": "PH",
    r"dom\.? e feriados": "Su,PH",
    r"sáb\.? dom\.? e feriados": "Sa,Su,PH",
    r"sex(tas|\.),? sáb(ados|\.) e v[eé]sp(era|\.)( de)? feriados?": "Fr,Sa,PH -1 day",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{2})h\s*às\s*(\d{2})h": r"\1:00-\2:00",
    r"(\d{2})h\s*às\s*(\d{2})h(\d{2})": r"\1:00-\2:\3",
    r"(\d{1})[:h](\d{2})h?\s*(?:às|-)\s*(\d{2})[:h](\d{2})": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{2})[:h](\d{2})\s*(?:às|-)\s*(\d{2})[:h](\d{2})": r"\1:\2-\3:\4",
    r"encerrad[ao]": r"off",
}
CITIES = {
    "2975-333": "Quinta do Conde",
    "4525-117": "Canedo",
    "9050-299": "São Gonçalo",
    "9600-516": "Ribeira Grande",
    "9900-038": "Horta (Angústias)",
}


def fetch_data():
    def post_process(page):
        page_tree = etree.fromstring(page, etree.HTMLParser())
        return page_tree.xpath("//script[@id='locations-data']/text()")[0]

    result = fetch_json_data(DATA_URL, post_process=post_process)
    result = [
        {
            "info": etree.fromstring(x["infoWindowHtml"], etree.HTMLParser()),
            **{k: v for k, v in x.items() if k not in ("infoWindowHtml",)},
        }
        for x in result
    ]
    return [
        {
            "id": x["info"][0][0].attrib["data-store-id"],
            "phones": [
                re.sub(r"^\s*([';]\s*)?(([^0-9:]+)\s*:?)?\s*(\d+)/?$", r"\3:\4", y.lower()).replace("geral", "").replace("saude", "saúde").split(":")
                for y in x["info"].xpath("//a[contains(@class, 'w-store-locator-phone')]/text()")
            ],
            "services": [y.strip() for y in x["info"].xpath(".//p[contains(@class, 'w-store-service')]/text()")],
            **{k: v for k, v in x.items() if k not in ("info",)}
        }
        for x in result
    ]


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"Wells"](area.country);')]

    for nd in new_data:
        branch = re.sub(r"^Wells\s+", "", titleize(nd["name"]))
        public_id = nd["id"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None and nd["latitude"] is not None and nd["longitude"] is not None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = sorted([[od, distance([od.lat, od.lon], coord)] for od in old_data if not od[REF] and distance([od.lat, od.lon], coord) < 250], key=lambda x: x[1])
            if len(ds) == 1:
                d = ds[0][0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["latitude"] or 38.306893
            d.data["lon"] = nd["longitude"] or -17.050891
            old_data.append(d)

        d[REF] = public_id
        d["shop"] = "chemist"
        d["name"] = "Wells"
        d["brand"] = "Wells"
        d["brand:wikidata"] = "Q115388598"
        d["brand:wikipedia"] = "pt:Wells (lojas)"
        d["branch"] = BRANCHES.get(branch, branch)

        schedule = re.split(r"\s*;\s*", re.sub(r"(\d+[h:]\d+)\.", r"\1;", nd["storeHours"].lower().replace("horário:", "")))
        schedule = [[y.strip() for y in re.sub(r"^([^0-9:]*?)\s*(?=\d|das |encerrad)", r"\1: ", x).split(":", 1)] for x in schedule]
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

            ss = []
            for sa in re.split(r"\s*(?:\be\b|/|,)\s*", s[1]):
                sb = "<ERR>"
                for sma, smb in SCHEDULE_HOURS_MAPPING.items():
                    if re.fullmatch(sma, sa) is not None:
                        sb = re.sub(sma, smb, sa)
                        break
                ss.append(sb)
            s[1] = ",".join(ss)
        if len(schedule) >= 2 and schedule[0][0] == "Mo-Sa" and schedule[1][0] == "Su,PH" and schedule[0][1] == schedule[1][1]:
            schedule = [["Mo-Su,PH", schedule[0][1]], *schedule[2:]]
        if len(schedule) >= 2 and schedule[0][0] == "Mo-Sa" and schedule[1][0] == "PH" and schedule[0][1] == schedule[1][1]:
            schedule = [["Mo-Sa,PH", schedule[0][1]], *schedule[2:]]
        schedule = "; ".join([" ".join(x) for x in schedule])
        d["opening_hours"] = schedule

        mobiles = []
        phones = []
        for comment, phone in nd["phones"]:
            if len(phone) == 9:
                phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
                if comment:
                    phone += f' "{comment}"'
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
        d["contact:website"] = "https://wells.pt/"
        d["contact:facebook"] = "WellsPT"
        d["contact:youtube"] = "https://www.youtube.com/channel/UCaX7TnIZS_c1J5OBqlTgtvQ"
        d["contact:instagram"] = "wells_oficial"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        postcode = nd["postalCode"]
        if len(postcode) == 4:
            postcode += "-000"
        d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, nd["city"])

        if not d["addr:street"] and not (d["addr:housenumber"] or d["addr:housename"] or d["nohousenumber"]) and not d["addr:place"] and not d["addr:suburb"]:
            d["x-dld-addr"] = "; ".join([nd["address1"], nd["address2"]])

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

    write_diff("Wells", REF, old_data)
