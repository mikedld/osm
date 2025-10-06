#!/usr/bin/env python3

import re

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.agriloja.pt/pt/as-nossas-lojas_596.html"

REF = "ref"

SCHEDULE_DAYS = {
    "Seg. a Sex": "Mo-Fr",
    "Seg. a Sáb": "Mo-Sa",
    "Seg. a Sáb.": "Mo-Sa",
    "Seg. a Dom": "Mo-Su",
    "Seg. a Dom.": "Mo-Su",
    "Sáb": "Sa",
    "Sábados": "Sa",
    "Dom": "Su",
    "Domingo": "Su",
    "Feriados": "PH",
}
SCHEDULE_DAYS_EX = {
    "Feriados Civis": ("PH", "civil only"),
    "Feriados Religiosos": ("PH", "religious only"),
}
SCHEDULE_HOURS = {
    "Encerrada": "off",
    "08h às 20h": "08:00-20:00",
    "08h30 às 18h": "08:30-18:00",
    "8h30 às 18h30": "08:30-18:30",
    "08h30 às 19h30": "08:30-19:30",
    "8h30 às 20h": "08:30-20:00",
    "08h30 às 20h": "08:30-20:00",
    "08h30 às 20h00": "08:30-20:00",
    "08h30 às 20h30": "08:30-20:30",
    "09h às 12h30 e das 13h30 às 19h": "09:00-12:30,13:30-19:00",
    "09h às 13h": "09:00-13:00",
    "09h às 13h e das 14h às 19h": "09:00-13:00,14:00-19:00",
    "09h às 18h": "09:00-18:00",
    "09h às 18h30": "09:00-18:30",
    "9h às 19h": "09:00-19:00",
    "09h às 19h": "09:00-19:00",
    "09h às 19h30": "09:00-19:30",
    "09h às 20h": "09:00-20:00",
    "09h30 às 19h": "09:30-19:00",
    "10h às 12h30 e das 13h30 às 19h": "10:00-12:30,13:30-19:00",
    "10h às 13h e das 15h às 19h": "10:00-13:00,15:00-19:00",
}
SCHEDULE_HOURS_EX = {
    "08h às 20h (incluindo feriados)": ("08:00-20:00", "PH"),
}


def fetch_data():
    def post_process(page):
        page = re.sub(r"^.*\baddresses:", "", page, flags=re.DOTALL)
        page = re.sub(r"\].*", "]", page, flags=re.DOTALL)
        page = re.sub(
            r"\b(id|name|coordinates|street|zip|city|short_content|phone|fax|country|country_name|email|schedule|image|zoneID):",
            r'"\1":',
            page,
        )
        page = re.sub(r"\}\s*,\s*\]", "}]", page, flags=re.DOTALL)
        page = page.replace("'", '"')
        return page

    result = fetch_json_data(DATA_URL, encoding="latin1", post_process=post_process)
    result = [x for x in result if x["country"] == "176"]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query("nwr[shop][name=Agriloja](area.country);")]

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = list(map(float, re.split(r"\s*[,;]\s*", nd["coordinates"].strip())))[:2]
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

        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "agrarian"
        d["name"] = "Agriloja"

        schedule = nd["schedule"].split("<br>")
        if schedule:
            result = ""
            for line in schedule:
                parts = [x.strip() for x in line.split(":")]
                days = SCHEDULE_DAYS.get(parts[0])
                comment = ""
                if not days:
                    days, comment = SCHEDULE_DAYS_EX.get(parts[0], (None, None))
                hours = SCHEDULE_HOURS.get(parts[1])
                more_days = ""
                if not hours:
                    hours, more_days = SCHEDULE_HOURS_EX.get(parts[1], (None, None))
                if days and more_days:
                    days += f",{more_days}"
                if days and hours:
                    if result:
                        result += "; "
                    result += f"{days} {hours}"
                    if comment:
                        result += f' "{comment}"'
            d["opening_hours"] = result

        phone = nd["phone"]
        if phone:
            d["contact:phone"] = f"+351 {phone[7:18]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://www.agriloja.pt/"
        d["contact:facebook"] = "Agriloja"
        d["contact:youtube"] = "grupoagriloja"
        d["contact:instagram"] = "agriloja"
        d["contact:linkedin"] = "https://www.linkedin.com/company/_agriloja"
        d["contact:email"] = nd["email"]

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:addr"] != "survey":
            d["source:addr"] = "website"
        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        d["addr:city"] = titleize(nd["city"])
        d["addr:postcode"] = nd["zip"]

        if not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"]):
            d["x-dld-addr"] = nd["street"]

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

    write_diff("Agriloja", REF, old_data)
