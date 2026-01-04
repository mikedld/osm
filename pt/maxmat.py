#!/usr/bin/env python3

import re

from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, format_phonenumber, overpass_query, titleize, write_diff


DATA_URL = "https://www.maxmat.pt/pt/contactos-de-lojas_421.html"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"(De )?Segunda a Sábados? e Feriados": "Mo-Sa,PH",
    r"Domingos": "Su",
    r"Todos os dias": "Mo-Su",
}
SCHEDULE_HOURS_MAPPING = {
    r"(\d{1})h(\d{2}) às (\d{2})h(\d{2})": r"0\1:\2-\3:\4",
    r"(\d{2})h(\d{2}) às (\d{2})h(\d{2})": r"\1:\2-\3:\4",
}
BRANCHES = {
    "Marco Canaveses": "Marco de Canaveses",
    "S. J. Madeira": "São João da Madeira",
}
CITIES = {
    "3700-268": "São João da Madeira",
    "4470-045": "Castêlo da Maia",
    "8200-000": "Guia",
    "9060-382": "Funchal",
}


def fetch_data():
    def post_process(page):
        page = re.sub(r"^.*\bJSVarsStores\s*=", "", page, flags=re.DOTALL)
        page = re.sub(r"\bvar\s+ocvar\b.*$", "", page, flags=re.DOTALL)
        page = re.sub(
            r"\b("
            r"tile_map_server|googleapis|showStoresDetail|page|url|shop|CDN|country_code|expressions|addresses|countryID|"
            r"countryName|stores|id|newid|name|coordinates|street|address1|zip|city|email|phone|fax|short_content|schedule|\d+"
            r")\s*:",
            r'"\1":',
            page,
        )
        page = re.sub(r"\}\s*,\s*\}", "}}", page, flags=re.DOTALL)
        page = page.replace("'", '"')
        return page

    result = fetch_json_data(DATA_URL, encoding="latin1", post_process=post_process)["addresses"]
    result = [{**v, "stores": [vv for kk, vv in v["stores"].items()]} for k, v in result.items()]
    result = [s for x in result for s in x["stores"] if x["countryID"] in ("176", "247")]
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

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"max[ ]?mat",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        tags_to_reset = set()

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
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = "doityourself"
        d["name"] = "Maxmat"
        d["brand"] = "Maxmat"
        d["brand:wikidata"] = "Q137691265"
        d["branch"] = BRANCHES.get(nd["name"], nd["name"])

        schedule = [re.split(r"\s*:\s*(?:das\s+)?", x.strip(". ")) for x in nd["schedule"].split(".") if x]
        schedule = [[schedule_time(x[0], SCHEDULE_DAYS_MAPPING), schedule_time(x[1], SCHEDULE_HOURS_MAPPING)] for x in schedule]
        d["opening_hours"] = "; ".join([" ".join(x) for x in schedule])
        d["source:opening_hours"] = "website"

        if phone := format_phonenumber(re.sub(r"\s*\(Chamada[^)]*\)\s*", "", nd["phone"])):
            d["contact:phone"] = phone
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = f"{DATA_URL}?idst={public_id}"
        d["contact:facebook"] = "maxmatpt"
        d["contact:youtube"] = "@maxmatonline"
        d["contact:instagram"] = "maxmatpt"
        d["contact:linkedin"] = "https://www.linkedin.com/company/maxmatpt"
        d["contact:tiktok"] = "maxmatpt"
        d["contact:email"] = f"{nd['email']};maxmat.cliente@maxmat.pt".strip(";")

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        d["addr:postcode"] = nd["zip"]
        d["addr:city"] = CITIES.get(d["addr:postcode"], titleize(nd["city"]))
        if not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"]):
            d["x-dld-addr"] = "; ".join(
                [x.strip() for x in etree.fromstring(nd["street"], etree.HTMLParser()).xpath("//text()")]
            )

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Maxmat", REF, old_data)
