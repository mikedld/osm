#!/usr/bin/env python3

import datetime
import json
import re

from impl.common import BASE_DIR, BASE_NAME, LISBON_TZ, DiffDict, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://storage.googleapis.com/pro-bucket-wcorp-files/json/data.js"

REF = "ref"

STREET_ABBREVS = [
    [r"\bal\.? ", "alameda "],
    [r"\bav\.? ", "avenida "],
    [r"\bdr\. ", "doutor "],
    [r"\bdra\. ", "doutora "],
    [r"\beng\. ", "engenheiro "],
    [r"\bestr\.? ", "estrada "],
    [r"\bestrada n\. ", "estrada nacional "],
    [r"\br\.? ", "rua "],
    [r"\btv\.? ", "travessa "],
]
STREET_FIXUPS = {
    "barreiro | rua dos resistentes anti-fascistas": "rua resistentes anti-fascistas",
    "braga | avenida doutor antónio palha": "avenida doutor antónio alves palha",
    "coimbra | estrada eiras": "estrada da ribeira de eiras",
    "guimarães | rua eduardo manuel de almeida": "rua eduardo manuel josé de almeida",
    "loures | rua dom afonso albuquerque": "rua dom afonso de albuquerque",
    "maia | avenida josé afonso m. de figueiredo": "avenida do engenheiro josé afonso moreira de figueiredo",
    "marco de canaveses | travessa dos bombeiros voluntários": "travessa dos bombeiros voluntários do marco de canaveses",
    "torres vedras | avenida poente": "variante poente",
    "trofa | rua d. pedro v": "rua dom pedro v",
    "valongo | rua jose joaquim ribeiro teles": "avenida engenheiro josé joaquim ribeiro teles",
    "vila nova de gaia | rua de raimundo de carvalho": "rua raimundo de carvalho",
    "vila nova de gaia | rua rechousa": "rua da rechousa",
    "viseu | estrada nacional 231": "estrada de nelas",
}


def fetch_data():
    def post_process(page):
        page = re.sub(r"^var dataJson\s*=\s*", "", page)
        page = re.sub(r";$", "", page)
        return page

    params = {
        "timestamp": datetime.datetime.now(datetime.UTC).astimezone(LISBON_TZ).strftime("%s000"),
    }
    result = fetch_json_data(DATA_URL, params=params, post_process=post_process)
    result = [x for x in result["tiendasFull"] if x["p"] == "PT"]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query("nwr[shop][name=Mercadona](area.country);")]

    custom_ohs = {}
    custom_ohs_file = BASE_DIR / f"{BASE_NAME}-custom-ohs.json"
    if custom_ohs_file.exists():
        custom_ohs = json.loads(custom_ohs_file.read_text())

    for nd in new_data:
        private_id = str(nd["id"])
        public_id = str(nd["site_public_id"])
        d = next((od for od in old_data if od[REF] == public_id or od[REF][1:] == private_id[1:]), None)
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["lt"]
            d.data["lon"] = nd["lg"]
            old_data.append(d)

        tags_to_reset = set()

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["name"] = "Mercadona"
        d["brand"] = "Mercadona"
        d["brand:wikidata"] = "Q377705"
        d["brand:wikipedia"] = "pt:Mercadona"

        opens = list(set(nd["in"].split("#")))
        closes = list(set(nd["fi"].split("#")))
        if len(opens) == 1 and len(closes) == 1:
            t = f"{opens[0][:2]}:{opens[0][2:]}-{closes[0][:2]}:{closes[0][2:]}"
            if d["opening_hours"] != f"Mo-Su,PH {t}":
                d["opening_hours"] = f"Mo-Su {t}"
        else:
            d["opening_hours"] = f"ERR: '{nd['in']}', '{nd['fi']}'"

        d["contact:phone"] = f"+351 {nd['tf'][0:3]} {nd['tf'][3:6]} {nd['tf'][6:9]}"
        d["website"] = "https://www.mercadona.pt/"
        d["contact:facebook"] = "MercadonaPortugal"
        d["contact:twitter"] = "Mercadona_pt"
        d["contact:youtube"] = "mercadonaportugal"
        d["contact:instagram"] = "mercadona_portugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/mercadonaportugal"
        d["contact:email"] = "apoiocliente@mercadona.com"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:addr"] != "survey":
            d["source:addr"] = "website"
        if d["source:contact"] != "survey":
            d["source:contact"] = "website"
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        locality = titleize(nd["lc"])
        if d["addr:municipality"] != locality:
            d["addr:municipality"] = locality
        d["addr:postcode"] = nd["cp"]

        m = re.match(r"^(.+?), (\d+|S/N.*?)\.?$", nd["dr"])
        if m is not None:
            street = m[1].lower()
            for r in STREET_ABBREVS:
                street = re.sub(r[0], r[1], street)
            street = STREET_FIXUPS.get(f"{locality.lower()} | {street}", street)
            d["addr:street"] = titleize(street)

            if m[2].startswith("S/N"):
                if d["addr:housenumber"]:
                    d["addr:housenumber"] = ""
                d["nohousenumber"] = "yes"
            else:
                if d["nohousenumber"]:
                    d["nohousenumber"] = ""
                d["addr:housenumber"] = m[2]
        else:
            d["x-dld-addr"] = nd["dr"]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

        custom_oh = nd.get("fs")
        if custom_oh:
            for coh in custom_oh.split("#"):
                if public_id not in custom_ohs:
                    custom_ohs[public_id] = []
                if coh not in custom_ohs[public_id]:
                    custom_ohs[public_id].append(coh)
                if not coh.endswith("-FA"):
                    print(f"Not open all day: #{public_id}, {coh}")

    custom_ohs_file.write_text(json.dumps(custom_ohs))

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == str(nd["site_public_id"]) or ref[1:] == str(nd["id"])[1:]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Mercadona", REF, old_data)
