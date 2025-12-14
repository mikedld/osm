#!/usr/bin/env python3

import re
from multiprocessing import Pool

import requests
from lxml import etree

from impl.common import DiffDict, distance, fetch_json_data, format_phonenumber, overpass_query, titleize, write_diff


PAGE_URL = "https://www.era.pt/agencias"
LEVEL1_DATA_URL = "https://www.era.pt/API/ServicesModule/agencies/map"
LEVEL2_DATA_URL = "https://www.era.pt/API/ServicesModule/agencies/card"

REF = "ref"

OPERATOR_FIXES = (
    # proper names
    (r"\bacontecevalor\b", "AconteceValor"),
    (r"\baçorbase\b", "AçorBase"),
    (r"\badvancedpurposes\b", "AdvancedPurposes"),
    (r"\baim2achieve\b", "Aim2Achieve"),
    (r"\balbatrossworld\b", "AlbatrossWorld"),
    (r"\balbuincome\b", "AlbuIncome"),
    (r"\bamb2f\b", "AMB2F"),
    (r"\bamcfarinha\b", "AMCFarinha"),
    (r"\bbesthomes\b", "BestHomes"),
    (r"\bchasingideas\b", "ChasingIdeas"),
    (r"\bcmi - gest\b", "CMI-Gest"),
    (r"\bcr2i\b", "CR2I"),
    (r"\bdadosuper\b", "DadoSuper"),
    (r"\bera\b", "ERA"),
    (r"\beratrofa\b", "ERATrofa"),
    (r"\bespoinvest\b", "EspoInvest"),
    (r"\bexplorar100parar\b", "Explorar100Parar"),
    (r"\bguia&vitorino\b", "Guia & Vitorino"),
    (r"\bhouseview\b", "HouseView"),
    (r"\bimpetuosocasião\b", "ImpetuosOcasião"),
    (r"\binvestpeople\b", "InvestPeople"),
    (r"\bjnl\b", "JNL"),
    (r"\blivingmoods\b", "LivingMoods"),
    (r"\blivremaneira\b", "LivreManeira"),
    (r"\bm3f\b", "M3F"),
    (r"\bmediaprimavera\b", "MediaPrimavera"),
    (r"\bmnz\b", "MNZ"),
    (r"\bmtf\b", "MTF"),
    (r"\bnfpt\b", "NFPT"),
    (r"\bnt sim\b", "NT SIM"),
    (r"\bo vizinho\b", "O Vizinho"),
    (r"\bpdreams\b", "PDreams"),
    (r"\bpensarenvolvente\b", "PensarEnvolvente"),
    (r"\bpineu\b", "Pinéu"),
    (r"\bplss\b", "PLSS"),
    (r"\bprediglobal\b", "PrediGlobal"),
    (r"\bpropertyland\b", "PropertyLand"),
    (r"\bprosperfavorite\b", "ProsperFavorite"),
    (r"\bquimeraudaz\b", "QuimerAudaz"),
    (r"\brabbit's\b", "Rabbit's"),
    (r"\brodrigues e ferreira\b", "Rodrigues & Ferreira"),
    (r"\bsoftevidence\b", "SoftEvidence"),
    (r"\bsw\b", "SW"),
    (r"\bvalorbase\b", "ValorBase"),
    (r"\bvilazigzag\b", "VilaZigzag"),
    # abbreviations
    (r"(?<=- )m\. ?i\.", "Mediação Imobiliária"),
    (r"\bs\. ?m\. ?i\.", "Sociedade de Mediação Imobiliária"),
    (r"(?<=mediação )\bimob\.", "Imobiliária"),
    (r"\bmedi?\.? imob\b\.?", "Mediação Imobiliária"),
    (r"\bmed\.?(?= imob)", "Mediação"),
    (r"\bmed\. ?(?=imob)", "Mediação "),
    (r"\bsoc(\.|iedade)( de)?(?= med)", "Sociedade de"),
    (r"\bsmi\b", "Sociedade de Mediação Imobiliária"),
    (r"\bunip?\b\.?", "Unipessoal"),
    # forms of ownership
    (r"(\s*[-,])?\s*\bunipessoal\b", ", Unipessoal"),
    (r"(\s*[-,.])?\s*\blda\b\.?", ", Lda."),
    # dashes
    (r"\b(?<!de)[\s>,]+\b((sociedade de )?mediação\b|(soluções|serviços) imob)", r" - \1"),
    # typos and other mistakes
    (r"\bimobi?liari([ao])", r"Imobiliári\1"),
)
CITIES = {
    "2620-315": "Ramada",
    "2655-333": "Ericeira",
    "2745-755": "Queluz",
    "2765-278": "Estoril",
    "2820-190": "Charneca de Caparica",
    "2825-336": "Costa da Caparica",
    "2855-366": "Corroios",
    "2955-112": "Pinhal Novo",
    "4415-284": "Pedroso",
    "4445-485": "Ermesinde",
    "7645-011": "Vila Nova de Milfontes",
    "8365-100": "Armação de Pêra",
    "9125-035": "Caniço",
}


def get_api_headers():
    result = requests.get(PAGE_URL, timeout=30)
    result_etree = etree.fromstring(result.content.decode("utf-8"), etree.HTMLParser())
    return {
        "Cookie": f"__RequestVerificationToken={result.cookies['__RequestVerificationToken']}",
        "RequestVerificationToken": result_etree.xpath("//input[@name='__RequestVerificationToken']/@value")[0],
    }


def fetch_level1_data(api_headers):
    payload = {}
    headers = {"Content-Type": "application/json"}
    return fetch_json_data(LEVEL1_DATA_URL, json=payload, headers=headers, var_headers=api_headers)["Agencies"]


def fetch_level2_data(data, api_headers):
    payload = {"id": data["AgencyId"], "idsalesoffice": data["IdSalesOffice"]}
    headers = {"Content-Type": "application/json"}
    return fetch_json_data(LEVEL2_DATA_URL, json=payload, headers=headers, var_headers=api_headers)


def get_ref(nd):
    ref = str(nd["Id"])
    if office_id := nd["IdSalesOffice"]:
        ref += f"-{office_id}"
    return ref


def fixup_operator(v):
    v = titleize(re.sub(r"\s*[-–]\s*", " - ", v).replace("´", "'"))
    for a, b in OPERATOR_FIXES:
        v = re.sub(a, b, v, flags=re.IGNORECASE)
    return v


if __name__ == "__main__":
    api_headers = get_api_headers()

    new_data = fetch_level1_data(api_headers)
    with Pool(4) as p:
        new_data = list(p.starmap(fetch_level2_data, ((x, api_headers) for x in new_data)))

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "("
            r'nwr[office=estate_agent][~"^(name|brand)$"~"\\bera\\b",i](area.country);'
            r'nwr[shop=estate_agent][~"^(name|brand)$"~"\\bera\\b",i](area.country);'
            ");"
        )
    ]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = get_ref(nd)
        branch = nd["Name"].removeprefix("ERA ").strip()
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["Location"]["lat"], nd["Location"]["lng"]]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
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
        d["office"] = "estate_agent"
        d["name"] = "ERA Imobiliária"
        d["brand"] = "ERA Imobiliária"
        d["brand:wikidata"] = "Q121496901"
        # d["brand:wikipedia"] = "en:Anywhere Real Estate"  # noqa: ERA001
        d["branch"] = branch
        d["operator"] = fixup_operator(nd["LegalDesignation"].strip())

        tags_to_reset.add("shop")

        if phone := format_phonenumber(nd["Phone"]):
            d["contact:phone"] = phone
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["Email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.add("contact:email")
        d["website"] = f"https://www.era.pt{nd['URL']}"
        if "eraimobiliaria" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};eraimobiliaria".strip(";")
        d["contact:youtube"] = "@eraimobiliaria"
        d["contact:instagram"] = "eraportugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/eraportugal/"
        d["contact:pinterest"] = "eraportugal"

        tags_to_reset.update({"email", "phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        if m := re.fullmatch(r"(\d{4}-\d{3})\s+(.+)", nd["PostTown"]):
            d["addr:postcode"] = m[1]
            d["addr:city"] = CITIES.get(d["addr:postcode"], m[2])
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = nd["Address"].strip()

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("ERA", REF, old_data, osm=True)
