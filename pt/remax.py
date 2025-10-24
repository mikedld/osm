#!/usr/bin/env python3

import re
from urllib.parse import urlparse

from impl.common import DiffDict, distance, fetch_json_data, format_phonenumber, overpass_query, titleize, write_diff


DATA_URL = "https://remax.pt/api/Office/PaginatedSearch"

REF = "ref"

BRANCHES = {
    "ConviCtus": "Convictus",
}
OPERATOR_FIXES = (
    # proper names
    (r"\bbizzyland\b", "BizzyLand"),
    (r"\bblueland\b", "BlueLand"),
    (r"\bcirc\b", "CIRC"),
    (r"\bclassalegre\b", "ClassAlegre"),
    (r"\bconquistenigma\b", "ConquistEnigma"),
    (r"\bdd[ ]?vendas\b", "DD Vendas"),
    (r"\bestorilhouse\b", "EstorilHouse"),
    (r"\bfcgm\b", "FCGM"),
    (r"\bgldn\b", "GLDN"),
    (r"\bgoldenloft\b", "GoldenLoft"),
    (r"\binteligentepartilha\b", "InteligentePartilha"),
    (r"\bjc2future\b", "JC2Future"),
    (r"\blpdp in motion\b", "LPDP In Motion"),
    (r"\bmaxlinha\b", "MaxLinha"),
    (r"\bmaxloja\b", "MaxLoja"),
    (r"\bmaxocidente\b", "MaxOcidente"),
    (r"\bmaxselect\b", "MaxSelect"),
    (r"\bmaxvilla\b", "MaxVilla"),
    (r"\bmedipombal\b", "MediPombal"),
    (r"\bmundilocation\b", "MundiLocation"),
    (r"\bnortecerto\b", "NorteCerto"),
    (r"\boceano d eleição\b", "Oceano d'Eleição"),
    (r"\bon the move\b", "On the Move"),
    (r"\bo chefe voltou\b", "O Chefe Voltou"),
    (r"\bpartilhanotável\b", "PartilhaNotável"),
    (r"\bpatrimonioforte\b", "PatrimonioForte"),
    (r"\bd'êxito\b", "d'Êxito"),
    (r"\bportalrumo\b", "PortalRumo"),
    (r"\bproudnumbers\b", "ProudNumbers"),
    (r"\brebelgolden\b", "RebelGolden"),
    (r"\brealwise\b", "RealWise"),
    (r"\bre - inventar\b", "Re-Inventar"),
    (r"\britualnorma\b", "RitualNorma"),
    (r"\bskyimage\b", "SkyImage"),
    (r"\bskyreal\b", "SkyReal"),
    (r"\bsuccessagain\b", "SuccessAgain"),
    (r"\btaguscasa\b", "TagusCasa"),
    (r"\bteclaperfeita\b", "TeclaPerfeita"),
    (r"\burbanland\b", "UrbanLand"),
    (r"\bwematch\b", "WeMatch"),
    (r"\bwhiteone\b", "WhiteOne"),
    (r"\bwhitetwo\b", "WhiteTwo"),
    (r"\bwonderfulsix\b", "WonderfulSix"),
    (r"\bworldwidexl\b", "WorldWideXL"),
    # abbreviations
    (r"(?<=- )\bimob\.", "Imobiliária"),
    (r"(?<=mediação )\bimob\.", "Imobiliária"),
    (r"(?<=atividades )\bimob\b\.?", "Imobiliárias"),
    (r"\bmed\.? imob\b\.?", "Mediação Imobiliária"),
    (r"\bmed\.?(?= imob| e ativ)", "Mediação"),
    (r"\bmed\.(?= imob| e constr)", "Mediação"),
    (r"\bsoc\.( de)?(?= med)", "Sociedade de"),
    (r"\bsociedade(?= med)", "Sociedade de"),
    (r"(?<=\bsoluções )imob\.", "Imobiliárias"),
    (r"\bunip\b\.?", "Unipessoal"),
    # forms of ownership
    (r"(\s*[-,])?\s*\bunipessoal\b", ", Unipessoal"),
    (r"(\s*[-,])?\s*\blda\b\.?", ", Lda."),
    (r"(\s*[-,])?\s*\bs\.a\b\.?", ", S.A."),
    # dashes
    (r"\b(?<!de)[\s>,]+\b((sociedade de )?mediação\b|soluções imob)", r" - \1"),
    # typos and other mistakes
    (r"\bmedição\b", "Mediação"),
    (r"\bimobiliari([ao])", r"Imobiliári\1"),
)
YOUTUBE_CHANNELS = {
    "UC0l_7DV4OhqwNh34es5Y-nw": "@REMAXPRO-porto",
    "UC1Sc_B1YlgIX0mUadegmubA": "@remax_braga",
    "UC32xdTs9NCUQ5XlU_2sUntA": "@remaxgrupoconvictus5876",
    "UC4-IMPeL1VTJCEJDoXn1knA": "@GrupoREMAXLatinaOficial",
    "UC4INSEE__VcAAhCJEFD0zJw": "@remax_spazio",
    "UC4s_knioPpVxC5tLubBSx_Q": "@remaxhappy5180",
    "UC6dnYf13EfW-arW6OtCSEBg": "@remaxconfianca917",
    "UC7nQxjKsF4US9Kbu5S9cwsQ": "@remaxmatch2999",
    "UCaED3RLnHpH5zF5l1aDVhEQ": "@remaxpinheiromanso7651",
    "UCaQeyo48QswKhgisghkebqQ": "@remaxplacestrada6512",
    "UCAvxKEVpNi-XdBxZdlxjU6w": "@remaxdinamica",
    "UCb1AEHdrnDAYc-sCG4m3NOQ": "@remaxliberty3744",
    "UCcEZfPvmXLMOHaPckoLQTyg": "@gruporemaxinn",
    "UCD0s8cZEakAIT5_4Bu1OeJQ": "@remaxpassioncenter",
    "UCdwVHpygvBsRMeSlxp-bbmQ": "@remaxvila1923",
    "UCex8Sjs2PygctGLgvPiizwg": "https://www.youtube.com/c/GrupoREMAXNeg%C3%B3cios",
    "UCeYDPra6bK60bslIPUNCLCg": "@gruporemaxteam",
    "UCf4tIhn4dfcdTUPSAoPbIsg": "@remax.capitulo",
    "UCfAYsDqU8hFaTwqdbUypYlw": "@grupoon3785",
    "UCFb8r8fiRlAzORJ51WXD78A": "@remaxnext8443",
    "UCFn4nEnTiglrkNGwvss2p_Q": "@remaxdirect1451",
    "UCgWlAU-gJoPWxmAAmtcUqPw": "@remaxuniversalportugal",
    "UCI7I3MT1OIvuZbkh8UjFJTw": "@remaxviva7221",
    "UCI8-RtBtb9CCeMrbA4JJvUg": "@remaxgrupogoldenline",
    "UCJq1ounrS4FwDK9smpZxTpQ": "@remaxpontedesor2716",
    "UCK4Ohw1-d4XM-t9CEmzhThw": "@cidadelaremax9106",
    "UCMPJIba7o9cgUVF1W5dmMSw": "@remaxpluspt",
    "UCo2js35eVkpIGRqjAsO9DwA": "@vitorfreitasremaxready",
    "UCOpT2Itbe6rBZm29mFpIf1g": "@remaxmatosinhos6780",
    "UCpm1ZMdisxxhU154yxS2x6Q": "@remaxrigor3065",
    "UCqfS9Ek5UfbNq8bCVsTmfvQ": "@remaxyes7983",
    "UC-Rd-m63cr00UGpKqGlIP1A": "@remaxatitude",
    "UCS6_7U3VdIPYhof6jVGlN3Q": "@remaxcostadosol704",
    "UCsHJsUUtJYX1AgANwP8sT-w": "@remaxchampion7937",
    "UCTIoyFbepurlqGwh9VWIHgg": "@remaxconsigo5731",
    "UCu1P8N23TnlEdWFKgqIj7_A": "@marketingremaxsky8249",
    "UCu75cLGJ-TjNMjgOzDvr47A": "@remaxgap5169",
    "UCUjJbXXDNaoEfZLtyS3dLrA": "@remaxcerne",
    "UCUv_X6A933HWf8OHnutOCqA": "@remaxmaia3951",
    "UCV8SgiF8_jre8Xh3DAeXLRQ": "@remaxalanorte",
    "UCVMZS2sYvLSeLX2_1oq2GZg": "@remaxg4",
    "UCwdey-XzepK1f57_sBfISpg": "@remaxgrupoempire",
    "UCwgSFJ5xUya47i5ExCabmHA": "@remaxelite-madeiraportosan449",
    "UCWVvmgdWg3ukW5eIuOY2h6w": "@remaxradialmarketing9621",
    "UC_xMIUiLh9BOurkG90KqQdw": "@GrupoREMAXRumo",
    "UCxONeCfUFMwVkHmrGzVx5VQ": "@remaxportugal314",
    "UCycXvONjUEuZzulWV7VCDhw": "@RemaxSilver",
    "UCyjKSvB4gmHycppBulDzn-Q": "@remaxgrupomove",
    "UCymAuI1F800_Bk29atvh6nA": "@remaxalianca1835",
    "UCZZs6J71PCUbBCIwh6P9PFQ": "@remaxmarques1240",
}
YOUTUBE_EXCEPTIONS = {
    "expogrouptv": "@ExpoGroupTV",
    "remaxpaixao": "@RemaxPaixao",
}
LINKEDIN_COMPANIES = {
    "10564050": "re-max-matosinhos",
    "11261087": "remaxcollectionvintage",
    "11515263": "remaxelite-madeira-portosanto",
    "18772216": "remaxyes",
    "19038659": "remax-alpha-portugal",
    "37458522": "remax-barcovez",
    "66920215": "remax-consigo",
}


def fetch_data():
    payload = {
        "filters": [],
        "pageNumber": 1,
        "pageSize": 9999,
        "sort": ["OfficeNameToSort"],
        "searchValue": "",
    }
    return fetch_json_data(DATA_URL, json=payload)["results"]


def fixup_operator(v):
    v = titleize(re.sub(r"\s*[-–]\s*", " - ", v))
    for a, b in OPERATOR_FIXES:
        v = re.sub(a, b, v, flags=re.IGNORECASE)
    return v


def fixup_media_instagram(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("instagram.com", "instagram.pt"):
        path_parts = parts.path.strip("/").split("/")
        v = parts.path.strip("/ ") if len(path_parts) == 1 else ""
    elif re.fullmatch(r"[0-9a-z_.]+", handle := v.strip("/ ")):
        v = handle
    else:
        v = ""
    if v != "remaxportugal":
        v = f"{v};remaxportugal".strip(";")
    return v


def fixup_media_tiktok(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("tiktok.com",):
        path_parts = parts.path.strip("/").split("/")
        v = path_parts[0] if len(path_parts) == 1 and path_parts[0].startswith("@") else ""
    elif re.fullmatch(r"@[0-9a-z_.]+", handle := v.strip("/ ")):
        v = handle
    else:
        v = ""
    return v


def fixup_media_facebook(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("facebook.com", "facebook.pt", "m.facebook.com", "pt-pt.facebook.com"):
        path_parts = parts.path.strip("/").split("/")
        if len(path_parts) == 1:
            v = path_parts[0]
        elif len(path_parts) == 3 and path_parts[0] == "people":
            pass
        else:
            v = ""
    else:
        v = ""
    if v != "remaxportugal":
        v = f"{v};remaxportugal".strip(";")
    return v


def fixup_media_twitter(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("twitter.com",):
        path_parts = parts.path.strip("/").split("/")
        v = path_parts[0] if len(path_parts) == 1 else ""
    else:
        v = ""
    return v


def fixup_media_youtube(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("youtube.com",):
        path_parts = parts.path.strip("/").split("/")
        if len(path_parts) == 1 and path_parts[0].startswith("@"):
            v = path_parts[0]
        elif len(path_parts) == 2 and path_parts[0] in ("user", "c"):
            v = f"https://www.youtube.com/{path_parts[0]}/{path_parts[1]}"
        elif len(path_parts) >= 2 and path_parts[0] == "channel" and (handle := YOUTUBE_CHANNELS.get(path_parts[1])):
            v = handle
        elif exception := YOUTUBE_EXCEPTIONS.get("/".join(path_parts)):
            v = exception
        else:
            v = ""
    else:
        v = ""
    if v != "@remaxportugal314":
        v = f"{v};@remaxportugal314".strip(";")
    return v or ""


def fixup_media_linkedin(v):
    if "/" in v.strip("/ ") and not v.startswith("http"):
        v = f"https://{v}"
    parts = urlparse(v)
    if parts.netloc.removeprefix("www.").lower() in ("linkedin.com", "pt.linkedin.com"):
        path_parts = parts.path.strip("/").split("/")
        if len(path_parts) >= 2 and path_parts[0] == "company":
            if handle := LINKEDIN_COMPANIES.get(path_parts[1]) if re.fullmatch(r"[0-9]+", path_parts[1]) else path_parts[1]:
                v = f"https://www.linkedin.com/company/{handle}/"
            else:
                v = ""
        elif len(path_parts) >= 2 and path_parts[0] == "in":
            v = f"https://www.linkedin.com/in/{path_parts[1]}/"
        else:
            v = ""
    else:
        v = ""
    if not v.endswith("/company/remaxportugal/"):
        v = f"{v};https://www.linkedin.com/company/remaxportugal/".strip(";")
    return v or ""


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "("
            'nwr[office][~"^(name|brand)$"~"re/?max",i](area.country);'
            'nwr[shop][~"^(name|brand)$"~"re/?max",i](area.country);'
            ");"
        )
    ]

    for nd in new_data:
        public_id = nd["officeNumber"]
        branch = nd["officeName"].removeprefix("RE/MAX ")
        branch = BRANCHES.get(branch, branch)
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [nd["latitude"], nd["longitude"]]
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
        d["office"] = "estate_agent"
        d["name"] = f"RE/MAX {branch}"
        d["brand"] = "RE/MAX"
        d["brand:wikidata"] = "Q965845"
        d["brand:wikipedia"] = "pt:RE/MAX"
        d["branch"] = branch
        d["operator"] = fixup_operator(nd["entityTypeName"].strip())

        tags_to_reset.add("shop")

        new_langs = {x["languageCode"].lower() for x in nd["languagesSpoken"]}
        old_langs = {k[9:] for k in d.data["tags"] if k.startswith("language:")}
        for lang in new_langs | old_langs:
            d[f"language:{lang}"] = "yes" if lang in new_langs else ""

        if phones := [x for x in (format_phonenumber(nd["phoneNumber"] or ""), format_phonenumber(nd["cellPhone"] or "")) if x]:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        if email := nd["email"]:
            d["contact:email"] = email
        else:
            tags_to_reset.add("contact:email")
        d["website"] = f"https://www.remax.pt/{nd['publicName']}"
        medias = {x["socialMediaChannelName"].lower(): x["socialMediaURL"] for x in nd["socialMediaUrls"]}
        for media in ("instagram", "tiktok", "facebook", "twitter", "youtube", "linkedin"):
            d[f"contact:{media}"] = globals()[f"fixup_media_{media}"](medias.get(media, "").strip())

        tags_to_reset.update({"phone", "mobile", "url", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        if len(d["addr:postcode"]) != 8:
            d["addr:postcode"] = nd["zipCode"]
        if not d["addr:city"] or d["addr:city"] not in [
            x
            for y in (nd["regionName1"], nd["regionName2"], nd["regionName3"], nd["regionName4"])
            for x in re.split(r"\s+e\s+|\s*,\s*", y or "")
        ]:
            d["addr:city"] = "; ".join(
                [x for x in (nd["regionName1"], nd["regionName2"], nd["regionName3"], nd["regionName4"]) if x]
            )
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join([x for x in (nd["officeAddress"], nd["doorNumber"]) if x])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["officeNumber"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("RE/MAX", REF, old_data, osm=True)
