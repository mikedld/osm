#!/usr/bin/env python3

import json
import re

from impl.common import (
    DAYS,
    DiffDict,
    country_polygon,
    cover_polygon,
    distance,
    fetch_json_data,
    format_phonenumber,
    merge_weekdays,
    overpass_query,
    titleize,
    write_diff,
)


DATA_URL = "https://back-branchlocator.santander.com/branch-locator/find/pt"
MAX_RADIUS = 100_000

REF = "ref"

CITY_FIXES = {
    "Vila Pouca Aguiar": "Vila Pouca de Aguiar",
    "Canelas Vng": "Canelas",
    "Macedo Cavaleiros": "Macedo de Cavaleiros",
    "Portela Lrs": "Portela",
    "Vila Franca Xira": "Vila Franca de Xira",
    "Charneca Caparica": "Charneca da Caparica",
    "Sta Maria da Feira": "Santa Maria da Feira",
    "V. Real S. António": "Vila Real de Santo António",
    "Pampilhosa Serra": "Pampilhosa da Serra",
    "Celeirós Brg": "Celeirós",
    "Cernache Bonjardim": "Cernache de Bonjardim",
    "Paredes Pnf": "Paredes",
    "Balazar Pvz": "Balazar",
    "Barcelinhos Bcl": "Barcelinhos",
    "Gandra Prd": "Gandra",
    "Oliveira Hospital": "Oliveira do Hospital",
    "São Brás Alportel": "São Brás de Alportel",
    "Calheta (Madeira)": "Calheta",
    "Stª Cruz Graciosa": "Santa Cruz da Graciosa",
    "Ponta Delgada - a. Quental": "Ponta Delgada - A. Quental",
}
POSTCODE_CITY_FIXES = {
    **CITY_FIXES,
    "Estreito Câmara de Lobos": "Estreito de Câmara de Lobos",
}
WEEKDAY2STR = {
    "MONDAY": "Mo",
    "TUESDAY": "Tu",
    "WEDNESDAY": "We",
    "THURSDAY": "Th",
    "FRIDAY": "Fr",
    "SATURDAY": "Sa",
    "SUNDAY": "Su",
}
MONTH_NAMES = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
    "13": "Dec",
}


def fix_city(city):
    city = titleize(city)
    if city in CITY_FIXES:
        return CITY_FIXES[city]
    return city


def fix_postcode_city(city):
    city = titleize(city)
    if city in POSTCODE_CITY_FIXES:
        return POSTCODE_CITY_FIXES[city]
    return city


def fix_working_hours(working_hours):
    if not working_hours or (len(working_hours) == 1 and working_hours[0] in (None, "Encerrado")):
        return ["off"]
    for i in range(len(working_hours)):
        if m := re.fullmatch(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", working_hours[i]):
            working_hours[i] = f"{int(m[1]):02}:{m[2]}-{int(m[3]):02}:{m[4]}"
        else:
            working_hours[i] = f"<ERR:{working_hours[i]}>"
    return working_hours


def safe_remove(s, item, fallback=None):
    try:
        s.remove(item)
    except KeyError:
        return fallback
    return item


ALL_SERVICES = [
    "atm",
    "cash_out",
    "cash_in",
    "cheque_in",
    "prepaid_top_up:mobile",
    "internet_access",
]


def fetch_data():
    pois = {}

    def fetch_impl(coords):
        nonlocal pois

        coords = [coords[1], coords[0]]
        print(f"Fetching data for coordinates: {coords}")

        params = {
            "config": json.dumps({"coords": coords}),
            "filterType": "BRANCH",
        }
        data = fetch_json_data(DATA_URL, params=params)

        max_dist = 0
        for poi in data:
            pois[poi["poicode"]] = poi
            max_dist = max(max_dist, distance(coords, [poi["location"]["coordinates"][1], poi["location"]["coordinates"][0]]))

        return max_dist if data else MAX_RADIUS

    cover_polygon(country_polygon(), MAX_RADIUS, fetch_impl)

    results = list(pois.values())
    results = [r for r in results if r["entityCode"] == "Santander_Totta"]

    return results


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            """
(
    nwr[amenity=bank][name~"Santander"](area.country);
);
"""
        )
    ]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["poicode"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["location"]["coordinates"][1], nd["location"]["coordinates"][0]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["location"]["coordinates"][1]
            d.data["lon"] = nd["location"]["coordinates"][0]
            old_data.append(d)
            new_node_id -= 1

        d["ref"] = public_id
        d["amenity"] = "bank"

        if nd["subType"]["code"] == "WORKCAFE":
            d["name"] = "Santander (Work Café)"
            d["office"] = "coworking"
            d["branch:type"] = ""
        elif nd["subType"]["code"] == "EMPRESAS":
            if d["name"] is None:
                d["name"] = "Santander Empresas"
            d["office"] = ""
            d["branch:type"] = "business"
        else:
            d["name"] = "Santander"
            d["office"] = ""
            d["branch:type"] = ""

        d["operator"] = "Banco Santander (Portugal)"
        d["operator:wikidata"] = "Q4854116"
        d["operator:wikipedia"] = "pt:Banco Santander Portugal"
        d["brand"] = "Santander"
        d["brand:wikidata"] = "Q4854116"
        d["brand:wikipedia"] = "pt:Banco Santander Portugal"
        ref_name = fix_city(nd["name"])
        if ref_name not in d["ref_name"].split(";"):
            d["ref_name"] = ref_name
        if nd["status"]["code"] not in ("In_Service", "IN_SERVICE"):
            d["x-dld-status"] = nd["status"]["code"]
        d["addr:postcode"] = nd["location"]["zipcode"]
        d["addr:city"] = fix_postcode_city(nd["location"]["city"])

        if nd["location"]["urlPhoto"] is not None:
            d["image"] = nd["location"]["urlPhoto"]

        if contacts := nd.get("contactData"):
            phones = []
            if (phone := format_phonenumber(contacts.get("phoneNumber"))) and phone not in phones:
                phones.append(phone)
            if (phone := format_phonenumber(contacts.get("customerPhone"))) and phone not in phones:
                phones.append(phone)
            if phones:
                d["contact:phone"] = ";".join(phones)
            else:
                tags_to_reset.add("contact:phone")
            if fax := format_phonenumber(contacts.get("fax")):
                d["contact:fax"] = fax
            else:
                tags_to_reset.add("contact:fax")
            if (email := contacts.get("email")) and not email.endswith(".local"):
                d["contact:email"] = email
            else:
                tags_to_reset.add("contact:email")

        if socials := nd.get("socialData"):
            d["contact:youtube"] = socials.get("youtubeLink", "")
            d["contact:facebook"] = socials.get("facebookLink", "")
            d["contact:twitter"] = socials.get("twitterLink", "")
            d["contact:linkedin"] = socials.get("linkedinLink", "")
            d["contact:instagram"] = socials.get("instagramLink", "")
            d["contact:tiktok"] = socials.get("tiktokLink", "")

        d["website"] = nd.get("urlDetailPage", "")

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        # Schedule
        opening_hours = ""
        schedule = nd["schedule"]
        working_days = {}

        for workday, worktime in schedule["workingDay"].items():
            wd = WEEKDAY2STR[workday]
            working_hours = ",".join(fix_working_hours(worktime))
            if working_hours not in working_days:
                working_days[working_hours] = []
            working_days[working_hours].append(wd)
        for working_hours, days in working_days.items():
            days.sort(key=lambda s: DAYS.index(s))
            if opening_hours != "":
                opening_hours += "; "
            days = merge_weekdays(days)
            opening_hours += ",".join(days) + " " + working_hours
        days_off = []
        for day in schedule["specialDay"]:
            if fix_working_hours(day["time"]) == ["off"]:
                month, day = day["date"].split("-")
                month_name = MONTH_NAMES[month]
                days_off.append(f"{month_name} {day}")
            else:
                opening_hours = f"<ERR:{schedule}>"
                days_off = []
                break
        if days_off != []:
            opening_hours += "; " + ",".join(days_off) + " off"
        d["opening_hours"] = opening_hours

        services = {}

        # Services
        if "comercialProducts" in nd:
            commercial_products = {product["default"].upper() for product in nd["comercialProducts"]}
            commercial_products -= {
                "CONSULTAS",
                "TRANSFERÊNCIAS",
                "PAGAMENTOS DE SERVIÇOS",
                "REQUISIÇÃO DE CHEQUES",
                "SERVIÇO DE BALCÃO",
                "NÃO TEM SERVIÇO DE CAIXA",
                "APENAS SERVIÇOS COMERCIAIS DISPONÍVEIS",
            }
            if safe_remove(commercial_products, "MULTIBANCO"):
                services["atm"] = "yes"
            if safe_remove(commercial_products, "LEVANTAMENTOS"):
                services["cash_out"] = "yes"
            if safe_remove(commercial_products, "CARREGAMENTOS"):
                services["prepaid_top_up:mobile"] = "yes"
            if safe_remove(commercial_products, "DEPÓSITOS E CHEQUES"):
                services["cash_in"] = "yes"
                services["cheque_in"] = "yes"
            if safe_remove(commercial_products, "LEVANTAMENTOS E DEPÓSITOS"):
                services["cash_out"] = "yes"
                services["cash_in"] = "yes"
            if safe_remove(commercial_products, "LEVANTAMENTOS E CHEQUES"):
                services["cash_out"] = "yes"
                services["cheque_in"] = "yes"
            if safe_remove(commercial_products, "MÁQUINA DE DEPÓSITOS"):
                services["cash_in"] = "yes"
            if safe_remove(commercial_products, "DEPÓSITO DE NOTAS NA CONTA DE TERCEIROS"):
                services["cash_in"] = "yes"

            if commercial_products:
                d["x-dld-commercial-products"] = ";".join(sorted(commercial_products))

        if "dialogAttribute" in nd:
            if "WIFI" in nd["dialogAttribute"]:
                services["internet_access"] = "wlan"
            if "RETIRO_CON_CODIGO" in nd["dialogAttribute"]:
                services["cash_out"] = "yes"
            if "MULTICAJERO" in nd["dialogAttribute"]:
                services["atm"] = "yes"
                services["cash_in"] = "yes"

        for service in ALL_SERVICES:
            d[service] = services.get(service, "")

        if "spokenlanguages" in nd:
            keys = [k for k in list(d.data["tags"]) if k.startswith("language:")]

            languages = {lang.lower() for lang in nd["spokenlanguages"]}

            for k in keys:
                lang = k.split(":", 1)[1]
                if lang.lower() not in languages:
                    d[k] = ""

            for lang in languages:
                if d[f"language:{lang}"] != "yes":
                    d[f"language:{lang}"] = "yes"

        if (
            d.kind == "new"
            and not d["addr:street"]
            and not (d["addr:housenumber"] or d["nohousenumber"] or d["addr:housename"])
        ):
            d["x-dld-addr"] = nd["location"]["address"]

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["poicode"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Santander", REF, old_data)
