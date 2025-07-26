#!/usr/bin/env python3

import json
import itertools
import re
from multiprocessing import Pool

from lxml import etree

from impl.common import \
    DiffDict, \
    fetch_json_data, \
    fetch_html_data, \
    overpass_query, \
    titleize, \
    opening_weekdays, \
    distance, \
    write_diff, \
    DAYS, \
    format_phonenumber, \
    merge_weekdays

DATA_URL = "https://back-branchlocator.santander.com/branch-locator/find/pt"

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
    "Canelas Vng": "Canelas",
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
    "SUNDAY": "Su"
}

MONTH_NAMES =  {
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

def fixWorkingHours(workingHours):
    for i in range(len(workingHours)):
        if "-" in workingHours[i]:
            start, finish = workingHours[i].split("-")
            # as numbers
            startH, startM = map(int, start.split(":")) 
            finishH, finishM = map(int, finish.split(":"))
            start = f"{startH:02d}:{startM:02d}"
            finish = f"{finishH:02d}:{finishM:02d}"
            workingHours[i] = f"{start}-{finish}"
        else:
            continue
    return workingHours

def frange(x, y, jump):
    epsilon = jump * 0.1
    while x < y + epsilon:
        yield x
        x += jump

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

    # This can be optimized by fetching in parallel, but for simplicity we will fetch sequentially.
    
    # Continental Portugal
    for lat in frange(37.0, 42.0, 0.5):
        for lon in frange(-9.0, -6.0, 0.5):
            print(f"Fetching data for coordinates: {lat}, {lon}")
            data = fetch_json_data(DATA_URL, params={
                "config": json.dumps({
                    "coords": [
                        round(lat, 2),
                        round(lon, 2)
                    ]
                })
            })
            
            for poi in data:
                pois[poi['poicode']] = poi

    # Madeira
    for lat in frange(32.5, 33.5, 0.5):
        for lon in frange(-17.5, -16.0, 0.5):
            print(f"Fetching data for coordinates: {lat}, {lon}")
            data = fetch_json_data(DATA_URL, params={
                "config": json.dumps({
                    "coords": [
                        round(lat, 2),
                        round(lon, 2)
                    ]
                })
            })
            
            for poi in data:
                pois[poi['poicode']] = poi

    # Açores
    for lat in frange(36.5, 40.0, 0.5):
        for lon in frange(-31.5, -25.0, 0.5):
            print(f"Fetching data for coordinates: {lat}, {lon}")
            data = fetch_json_data(DATA_URL, params={
                "config": json.dumps({
                    "coords": [
                        round(lat, 2),
                        round(lon, 2)
                    ]
                })
            })

            for poi in data:
                pois[poi['poicode']] = poi

    results = list(pois.values())
    results = [
        r for r in results
        if r["entityCode"] == "Santander_Totta"
        and r["objectType"]["code"] == "BRANCH"
    ]

    return results

if __name__ == "__main__":
    new_data = fetch_data()
    
    old_data = [DiffDict(e) for e in overpass_query(
"""
(
    nwr[amenity=bank][name~"Santander"](area.country);
);
"""
    )]

    new_node_id = -10000
    
    for nd in new_data:
        public_id = nd["poicode"]
        d = next((od for od in old_data if public_id in od[REF].split(";")), None)
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
        if  nd["status"]["code"] not in ("In_Service", "IN_SERVICE"):
            d["x-dld-status"] = nd["status"]["code"]
        d["addr:postcode"] = nd["location"]["zipcode"]
        d["addr:city"] = fix_postcode_city(nd["location"]["city"])
        
        if nd["location"]["urlPhoto"] is not None:
            d["image"] = nd["location"]["urlPhoto"]
        
        if d["contact:phone"]:
            formatted_phone = ";".join([format_phonenumber(phonenumber) for phonenumber in d["contact:phone"].split(";")])
            if formatted_phone != d["contact:phone"]:
                d["contact:phone"] = formatted_phone

        if "contactData" in nd:
            phones = []
            if "phoneNumber" in nd["contactData"]:
                phone_formatted = format_phonenumber(nd["contactData"]["phoneNumber"])
                if phone_formatted not in d["contact:phone"].split(";"):
                    d["contact:phone"] += ";" + phone_formatted
            if nd["contactData"]["customerPhone"] != "":
                phone_formatted = format_phonenumber(nd["contactData"]["customerPhone"])
                if phone_formatted not in d["contact:phone"].split(";"):
                    d["contact:phone"] += ";" + phone_formatted
            if nd["contactData"]["fax"] != "":
                d["contact:fax"] = format_phonenumber(nd["contactData"]["fax"])
            if nd["contactData"]["email"] not in d["contact:email"].split(";"):
                d["contact:email"] = nd["contactData"]["email"]

        if "socialData" in nd:
            d["contact:youtube"] = nd["socialData"].get("youtubeLink", "")
            d["contact:facebook"] = nd["socialData"].get("facebookLink", "")
            d["contact:twitter"] = nd["socialData"].get("twitterLink", "")
            d["contact:linkedin"] = nd["socialData"].get("linkedinLink", "")
            d["contact:instagram"] = nd["socialData"].get("instagramLink", "")
            d["contact:tiktok"] = nd["socialData"].get("tiktokLink", "")
            
        # Schedule
        opening_hours = ""
        schedule = nd["schedule"]
        workingDays = {}
        
        for workday in schedule["workingDay"]:
            wd = WEEKDAY2STR[workday]
            if schedule["workingDay"][workday] == []:
                continue
            workingHours = ",".join(fixWorkingHours(schedule["workingDay"][workday]))
            if workingHours == "Encerrado":
                continue
            if workingHours not in workingDays:
                workingDays[workingHours] = []
            workingDays[workingHours].append(wd)
        for workingHours, days in workingDays.items():
            days.sort(key=lambda s: DAYS.index(s))
            if opening_hours != "":
                opening_hours += "; "
            days = merge_weekdays(days)
            opening_hours += ",".join(days) + " " + workingHours
        days_off = []
        for day in schedule["specialDay"]:
            if day["time"] == [None]:
                month, day = day["date"].split("-")
                monthName = MONTH_NAMES[month]
                days_off.append(f"{monthName} {day}")
            else:
                opening_hours = f"<ERR:{json.dump(schedule)}>"
                days_off = []
                break
        if days_off != []:
            opening_hours += "; " + ",".join(days_off) + " off"
        d["opening_hours"] = opening_hours

        if "urlDetailPage" in nd:
            d["website"] = nd["urlDetailPage"]
            
        if public_id not in d[REF].split(";"):
            d["ref"] = public_id

        services = {}

        # Services
        if "comercialProducts" in nd:
            
            commercialProducts = {product["default"].upper() for product in nd["comercialProducts"]}
            if 'MULTIBANCO' in commercialProducts:
                services["atm"] = "yes"
                commercialProducts.remove('MULTIBANCO')
            if 'LEVANTAMENTOS' in commercialProducts:
                services["cash_out"] = "yes"
                commercialProducts.remove('LEVANTAMENTOS')
            if 'CONSULTAS' in commercialProducts:
                commercialProducts.remove('CONSULTAS')
            if 'TRANSFERÊNCIAS' in commercialProducts:
                commercialProducts.remove('TRANSFERÊNCIAS')
            if 'CARREGAMENTOS' in commercialProducts:
                services["prepaid_top_up:mobile"] = "yes"
                commercialProducts.remove('CARREGAMENTOS')
            if 'PAGAMENTOS DE SERVIÇOS' in commercialProducts:
                commercialProducts.remove('PAGAMENTOS DE SERVIÇOS')
            if 'REQUISIÇÃO DE CHEQUES' in commercialProducts:
                commercialProducts.remove('REQUISIÇÃO DE CHEQUES')
            if 'DEPÓSITOS E CHEQUES' in commercialProducts:
                services["cash_in"] = "yes"
                services["cheque_in"] = "yes"
                commercialProducts.remove('DEPÓSITOS E CHEQUES')
            if 'LEVANTAMENTOS E DEPÓSITOS' in commercialProducts:
                services["cash_out"] = "yes"
                services["cash_in"] = "yes"
                commercialProducts.remove('LEVANTAMENTOS E DEPÓSITOS')
            if 'LEVANTAMENTOS E CHEQUES' in commercialProducts:
                services["cash_out"] = "yes"
                services["cheque_in"] = "yes"
                commercialProducts.remove('LEVANTAMENTOS E CHEQUES')
            if 'SERVIÇO DE BALCÃO' in commercialProducts:
                commercialProducts.remove('SERVIÇO DE BALCÃO')
            if 'MÁQUINA DE DEPÓSITOS' in commercialProducts:
                services["cash_in"] = "yes"
                commercialProducts.remove('MÁQUINA DE DEPÓSITOS')
            if 'DEPÓSITO DE NOTAS NA CONTA DE TERCEIROS' in commercialProducts:
                services["cash_in"] = "yes"
                commercialProducts.remove('DEPÓSITO DE NOTAS NA CONTA DE TERCEIROS')
            if 'NÃO TEM SERVIÇO DE CAIXA' in commercialProducts:
                commercialProducts.remove('NÃO TEM SERVIÇO DE CAIXA')
            if 'APENAS SERVIÇOS COMERCIAIS DISPONÍVEIS' in commercialProducts:
                commercialProducts.remove('APENAS SERVIÇOS COMERCIAIS DISPONÍVEIS')

            if commercialProducts != set():
                d["x-dld-commercial_products"] = ";".join(sorted(commercialProducts))

        if "dialogAttribute" in nd:
            if "WIFI" in nd["dialogAttribute"]:
                services["internet_access"] = "wlan"
            if "RETIRO_CON_CODIGO" in nd["dialogAttribute"]:
                services["cash_out"] = "yes"
            if "MULTICAJERO" in nd["dialogAttribute"]:
                services["atm"] = "yes"
                services["cash_in"] = "yes"

        for service in ALL_SERVICES:
            if service in services:
                d[service] = services[service]
            else:
                d[service] = ""

        if "spokenlanguages" in nd:
            keys = [k for k in list(d.data["tags"]) if k.startswith("language:")]
            
            languages = {lang.lower() for lang in nd["spokenlanguages"]}
            
            for k in keys:
                lang = k.split(":", 1)[1]
                if lang.lower() not in languages:
                    d[k] = ""
                    
            for l in languages:
                if d[f"language:{l}"] != "yes":
                    d[f"language:{l}"] = "yes"

        if d.kind == "new" and not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"] or d["addr:housename"]):
            d["x-dld-addr"] = nd["location"]["address"]

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF].split(";") if d[REF] else None
        if ref and any(nd for nd in new_data if nd["poicode"] in ref):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Santander", REF, old_data)
