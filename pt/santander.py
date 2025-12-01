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
    "Balazar Pvz": "Balazar",
    "Barcelinhos Bcl": "Barcelinhos",
    "Calheta (Madeira)": "Calheta",
    "Canelas Vng": "Canelas",
    "Celeirós Brg": "Celeirós",
    "Cernache Bonjardim": "Cernache de Bonjardim",
    "Charneca Caparica": "Charneca da Caparica",
    "Estreito Câmara de Lobos": "Estreito de Câmara de Lobos",
    "Gandra Prd": "Gandra",
    "Macedo Cavaleiros": "Macedo de Cavaleiros",
    "Oliveira Azeméis": "Oliveira de Azeméis",
    "Oliveira Hospital": "Oliveira do Hospital",
    "Paços Ferreira": "Paços de Ferreira",
    "Pampilhosa Serra": "Pampilhosa da Serra",
    "Paredes Pnf": "Paredes",
    "Ponta Delgada - a. Quental": "Ponta Delgada - A. Quental",
    "Portela Lrs": "Portela",
    "São Brás Alportel": "São Brás de Alportel",
    "Stª Cruz Graciosa": "Santa Cruz da Graciosa",
    "Sta Maria da Feira": "Santa Maria da Feira",
    "Termas S. Vicente": "Termas de São Vicente",
    "V. N. Famalicão": "Vila Nova de Famalicão",
    "V. N. Gaia": "Vila Nova de Gaia",
    "V. Real S. António": "Vila Real de Santo António",
    "V.N. Famalicão": "Vila Nova de Famalicão",
    "Vila Franca Xira": "Vila Franca de Xira",
    "Vila Pouca Aguiar": "Vila Pouca de Aguiar",
}
CITY_LOC_FIXES = {
    "5 Outubro": "5 de Outubro",
    "a. Quental": "Antero de Quental",
    "Areias São João": "Areias de São João",
    "Av. do Infante": "Avenida do Infante",
    "Av. Roma": "Avenida de Roma",
    "Avenida Londres": "Avenida de Londres",
    "Benfica Calhariz": "Benfica - Calhariz",
    "Benfica Igreja": "Benfica - Igreja",
    "Columbano B. Pinheiro": "Columbano Bordalo Pinheiro",
    "Fernão Magalhães": "Fernão de Magalhães",
    "Fontes P. Melo": "Fontes Pereira de Melo",
    "Heróis Angola": "Heróis de Angola",
    "Largo Chafariz": "Largo do Chafariz",
    "M. Albuquerque": "Mouzinho de Albuquerque",
    "Parque Nações": "Parque das Nações",
    "Pç. D. Maria II": "Praça Dona Maria II",
    "R. Júlio Dinis": "Rua de Júlio Dinis",
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


def fix_branch(name):
    branches = []
    for branch in re.split(r"\s+/\s+", titleize(name)):
        full_branch = []
        if m := re.fullmatch(r"((?:Grandes )?Empresas|Work Café)(?:\s*-)?\s*(.+)", branch):
            full_branch.append(m[1])
            branch = m[2]
        branch = re.sub(r"^(Cascais|Évora|Lisboa|Portela|Porto(?=\s+\d))(\s*-)?\s*", r"\1 - ", branch)
        if m := re.fullmatch(r"(.+?) - (.+)", branch):
            city, loc = m[1], m[2]
            city = CITY_FIXES.get(city, city)
            loc = CITY_LOC_FIXES.get(loc, loc)
            branch = f"{city} - {loc}"
        else:
            branch = CITY_FIXES.get(branch, branch)
        full_branch.append(branch)
        branches.append(" - ".join(full_branch))
    return ";".join(branches)


def fix_city(city):
    city = titleize(city)
    if city in CITY_FIXES:
        return CITY_FIXES[city]
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

    old_data = [DiffDict(e) for e in overpass_query('nwr[amenity=bank][name~"santander",i](area.country);')]

    new_node_id = -10000
    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["poicode"]
        branch = fix_branch(nd["name"])
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
        else:
            old_node_ids.remove(d.data["id"])

        d["ref"] = public_id
        d["amenity"] = "bank"

        if nd["subType"]["code"] == "WORKCAFE":
            d["name"] = "Santander Work Café"
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
        if d["branch"] not in branch.split(";"):
            d["branch"] = branch
        if nd["status"]["code"].lower() != "in_service":
            d["x-dld-status"] = nd["status"]["code"]
        d["addr:postcode"] = nd["location"]["zipcode"]
        d["addr:city"] = fix_city(nd["location"]["city"])

        tags_to_reset.add("ref_name")

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

        d["website"] = nd.get("urlDetailPage", "") or "https://www.santander.pt/"
        d["source:contact"] = "website"

        tags_to_reset.update({"phone", "mobile", "fax", "email", "contact:mobile", "contact:website"})

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
        d["source:opening_hours"] = "website"

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
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Santander", REF, old_data)
