#!/usr/bin/env python3

import itertools
import json
import re
from multiprocessing import Pool

from lxml import etree

from impl.common import DiffDict, distance, fetch_html_data, fetch_json_data, opening_weekdays, overpass_query, write_diff


LEVEL1_DATA_URL = "https://www.auchan.pt/pt/lojas"
LEVEL2_DATA_URL = "https://www.auchan.pt/pt/loja"

REF = "ref"

CITY_FIXES = {
    "Alcácer Sal": "Alcácer do Sal",
    "Caldas Rainha": "Caldas da Rainha",
    "Rio Mouro": "Rio de Mouro",
    "S.Brás de Alportel": "São Brás de Alportel",
    "Sta Maria Lamas": "Santa Maria de Lamas",
}
CITY_LOC_FIXES = {
    "1 Maio": "1 de Maio",
    "25 Abril": "25 de Abril",
    "5 Outubro": "5 de Outubro",
    "9 Julho": "9 de Julho",
    "Afonso Albuquerque": "Afonso de Albuquerque",
    "Alameda Oceanos": "Alameda dos Oceanos",
    "Alfredo Silva": "Alfredo da Silva",
    "Alvares Cabral": "Álvares Cabral",
    "Arístides Sousa Mendes": "Aristides de Sousa Mendes",
    "Avenida D. João II": "Avenida Dom João II",
    "Bento Jesus Caraça": "Bento de Jesus Caraça",
    "Bomb Voluntários Algés": "Bombeiros Voluntários de Algés",
    "Calçada Quintinha": "Calçada da Quintinha",
    "Casal Serra": "Casal da Serra",
    "Cidade Horta": "Cidade da Horta",
    "Cidade Viseu": "Cidade de Viseu",
    "Circular Sul": "Circular do Sul",
    "Combatentes Gr. Guerra": "Combatentes da Grande Guerra",
    "Conde Redondo": "Conde de Redondo",
    "Coração Maria": "Coração de Maria",
    "Cristóvão Gama": "Cristóvão da Gama",
    "D Dinis": "Dom Dinis",
    "D Filipa Vilhena": "Dona Filipa de Vilhena",
    "D Marcelino Franco": "Dom Marcelino Franco",
    "D Maria II": "Dona Maria II",
    "D Nuno Álvares Pereira": "Dom Nuno Álvares Pereira",
    "D Pedro V": "Dom Pedro V",
    "D. Manuel II": "Dom Manuel II",
    "D. Nuno Álvares Pereira": "Dom Nuno Álvares Pereira",
    "D.Pedro IV": "Dom Pedro IV",
    "Diogo Silves": "Diogo de Silves",
    "Direita Dafundo": "Direita do Dafundo",
    "Direita Massamá": "Direita de Massamá",
    "Dr António Elvas": "Doutor António Elvas",
    "Dr Aresta Branco": "Doutor Aresta Branco",
    "Dr Dário Gandra Nunes": "Doutor Dário Gandra Nunes",
    "Dr Francisco Sousa Tavares": "Doutor Francisco Sousa Tavares",
    "Dr Henrique de Barros": "Doutor Henrique de Barros",
    "Dr João Santos": "Doutor João Santos",
    "Dr João Silva": "Doutor João Silva",
    "Duque Loulé": "Duque de Loulé",
    "Eng Ferreira Dias": "Engenheiro Ferreira Dias",
    "Est Benfica": "Estrada de Benfica",
    "Est Luz": "Estrada da Luz",
    "Est Marquês Pombal": "Estrada Marquês de Pombal",
    "Est Mem Martins": "Estrada de Mem Martins",
    "Est S Domingos": "Estrada de São Domingos",
    "Fontes Pereira Melo": "Fontes Pereira de Melo",
    "Forno Tijolo": "Forno do Tijolo",
    "Foros Amora": "Foros de Amora",
    "Gabriel Ferreira Castro": "Gabriel Ferreira de Castro",
    "Gen Humberto Delgado": "General Humberto Delgado",
    "Helena Vaz Silva": "Helena Vaz da Silva",
    "Heróis Liberdade": "Heróis da Liberdade",
    "Infante D Augusto": "Infante Dom Augusto",
    "Infante D Henrique": "Infante Dom Henrique",
    "Infante D Pedro": "Infante Dom Pedro",
    "Infante Sagres": "Infante de Sagres",
    "Jaime Mota": "Jaime da Mota",
    "João Barros": "João de Barros",
    "José Conceição Nunes": "José da Conceição Nunes",
    "Luís Camões": "Luís de Camões",
    "Luis Pastor Macedo": "Luis Pastor de Macedo",
    "Luis Queiroz": "Luis de Queiroz",
    "Major Neutel Abreu": "Major Neutel de Abreu",
    "Marquês Pombal": "Marquês de Pombal",
    "Marquês Sá Bandeira": "Marquês Sá da Bandeira",
    "Mouzinho Albuquerque": "Mouzinho de Albuquerque",
    "Oscar Monteiro Torres": "Óscar Monteiro Torres",
    "Padre Manuel Nóbrega": "Padre Manuel da Nóbrega",
    "Pct Bento Gonçalves": "Praceta Bento Gonçalves",
    "Penha França": "Penha de França",
    "Prof Dr Augusto Abreu Lopes": "Professor Doutor Augusto Abreu Lopes",
    "Prof Francisco Gentil": "Professor Francisco Gentil",
    "Qta Campo": "Quinta do Campo",
    "Qta Lomba": "Quinta da Lomba",
    "Rua da Beneficiência": "Rua da Beneficência",
    "Rui Gomes Silva": "Rui Gomes da Silva",
    "S Bento": "São Bento",
    "S Paulo": "São Paulo",
    "S Sebastião": "São Sebastião",
    "Sá Bandeira": "Sá da Bandeira",
    "Saraiva Carvalho": "Saraiva de Carvalho",
    "Terreiro Bispo": "Terreiro do Bispo",
    "Ulisses Alves": "Ulysses Alves",
    "Urb Qta Sto Amaro": "Urbanização Quinta de Santo Amaro",
    "Vilar Andorinho": "Vilar de Andorinho",
    "Visconde Santarém": "Visconde de Santarém",
}
EVENTS_MAPPING = {
    r"Horário feriados: (\d{2}:\d{2}) - (\d{2}:\d{2})": r"PH \1-\2",
    r"Horário feriados: (\d{1}:\d{2}) - (\d{2}:\d{2})": r"PH 0\1-\2",
    r"Horário vésperas de feriado: (\d{2}:\d{2}) - (\d{2}:\d{2})": r"PH -1 days \1-\2",
    r"Encerramento: domingo de Páscoa, 25 de dezembro e 1 de janeiro": r"easter,Dec 25,Jan 01 off",
    r"Encerramento véspera de Ano Novo: (\d{2}:\d{2})": r"Dec 31 {opens-}\1",
    r"Encerramento véspera de Natal: (\d{2}:\d{2})": r"Dec 24 {opens-}\1",
}
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def fetch_level1_data():
    def post_process(page):
        page_tree = etree.fromstring(page, etree.HTMLParser())
        return page_tree.xpath("//@data-locations")[0]

    result = fetch_json_data(LEVEL1_DATA_URL, post_process=post_process)
    result = [x for x in result if x["type"] == "Auchan"]
    return result


def fetch_level2_data(data):
    store_id = re.sub(r'.*data-store-id="([^"]+)".*', r"\1", data["infoWindowHtml"], flags=re.DOTALL)
    params = {
        "StoreID": store_id,
    }
    result_tree = fetch_html_data(LEVEL2_DATA_URL, params=params)
    return {
        "id": store_id,
        "events": [
            x.strip()
            for x in "".join(result_tree.xpath("//div[contains(@class, 'store-events')]//text()")).split("\n")
            if x.strip()
        ],
        **data,
        **json.loads(result_tree.xpath("//script[@type='application/ld+json']/text()")[0]),
    }


def fix_branch(branch):
    branch = re.sub(r"^(Algés|Lisboa)(?:\s+-)?\s+(.+)$", r"\2 - \1", branch)
    branch = re.sub(r"(?<!-)(?<!\s)\s+(Almada|Campolide|Graça|Porto)$", r" - \1", branch)
    if m := re.fullmatch(r"(.+?) - (.+)", branch):
        loc, city = m[1], m[2]
        city = CITY_FIXES.get(city, city)
        if m := re.fullmatch(r"(.+?)\s+(\d+)", loc):
            loc = f"{CITY_LOC_FIXES.get(m[1], m[1])} {m[2]}"
        else:
            loc = CITY_LOC_FIXES.get(loc, loc)
        branch = f"{loc} - {city}"
    else:
        branch = CITY_FIXES.get(branch, branch)
        if m := re.fullmatch(r"(.+?)\s+(\d+)", branch):
            branch = f"{CITY_LOC_FIXES.get(m[1], m[1])} {m[2]}"
        else:
            branch = CITY_LOC_FIXES.get(branch, branch)
    return branch


if __name__ == "__main__":
    new_data = fetch_level1_data()
    with Pool(4) as p:
        new_data = list(p.imap_unordered(fetch_level2_data, new_data))

    old_data = [
        DiffDict(e)
        for e in overpass_query(
            "("
            'nwr[shop][shop!=electronics][shop!=houseware][shop!=pet][~"^(name|brand)$"~"auchan",i](area.country);'
            'nwr[amenity][amenity!=fuel][amenity!=charging_station][amenity!=parking][~"^(name|brand)$"~"auchan",i](area.country);'
            'nwr[shop][~"^(name|brand)$"~"minipreço|mais[ ]?perto",i](area.country);'
            ");"
        )
    ]

    new_node_id = -10000

    for nd in new_data:
        public_id = nd["id"]
        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = nd["latitude"]
            d.data["lon"] = nd["longitude"]
            old_data.append(d)
            new_node_id -= 1

        name = re.sub(r"^(Auchan( Supermercado)?|My Auchan( Saúde e Bem-Estar)?|Auchan).+", r"\1", nd["name"])
        branch = fix_branch(re.sub(r"[ ]{2,}", " ", nd["name"][len(name) :]).strip())
        is_super = name == "Auchan Supermercado"
        is_my = name == "My Auchan"
        is_my_saude = name == "My Auchan Saúde e Bem-Estar"
        tags_to_reset = set()

        d[REF] = public_id
        if is_my_saude:
            d["amenity"] = "pharmacy"
            tags_to_reset.add("shop")
        else:
            d["shop"] = d["shop"] or ("convenience" if is_my else "supermarket")
            tags_to_reset.add("amenity")
        d["name"] = name
        d["branch"] = branch
        d["brand"] = "My Auchan" if is_my or is_my_saude else ("Auchan Supermercado" if is_super else "Auchan")
        d["brand:wikidata"] = "Q115800307" if is_my or is_my_saude else ("Q105857776" if is_super else "Q758603")
        d["brand:wikipedia"] = "pt:Auchan"

        if (old_name := d.old_tags.get("name")) and "Auchan" not in old_name:
            d["old_name"] = old_name

        if d["operator"] not in (None, "Auchan"):
            tags_to_reset.add("operator")

        if schedule := nd["openingHoursSpecification"]:
            events = nd["events"]
            launch_break = ""
            for ea in events:
                if m := re.fullmatch(r"Encerra diariamente das (\d{2})h(\d{2}) às (\d{2})h(\d{2})", ea):
                    launch_break = f"{m[1]}:{m[2]},{m[3]}:{m[4]}-"
                    events.remove(ea)
                    break
            opens = {x["opens"] for x in schedule if x}
            schedule = [
                {
                    "d": DAYS.index(x["dayOfWeek"]),
                    "t": f"{x['opens']}-{launch_break}{x['closes']}",
                }
                for x in schedule
                if x
            ]
            for i in range(len(DAYS)):
                if not any(x for x in schedule if x["d"] == i):
                    schedule.append({"d": i, "t": "off"})
            schedule = [
                {
                    "d": sorted([x["d"] for x in g]),
                    "t": k,
                }
                for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
            ]
            schedule = [f"{opening_weekdays(x['d'])} {x['t']}" for x in sorted(schedule, key=lambda x: x["d"][0])]
            if events:
                events.sort(key=lambda x: -ord(x[0]))
                if len(opens) == 1:
                    opens = next(iter(opens))
                    for ea in events:
                        if "Auchan Saúde e Bem-Estar:" in ea:
                            continue
                        eb = f"<ERR:{ea}>"
                        for ema, emb in EVENTS_MAPPING.items():
                            if re.fullmatch(ema, ea) is not None:
                                eb = re.sub(ema, emb, ea)
                                break
                        schedule.append(eb.replace("{opens-}", f"{opens}-"))
                else:
                    schedule.append(f"<ERR:{opens}>")
            schedule = "; ".join(schedule)
            if d["opening_hours"].replace(" ", "") != schedule.replace(" ", ""):
                d["opening_hours"] = schedule
            if d["source:opening_hours"] != "survey":
                d["source:opening_hours"] = "website"

        phone = nd["telephone"][:16]
        if phone:
            d["contact:phone"] = phone
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = f"https://www.auchan.pt/pt/loja?StoreID={public_id}"
        d["contact:facebook"] = "AuchanPortugal"
        d["contact:youtube"] = "https://www.youtube.com/channel/UC6FSI7tYO9ISV11U2PHBBYQ"
        d["contact:instagram"] = "auchan_pt"
        d["contact:tiktok"] = "auchan_pt"
        d["contact:email"] = "apoiocliente@auchan.pt"

        tags_to_reset.update({"phone", "mobile", "email", "contact:mobile", "contact:website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["address"]

        if not d["addr:city"]:
            d["addr:city"] = address["addressLocality"]
        d["addr:postcode"] = address["postalCode"]

        if (
            d.kind == "new"
            and not d["addr:street"]
            and not (d["addr:housenumber"] or d["nohousenumber"] or d["addr:housename"])
        ):
            d["x-dld-addr"] = address["streetAddress"]

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

    write_diff("Auchan", REF, old_data)
