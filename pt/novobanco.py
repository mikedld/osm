#!/usr/bin/env python3

import json
import itertools
import re
from multiprocessing import Pool
import requests

from lxml import etree

from impl.common import DiffDict, fetch_json_data, fetch_html_data, overpass_query, titleize, opening_weekdays, distance, write_diff, DAYS

DATA_URL = "https://srv.novobanco.pt/web/ocb/pesquisabalcoes/balcoesmap"

REF = "ref"

CITY_FIXES = {
    "Bobadela Lrs": "Bobadela",
    "Alges": "Algés",
    "Santarem": "Santarém",
    "Covilha": "Covilhã",
    "Figueiro dos Vinhos": "Figueiró dos Vinhos",
    "Olhao": "Olhão",
    "Setubal": "Setúbal",
    "Reguengos  Monsaraz": "Reguengos de Monsaraz",
    "Calheta (Madeira)": "Calheta",
    "Loule": "Loulé",
    "Portimao": "Portimão",
    "Santiago Cacem": "Santiago do Cacém",
    "Macao": "Mação",
    "Ilhavo": "Ílhavo",
    "Canico": "Caniço",
    "Fundao": "Fundão",
    "Porto de Mos": "Porto de Mós",
    "Lourinha": "Lourinhã",
    "Alcobaca": "Alcobaça",
    "Guia Pbl": "Guia",
    "Fatima": "Fátima",
    "Azeitao": "Azeitão",
    "Estreito Camara de Lobos": "Estreito de Câmara de Lobos",
    "Sao Mamede de Infesta": "São Mamede de Infesta",
    "Fanzeres": "Fânzeres",
    "Leca da Palmeira": "Leça da Palmeira",
    "Pacos de Ferreira": "Paços de Ferreira",
    "Ourem": "Ourém",
    "Sao Joao da Madeira": "São João da Madeira",
    "Guimaraes": "Guimarães",
    "Trofa Sts": "Trofa",
    "Lordelo Prd": "Lordelo",
    "Povoa de Lanhoso": "Póvoa de Lanhoso",
    "Alijo": "Alijó",
    "Vila Praia de Ancora": "Vila Praia de Âncora",
    "Pardilho": "Pardilhó",
    "Moreira de Conegos": "Moreira de Cónegos",
    "Caldas de Sao Jorge": "Caldas de São Jorge",
    "Agueda": "Águeda",
    "Oliveira de Azemeis": "Oliveira de Azeméis",
    "Canelas Vng": "Canelas",
    "Peso da Regua": "Peso da Régua",
    "Valenca": "Valença",
    "Caldelas Gmr": "Caldelas",
    "Celeiros Brg": "Celeirós",
    "Celeiros": "Celeirós",
    "Vila Real Santo Antonio": "Vila Real de Santo António",
    "Nazare": "Nazaré",
    "Mais": "Maia",
    "Vila Praia de Ancora / Caminha (Extensão)": "Vila Praia de Âncora / Caminha (Extensão)",
    "Melgaco": "Melgaço",
}

CITY_IS_IN = {
    "Campo de Ourique": "Lisboa",
    "Lourel": "Sintra",
    "Casal do Marco": "Seixal",
    "Cotovia": "Sesimbra",
    "Vilamoura": "Quarteira",
}

def fetch_data():
    results = fetch_json_data(DATA_URL, headers={"x-nb-oc-channel": "1.2"})["data"]

    return results

def fix_postal_code(postal_code):
    return postal_code.replace(" ", "")

def fix_city(city):
    if city in CITY_FIXES:
        return CITY_FIXES[city]
    return city

if __name__ == "__main__":
    new_data = fetch_data()
    
    with open("novobanco.json", "w") as f:
        json.dump(new_data, f, indent=2, ensure_ascii=False)
    
    old_data = [DiffDict(e) for e in overpass_query(
"""
(
    nwr[amenity=bank][name~"Novo Banco"][brand!="Novo Banco dos Açores"](area.country);
);
"""
    )]
    
    new_node_id = -10000

    for nd in new_data:
        public_id = nd["codigo"]
        if public_id == "Codigo":
            continue
        d = next((od for od in old_data if public_id in od[REF].split(";")), None)
        if d is None:
            coord = [float(nd["latitude"]), float(nd["longitude"])]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = float(nd["latitude"])
            d.data["lon"] = float(nd["longitude"])
            old_data.append(d)
            new_node_id -= 1

        if nd["designacao"] == "Sede":
            d["amenity"] = "bank"
            d["name"] = "Novo Banco - Sede"
        elif nd["tipoEstrutura"] == "BL" or nd["tipoEstrutura"] == "EXT":
            d["amenity"] = "bank"
            if d["name"] is None:
                d["name"] = "Novo Banco"
        elif nd["tipoEstrutura"] == "CE":
            d["amenity"] = "bank"
            if d["name"] is None:
                d["name"] = "Novo Banco Empresas"
        else:
            raise ValueError(f"Unknown tipoEstrutura: {nd['tipoEstrutura']}")

        d["operator:wikipedia"] = "pt:Novo Banco"
        d["operator:wikidata"] = "Q17488861"
        d["brand"] = "Novo Banco"
        d["brand:wikipedia"] = "pt:Novo Banco"
        d["brand:wikidata"] = "Q17488861"
        
        if nd["codigo"] not in d[REF].split(";"):
            d[REF] = nd["codigo"]
        ref_name = fix_city(nd["designacao"])
        if ref_name not in d["ref_name"].split(";"):
            d["ref_name"] = ref_name
        
        postcode = fix_postal_code(nd["codigoPostal"])
        if re.match(r"^\d{4}-\d{3}$", postcode):
            d["addr:postcode"] = postcode

        city = fix_city(titleize(nd["localidadePostal"]))
        if CITY_IS_IN.get(d["addr:city"], None) != city:
            d["addr:city"] = city
        
        phone = "+351 " + nd["telefone"]
        if phone not in d["contact:phone"].split(";"):
            d["contact:phone"] = "+351 " + nd["telefone"]
        
        fax = "+351 " + nd["fax"]
        if fax not in d["contact:fax"].split(";"):
            d["contact:fax"] = "+351 " + nd["fax"]
                   
        if d.kind == "new" and not d["addr:street"] and not (d["addr:housenumber"] or d["nohousenumber"] or d["addr:housename"]):
            d["x-dld-addr"] = nd["morada"]
        
    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF].split(";") if d[REF] else None
        if ref and any(nd for nd in new_data if nd["codigo"] in ref):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Novo Banco", REF, old_data)