#!/usr/bin/env python3

import json
import itertools
import re
from multiprocessing import Pool
import requests

from lxml import etree

from impl.common import DiffDict, fetch_json_data, fetch_html_data, overpass_query, titleize, opening_weekdays, distance, write_diff, DAYS

DATA_URL = "https://www.gesedu.pt/PesquisaRede/GetEscolasDT"

REF = "ref"

CITY_FIXES = {
    "Aljubarrota (Prazeres)": "Prazeres de Aljubarrota",
    "S Bartolomeu de Messines": "São Bartolomeu de Messines",
    "Aljubarrota (São Vicente)": "São Vicente de Aljubarrota",
    "Aldeia S Francisco Assis": "Aldeia de São Francisco de Assis",
    "Panoias de Cima": "Panóias de Cima",
    "São Felix da Marinha": "São Félix da Marinha",
    "VIL de Souto": "Vil de Souto",
    "Vila Real Santo António": "Vila Real de Santo António",
    "Santo António Cavaleiros": "Santo António dos Cavaleiros",
    "São Romão Coronado": "São Romão do Coronado",
    "São Martinho Campo": "São Martinho do Campo",
    "Costa de Caparica": "Costa da Caparica",
    "a dos Cunhados": "A dos Cunhados",
    "Cruz Quebrada-Dafundo": "Cruz Quebrada - Dafundo",
    "S Bartolomeu dos Galegos": "São Bartolomeu dos Galegos",
    "Entre-Os-Rios": "Entre-os-Rios",
    "São Mamede Coronado": "São Mamede do Coronado",
    "Alvito (São Pedro)": "São Pedro de Alvito",
    "Bastuço (Santo Estevão)": "Santo Estêvão de Bastuço",
    "São Paio Vizela": "São Paio de Vizela",
    "Geraz Lima (Sta Leocádia)": "Santa Leocádia de Geraz do Lima",
    "Geraz Lima (Sta Maria)": "Santa Maria de Geraz do Lima",
    "Távora (Santa Maria)": "Santa Maria de Távora",
    "Entre Ambos-Os-Rios": "Entre Ambos-os-Rios",
    "São Martinho de Antas": "São Martinho de Anta",
    "Figueira Castelo Rodrigo": "Figueira de Castelo Rodrigo",
    "N Senhora Graça do Divor": "Nossa Senhora da Graça do Divor",
    "S Sebastião da Giesteira": "São Sebastião da Giesteira",
    "N Senhora de Guadalupe": "Nossa Senhora de Guadalupe",
    "N Senhora de Machede": "Nossa Senhora de Machede",
    "Azinh Barros S Mam Sádão": "Azinheira dos Barros e São Mamede do Sádão",
    "S Martinho das Amoreiras": "São Martinho das Amoreiras",
    "Sto Aleixo da Restauração": "Santo Aleixo da Restauração",
    "Estombar": "Estômbar",
    "Sta Catarina Fonte Bispo": "Santa Catarina da Fonte do Bispo",
    "Sto António da Serra": "Santo António da Serra",
    "Calheta (Madeira)": "Calheta",
    "N Senhora dos Remédios": "Nossa Senhora dos Remédios",
    "Madalena (Pico)": "Madalena",
    "S Vicente de Pereira Jusã": "São Vicente de Pereira Jusã",
    "Oliveira (São Mateus)": "São Mateus de Oliveira",
    "Santa Maria Arnoso": "Santa Maria de Arnoso",
    "São Tomé Negrelos": "São Tomé de Negrelos",
    "Briteiros (Santo Estêvão)": "Santo Estêvão de Briteiros",
    "São Martinho Sande": "São Martinho de Sande",
    "Estreito Câmara de Lobos": "Estreito de Câmara de Lobos",
}

MUNICIPALITY_ABBREVIATIONS = set([
    "ABF",
    "ABT",
    "ACB",
    "ADV",
    "AGB",
    "AGD",
    "AGH",
    "AJT",
    "ALB",
    "AMR",
    "AMT",
    "ARC",
    "ARL",
    "ASL",
    "BAO",
    "BCL",
    "BGC",
    "BMT",
    "BRG",
    "BRR",
    "CBT",
    "CDR",
    "CDV",
    "CHV",
    "CMN",
    "CNF",
    "CPV",
    "CTB",
    "CTM",
    "CUB",
    "ELV",
    "EPS",
    "ETR",
    "ETZ",
    "FAF",
    "FAG",
    "FAL",
    "FLG",
    "FND",
    "FZZ",
    "GDL",
    "GDM",
    "GMR",
    "GRD",
    "HRT",
    "LGA",
    "LGS",
    "LMG",
    "LNH",
    "LRA",
    "LRS",
    "LSA",
    "LSD",
    "MAD",
    "MCH",
    "MCN",
    "MFR",
    "MGD",
    "MGL",
    "MLG",
    "MNC",
    "MOU",
    "MTJ",
    "MTR",
    "MTS",
    "NLS",
    "OAZ",
    "OBD",
    "OBR",
    "OFR",
    "OHP",
    "OVR",
    "PBL",
    "PCR",
    "PCT",
    "PCV",
    "PDL",
    "PFR",
    "PNF",
    "PRD",
    "PRG",
    "PRL",
    "PRS",
    "PTL",
    "PVL",
    "PVZ",
    "RGR",
    "RMR",
    "RMZ",
    "SBG",
    "SEI",
    "SMP",
    "SNT",
    "SPS",
    "SRN",
    "SRT",
    "STR",
    "STS",
    "SVV",
    "TBU",
    "TCS",
    "TND",
    "TRF",
    "TVR",
    "VCT",
    "VFL",
    "VFR",
    "VGS",
    "VIS",
    "VIZ",
    "VLG",
    "VLN",
    "VNC",
    "VNF",
    "VNG",
    "VNH",
    "VNT",
    "VPA",
    "VPT",
    "VRM",
    "VZL",
    "CLD",
    "CDV",
    "VLC",
    "VCD",
    "TVD",
    "VRL",
    "ALJ",
    "SVC",
])

CITY_IS_IN_MUNICIPALITY = {
    "Vermoim": "Maia",
    "São Pedro Fins": "Maia",
    "Pedrouços": "Maia",
    "Folgosa": "Maia",
    "Águas Santas": "Maia",
    "Milheirós": "Maia",
    "Nogueira da Maia": "Maia",
}

def fetch_data():
    results = fetch_json_data(DATA_URL, params={
        'search[value]': "",
        'search[regex]': "false",
        'filtroesc': '{"Regiao":"","Concelho":"","Distrito":"","NivelEnsino":"","Natureza":"","ApenasUO":"","ApenasEscolas":"","NomeUO":"","NomeEscola":"","linha_inicial":"0","linha_final":"1000000"}',
    })["data"]

    return results

def fix_postal_code(postal_code):
    return postal_code.replace(" ", "")

def fix_city(city):
    for abbr in MUNICIPALITY_ABBREVIATIONS:
        if city.endswith(" " + abbr):
            city = city[:len(city)-len(abbr)-1]
            break
    city = titleize(city).strip()
    if city in CITY_FIXES:
        return CITY_FIXES[city]
    return city

def getOperatorType(grupoNaturezaInst):
    if grupoNaturezaInst == "Publico":
        return "public"
    elif grupoNaturezaInst == "Privado":
        return "private"
    else:
        return f"<ERR:{grupoNaturezaInst}>"

def removeDuplicates(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def to_ranges(nums):
    if not nums:
        return []

    nums.sort()
    ranges = []
    start = end = nums[0]

    for num in nums[1:]:
        if num == end + 1:
            end = num
        else:
            ranges.append([start, end])
            start = end = num
    ranges.append([start, end])

    return ranges

def getGrades(ciclos):
    if ciclos == None:
        return ""
    
    ciclos = ciclos.split(";")

    ret = []

    for ciclo in ciclos:
        if ciclo == "Pré-escolar":
            # append numbers from 3 to 5 to ret
            ret.extend([3, 4, 5])
        elif ciclo == "1º Ciclo":
            ret.extend([6, 7, 8, 9])
        elif ciclo == "2º Ciclo":
            ret.extend([10, 11])
        elif ciclo == "3º Ciclo":
            ret.extend([12, 13, 14])
        elif ciclo == "Secundário":
            ret.extend([15, 16, 17])
        elif ciclo == "Profissional":
            ret.extend([15, 16, 17])
        elif ciclo == "Artistico":
            pass
        elif ciclo == "Especial":
            pass
        elif ciclo == "Extra-escolar":
            pass
        else:
            raise Exception(f"Unknown ciclo: {ciclo}")

    ret = list(set(ret))
    ret = to_ranges(ret)
    ret = [f"{start}-{end}" if start != end else str(start) for start, end in ret]
    return ";".join(ret)

if __name__ == "__main__":
    new_data = fetch_data()
    
    with open("schools.json", "w") as f:
        json.dump(new_data, f, indent=2, ensure_ascii=False)
    
    old_data = [DiffDict(e) for e in overpass_query(
"""
(
    nwr[amenity=school](area.country);
	nwr["disused:amenity"=school](area.country);
	nwr["abandoned:amenity"=school](area.country);
	nwr[amenity=kindergarten](area.country);
	nwr["disused:amenity"=kindergarten](area.country);
	nwr["abandoned:amenity"=kindergarten](area.country);
	nwr[amenity=music_school](area.country);
	nwr[amenity=dancing_school](area.country);
	nwr[amenity=language_school](area.country);
);
"""
    )]
    
    new_node_id = -10000

    for nd in new_data:
        public_id = nd["CODESCME"]
        d = next((od for od in old_data if public_id in od[REF].split(";")), None)
        lat = float((nd["LATITUDE"].strip(",") if nd["LATITUDE"] else "40").replace(",", "."))
        lon = float((nd["LONGITUDE"].strip(",") if nd["LONGITUDE"] else "-8").replace(",", "."))
        if d is None:
            coord = [lat, lon]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 100]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = str(new_node_id)
            d.data["lat"] = lat
            d.data["lon"] = lon
            old_data.append(d)
            new_node_id -= 1

        if nd["TIPOLOGIA"] == "Jardim de Infância":
            d["amenity"] = "kindergarten"
        elif nd["TIPOLOGIA"] == "Escola Artistica":
            if d["amenity"] == "music_school" or d["amenity"] == "dancing_school":
                pass
            else:
                d["amenity"] = "school"
                d["school"] = "art"
        else:
            if not d["amenity"]:
                d["amenity"] = "school"

        if nd["CODUOME"] is not None:
            d["operator:ref"] = nd["CODUOME"]
            
        if nd["CODESCME"] not in d[REF].split(";"):
            d[REF] = nd["CODESCME"]
        
        if not d["name"]:
            d["name"] = nd["NOME"]
        
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-address"] = nd["MORADA"]
            
        d["addr:postcode"] = f"{nd["CP4"]}-{nd["CP3"]}"
        
        city = fix_city(nd["LOCALIDADE"])
        if not d["addr:city"]:
            d["addr:city"] = city
        elif city not in CITY_IS_IN_MUNICIPALITY or CITY_IS_IN_MUNICIPALITY[city] != d["addr:city"]:
            d["addr:city"] = city
        
        if nd["CODQZP"] is not None:
            d["ref:qzp"] = nd["CODQZP"]
        
        operatorType = getOperatorType(nd["GRUPONATUREZAINST"])
        if d["operator:type"] == "charitable" and operatorType == "private":
            pass
        else:
            d["operator:type"] = operatorType
        
        grades = getGrades(nd["CICLO"])
        if grades != "":
            d["grades"] = grades

        if not d["operator"]:
            d["operator"] = nd["NOMEUO"]
        
        email = ";".join([contacto["VALOR_CONTACTO"].strip() for contacto in nd["O_CONTACTOS"] if contacto["TIPO_CONTACTO"] == "EMAIL" and contacto["VALOR_CONTACTO"] is not None])
        if email not in d["contact:email"]:
            d["contact:email"] = email

        d["contact:fax"] = ";".join(["+351 " + contacto["VALOR_CONTACTO"].strip() for contacto in nd["O_CONTACTOS"] if contacto["TIPO_CONTACTO"] == "FAX"])

        phone = ";".join(["+351 " + contacto["VALOR_CONTACTO"].strip() for contacto in nd["O_CONTACTOS"] if contacto["TIPO_CONTACTO"] == "TELEFONE1" or contacto["TIPO_CONTACTO"] == "TELEFONE2"])
        if phone != "" and phone not in d["contact:phone"]:
            d["contact:phone"] = phone

        website = ";".join([contacto["VALOR_CONTACTO"].strip() for contacto in nd["O_CONTACTOS"] if contacto["TIPO_CONTACTO"] == "URL"])
        if website != "":
            d["contact:website"] = website
    
    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF].split(";") if d[REF] else None
        if ref and any(nd for nd in new_data if nd["CODESCME"] in ref):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d["addr:postcode"] or "")

    write_diff("Schools", REF, old_data)