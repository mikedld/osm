#!/usr/bin/env python3

import html
import re
import unicodedata

from impl.common import DiffDict, distance, fetch_json_data, overpass_query, titleize, write_diff


DATA_URL = "https://www.meusuper.pt/lojas/"

REF = "ref"

SCHEDULE_DAYS_MAPPING = {
    r"(de )?segunda a sexta( feira)?": "Mo-Fr",
    r"segunda a sexta e feriados": "Mo-Fr,PH",
    r"(de )?(2[ºª]( feira)?|seg(unda)?) a s[áa]b(ados?)?": "Mo-Sa",
    r"(de )?segunda( feira)? a sábados? (e feriados|\(in(cl|lc)u[ií]n?do feriados\))": "Mo-Sa,PH",
    r"(de )?(2[ºa]|segunda)( feira)?( a |\s*-\s*)domingo|todos os dias": "Mo-Su",
    r"(de )?segunda a domingos? (e feriados|\(inclu[ií]ndo feriados\))": "Mo-Su,PH",
    r"sábados?": "Sa",
    r"sábados e feriados": "Sa,PH",
    r"sábados? e domingos?": "Sa,Su",
    r"sábados, domingos e feriados": "Sa,Su,PH",
    r"domingos?( é)?": "Su",
    r"domingos? e feriados?": "Su,PH",
    r"feriados": "PH",
}
SCHEDULE_DAYS_OFF_MAPPING = {
    r"domingos? (encerrados?|fechados)": "Su",
    r"domingos e feriados encerrado": "Su,PH",
    r"em agosto não abre ao domingo": "Aug Su",
    r"encerra(do)?( aos?)? domingos?": "Su",
    r"encerra aos domingos e feriados": "Su,PH",
    r"encerra aos feriados": "PH",
    r"encerra(do)? domingos? e feriados": "Su,PH",
}
SCHEDULE_HOURS_MAPPING = {
    r"(?:das )?(\d{1})h [àáa]s (\d{2})h?(?: e)?": r"0\1:00-\2:00",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})h": r"0\1:00-\2:00,\3:00-\4:00",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})": r"0\1:00-\2:00,\3:00-\4:\5",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})h e das (\d{2})[:.h](\d{2}) [àáa]s (\d{2})h": r"0\1:00-\2:00,\3:\4-\5:00",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})[:.h](\d{2})": r"0\1:00-\2:\3",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})h [àáa]s (\d{2})h": r"0\1:00-\2:\3,\4:00-\5:00",
    r"(?:das )?(\d{1})h [àáa]s (\d{2})[:.h](\d{2}) e(?: das)? (\d{2})[:.h](\d{2}) [àáa]s (\d{2})h?": (
        r"0\1:00-\2:\3,\4:\5-\6:00"
    ),
    r"(?:das )?(\d{1})h ao (\d{2})[:.h](\d{2})": r"0\1:00-\2:\3",
    r"(?:das )?(\d{1})[:.h](\d{2}) - (\d{2})[:.h](\d{2})": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{1})[:.h](\d{2}) - (\d{2})[:.h](\d{2}) e das (\d{2})[:.h](\d{2}) - (\d{2})[:.h](\d{2})": (
        r"0\1:\2-\3:\4,\5:\6-\7:\8"
    ),
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})h": r"0\1:\2-\3:00",
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})h": r"0\1:\2-\3:00,\4:00-\5:00",
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})": r"0\1:\2-\3:00,\4:00-\5:\6",
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2})": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2}) [àáa]s (\d{2})[:.h](\d{2})h": (
        r"0\1:\2-\3:\4,\5:00-\6:\7"
    ),
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2})": (
        r"0\1:\2-\3:\4,\5:\6-\7:\8"
    ),
    r"(?:das )?(\d{1})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2})h": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{1})h-(\d{2})h": r"0\1:00-\2:00",
    r"(?:das )?(\d{1})[:.h](\d{2})h [àáa]s (\d{2})h, e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})h": r"0\1:\2-\3:00,\4:00-\5:\6",
    r"(?:das )?(\d{1})[:.h](\d{2})h [àáa]s (\d{2})[:.h](\d{2})h": r"0\1:\2-\3:\4",
    r"(?:das )?(\d{1})h-(\d{2})[:.h](\d{2}) e (\d{2})h-(\d{2})h": r"0\1:00-\2:\3,\4:00-\5:00",
    ##
    r"(?:das )?(\d{2})h? [àáa]s (\d{2})h": r"\1:00-\2:00",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})h e (\d{2})h [àáa]s (\d{2})h": r"\1:00-\2:00,\3:00-\4:00",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})h?": r"\1:00-\2:00,\3:00-\4:00",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})": r"\1:00-\2:00,\3:00-\4:\5",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})h e das (\d{2})[:.h](\d{2}) [àáa]s (\d{2})h": r"\1:00-\2:00,\3:\4-\5:00",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})[:.h](\d{2})h?": r"\1:00-\2:\3",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})h [àáa]s (\d{2})h": r"\1:00-\2:\3,\4:00-\5:00",
    r"(?:das )?(\d{2})h [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})[:.h](\d{2}) [àáa]s (\d{2})h": r"\1:00-\2:\3,\4:\5-\6:00",
    r"(?:das )?(\d{2})h e das (\d{2})h e das (\d{2})h [àáa]s (\d{2})h": r"\1:00-\2:00,\3:00-\4:00",
    r"(?:das )?(\d{2})h e das (\d{2})h e das (\d{2})h e das (\d{2})[:.h](\d{2})": r"\1:00-\2:00,\3:00-\4:\5",
    r"(?:das )?(\d{2})[:.h](\d{2}) - [àáa]s (\d{2})[:.h](\d{2})": r"\1:\2-\3:\4",
    r"(?:das )?(\d{2})[:.h](\d{2})h? [àáa]s (\d{2})h": r"\1:\2-\3:00",
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})h(?: e)? das (\d{2})h [àáa]s (\d{2})h": r"\1:\2-\3:00,\4:00-\5:00",
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})h e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})": r"\1:\2-\3:00,\4:00-\5:\6",
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]ss? (\d{2})[:.h](\d{2})": r"\1:\2-\3:\4",
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) (\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2})": (
        r"\1:\2-\3:\4,\5:\6-\7:\8"
    ),
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})h [àáa]s (\d{2})h": r"\1:\2-\3:\4,\5:00-\6:00",
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})h [àáa]s (\d{2})[:.h](\d{2})": (
        r"\1:\2-\3:\4,\5:00-\6:\7"
    ),
    r"(?:das )?(\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2}) e das (\d{2})[:.h](\d{2}) [àáa]s (\d{2})[:.h](\d{2})": (
        r"\1:\2-\3:\4,\5:\6-\7:\8"
    ),
    r"(?:das )?(\d{2})h-(\d{2})h": r"\1:00-\2:00",
    r"(?:das )?o(\d{1})h [àáa]s (\d{2})h": r"0\1:00-\2:00",
}
SCHEDULE_SEASONS_MAPPING = {
    r"inverno": "Sep 22-Jun 20",
    r"inverno \((\d{2})/09 a (\d{2})/06\)": r"Sep \1-Jun \2",
    r"inverno \(novemnro a maio\)": "Nov-May",
    r"inverno \(outubro a março\)": "Oct-Mar",
    r"verão": "Jun 21-Sep 21",
    r"verão \((\d{2})/06 a (\d{2})/09\)": r"Jun \1-Sep \2",
    r"verão \(abril a setembro\)": "Apr-Sep",
    r"verão \(de (\d{2})\.06 a (\d{2})\.09\)": r"Jun \1-Sep \2",
    r"verão \(junho a outubro\)": "Jun-Oct",
}
BRANCHES = {
    "Bom Sucesso (Fnc)": "Bom Sucesso (Funchal)",
    "Padre António Vieira (Cbr)": "Padre António Vieira (Coimbra)",
    "Pe. Francisco Álvares (Lisboa)": "Padre Francisco Álvares (Lisboa)",
    "Porto Côvo": "Porto Covo",
    "S. Martinho Bispo": "São Martinho do Bispo",
    "Serra Del Rey": "Serra del Rey",
}
CITIES = {
    "2415-409": "Leiria",
    "2525-801": "Serra d'El Rei",
    "2970-593": "Sesimbra",
    "3030-853": "Ceira",
    "3250-108": "Alvaiázere",
    "3440-018": "Óvoa",
    "3510-811": "Torredeita",
    "3830-748": "Gafanha da Nazaré",
    "3840-271": "Gafanha da Boa Hora",
    "4620-848": "Meinedo",
    "4705-719": "Figueiredo",
    "4710-093": "Braga",
    "4710-820": "Adaúfe",
    "7520-437": "Porto Covo",
    "8670-156": "Aljezur",
    "9100-074": "Gaula",
    "9360-324": "Canhas",
}


def fetch_data():
    def post_process(page):
        page = re.sub(r".*window\.loja_to_openRaw\s*=\s*'([^']+)'.*$", r"\1", page, flags=re.DOTALL)
        return page

    return fetch_json_data(DATA_URL, post_process=post_process)


def schedule_time(v, mapping):
    for sma, smb in mapping.items():
        if re.fullmatch(sma, v) is not None:
            return True, re.sub(sma, smb, v)
    return False, f"<ERR:{v}>"


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][name~"meu[ ]*super",i](area.country);')]

    for nd in new_data:
        public_id = str(nd["id"])
        branch = titleize(re.sub(r"^(Meu Super|MS)\s+", "", html.unescape(nd["name"]).replace("–", "-")))
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["latitude"]), float(nd["longitude"])]
        if coord[1] > 0:
            coord[1] = -coord[1]
        if coord[1] < -180 and "." not in nd["longitude"]:
            lng = str(int(coord[1]))
            if re.match(r"-[6-9]", lng):
                coord[1] = float(f"{lng[:2]}.{lng[2:]}")
            elif re.match(r"-[1-3]", lng):
                coord[1] = float(f"{lng[:3]}.{lng[3:]}")
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
        d["shop"] = d["shop"] or "supermarket"
        d["name"] = "Meu Super"
        d["brand"] = "Meu Super"
        d["brand:wikidata"] = "Q119948154"
        d["branch"] = BRANCHES.get(branch, branch)

        tags_to_reset.update({"not:brand:wikidata", "wikidata"})
        for name_tag in ("name:en", "name:pt"):
            if d[name_tag].lower() == d["name"].lower():
                tags_to_reset.add(name_tag)

        schedule = re.sub(r"\s+", " ", nd["horarios"].lower().replace("–", "-"))
        schedule = re.sub(
            r"(?<!domingo)(?<!domingos)(?<!feriados)[;:,.(\s]+(?=encerra)|(?<=[h\d]),?\s+e(?=\s+domingo)|/(?=\s*(?=\b\D))",
            ";",
            schedule,
        )
        schedule = re.sub(r";(?:\s*\bdas\b)", ",", schedule)
        schedule = re.sub(
            r"((?<!sábados)[.,]|(?<=\d\dh)|(?<=\d\dh e)|(?<=\d\d[h:]\d\d))(?=\s+(?:sábado|domingo|feriado|inverno))",
            ";",
            schedule,
        )
        schedule = re.sub(r"(?<=fechados)\s+(?=feriados das)", "; ", schedule)
        schedule = re.sub(r"((?:inverno|verão)[^:]*):", r"\1;", schedule)
        schedule = [x.strip(";. ") for x in re.split(r";", schedule) if x.strip(";. ")]
        active_season = None
        delta = 0
        for i, s in enumerate(list(schedule)):
            p = re.split(
                r"\s+(?=\bdas\b)|(?:(?<=\))|(?<=sexta)|(?<=sab)|(?<=s[áa]bado)|(?<=domingo)|(?<=domingos)|(?<=feriado)|(?<=feriados))\s*[:,]?\s*(?=\d)",
                s,
                maxsplit=1,
            )
            if len(p) == 1:
                p = re.split(r"(?<=\d\dh)\s+(?=segunda)", p[0])
                if len(p) == 2:
                    p[0], p[1] = p[1], p[0]
                elif re.search(r"\dh", p[0]):
                    p = ["todos os dias", p[0]]
            if len(p) == 2 and (m := re.fullmatch(r"(.+?)\s*(\(.+)", p[1])):
                p[0], p[1] = f"{p[0]} {m[2]}", m[1]
            if len(p) == 2:
                p = [
                    schedule_time(p[0].strip("-,. "), SCHEDULE_DAYS_MAPPING)[1],
                    schedule_time(unicodedata.normalize("NFC", p[1]), SCHEDULE_HOURS_MAPPING)[1],
                ]
            elif (doff := schedule_time(p[0].strip("() "), SCHEDULE_DAYS_OFF_MAPPING)) and doff[0]:
                p = [doff[1], "off"]
            elif (season := schedule_time(p[0], SCHEDULE_SEASONS_MAPPING)) and season[0]:
                active_season = season[1]
                schedule.pop(i)
                delta += 1
                continue
            if active_season:
                p[0] = f"{active_season} {p[0]}"
            schedule[i - delta] = p
        if schedule:
            schedule = [" ".join(x) for x in schedule]
            d["opening_hours"] = "; ".join(schedule)
            d["source:opening_hours"] = "website"

        phones = [re.sub(r"\s+", "", x) for x in re.split(r"\s+ou\s+|/", nd["telefone"])] if nd["telefone"] else []
        if not phones and (old_phones := d["contact:phone"] or d["phone"]):
            phones = [re.sub(r"\s+", "", x).removeprefix("+351") for x in re.split(r"\s*[;,]\s*", old_phones)]
        phones = [f"+351 {x[0:3]} {x[3:6]} {x[6:9]}" for x in phones if len(x) == 9]
        if phones:
            d["contact:phone"] = ";".join(phones)
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://www.meusuper.pt/"
        if "meusuperoficial" not in d["contact:facebook"].split(";"):
            d["contact:facebook"] = f"{d['contact:facebook']};meusuperoficial".strip(";")
        d["contact:instagram"] = "meusuperoficial"
        d["contact:linkedin"] = "https://www.linkedin.com/company/meusuperoficial"

        tags_to_reset.update({"phone", "mobile", "fax", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        address = re.sub(r"^[\s;]+|[\s;]+$", "", nd["morada"].replace("<br />", ";"))
        if m := re.fullmatch(r"(.+?)\b[\s,]+(\d{4})\s*[-–]\s*(\d{3})\s*,?\s+(.+)", address, flags=re.DOTALL):
            d["addr:postcode"] = f"{m[2]}-{m[3]}"
            d["addr:city"] = CITIES.get(d["addr:postcode"], titleize(m[4]))
            address = m[1]
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = address

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == str(nd["id"])):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Meu Super", REF, old_data)
