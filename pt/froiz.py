#!/usr/bin/env python3

from impl.common import DiffDict, distance, fetch_html_data, overpass_query, write_diff


DATA_URL = "https://www.froiz.pt/localizador-de-lojas/"

REF = "ref"

POSTCODES = {
    "515": "4415-307",
}
CITIES = {
    "4700-213": "Braga",
    "4430-518": "Vila Nova de Gaia",
    "4750-191": "Barcelos",
    "4415-307": "Pedroso",
    "4755-522": "Várzea",
    "4250-163": "Porto",
    "4200-000": "Porto",
}


def fetch_data():
    result_tree = fetch_html_data(DATA_URL)
    result = [
        {k[5:]: v.strip() for k, v in el.attrib.items() if k.startswith("data-")}
        for el in result_tree.xpath("//div[@class='froiz-shop-list-row']")
    ]
    return result


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[shop][~"^(name|brand)$"~"froiz",i](area.country);')]

    old_node_ids = {d.data["id"] for d in old_data}

    for nd in new_data:
        public_id = nd["id"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        coord = [float(nd["lat"]), float(nd["long"])]
        if d is None:
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = public_id
            d.data["lat"], d.data["lon"] = coord
            old_data.append(d)
        else:
            old_node_ids.remove(d.data["id"])

        d[REF] = public_id
        d["shop"] = "supermarket"
        d["name"] = "Froiz"
        d["brand"] = "Froiz"
        d["brand:wikidata"] = "Q17070775"
        d["brand:wikipedia"] = "pt:Froiz"

        schedule = f"Mo-Sa {nd['apertura-1']}-{nd['cierre-1']}"
        if nd["apertura-2"] != "00:00":
            schedule += f"; <ERR:{nd['apertura-2']}-{nd['cierre-2']}>"
        d["opening_hours"] = schedule
        d["source:opening_hours"] = "website"

        phone = nd["phone"]
        if len(phone) == 9:
            d["contact:phone"] = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
        else:
            tags_to_reset.add("contact:phone")
        d["website"] = "https://www.froiz.pt/"
        d["contact:facebook"] = "SupermercadosFroizPortugal"
        d["contact:youtube"] = "http://www.youtube.com/@supermercadosfroizportugal4467"
        d["contact:instagram"] = "supermercados_froiz_portugal"
        d["contact:linkedin"] = "https://www.linkedin.com/company/supermercados-froiz-portugal/"
        d["contact:tiktok"] = "froiz.portugal.oficial"

        tags_to_reset.update({"phone", "mobile", "contact:mobile", "contact:website"})

        d["source:contact"] = "website"

        address = nd["address"].split(" - ")
        address.pop()
        if postcode := address.pop():
            if len(postcode) == 4:
                postcode += "-000"
            if len(postcode) == 8:  # and not d["addr:postcode"]:
                d["addr:postcode"] = POSTCODES.get(public_id, postcode)
        city = address.pop().strip()
        d["addr:city"] = CITIES.get(d["addr:postcode"], city)
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
            d["x-dld-addr"] = "; ".join(address)

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.data["id"] in old_node_ids:
            d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Froiz", REF, old_data, osm=True)
