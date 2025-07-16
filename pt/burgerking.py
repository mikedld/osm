#!/usr/bin/env python3

import itertools
import re

from impl.common import DiffDict, fetch_json_data, overpass_query, distance, titleize, opening_weekdays, write_diff


# DATA_URL = "https://www.burgerking.pt/api/whitelabel"
DATA_URL = "https://euw3-prod-bk.rbictg.com/graphql"
DATA_QUERY = """
    query GetRestaurants($input: RestaurantsInput) {
      restaurants(input: $input) {
        pageInfo {
          hasNextPage
          endCursor
        }
        totalCount
        nodes {
          ...RestaurantNodeFragment
        }
      }
    }

    fragment RestaurantNodeFragment on RestaurantNode {
      _id
      storeId
      isAvailable
      posVendor
      chaseMerchantId
      curbsideHours {
        ...OperatingHoursFragment
      }
      cybersourceTransactingId
      deliveryHours {
        ...OperatingHoursFragment
      }
      deliveryOrderAmountLimit {
        ...DeliveryOrderAmountLimitFragment
      }
      diningRoomHours {
        ...OperatingHoursFragment
      }
      distanceInMiles
      drinkStationType
      driveThruHours {
        ...OperatingHoursFragment
      }
      driveThruLaneType
      email
      environment
      franchiseGroupId
      franchiseGroupName
      frontCounterClosed
      hasBreakfast
      hasBurgersForBreakfast
      hasCatering
      hasCurbside
      hideClickAndCollectOrdering
      hasDelivery
      hasDineIn
      hasDriveThru
      hasTableService
      hasMobileOrdering
      hasLateNightMenu
      hasParking
      hasPlayground
      hasTakeOut
      hasWifi
      hasLoyalty
      id
      isDarkKitchen
      isFavorite
      isHalal
      isRecent
      latitude
      longitude
      mobileOrderingStatus
      name
      number
      parkingType
      paymentMethods {
        ...PaymentMethodsFragment
      }
      phoneNumber
      physicalAddress {
        address1
        address2
        city
        country
        postalCode
        stateProvince
        stateProvinceShort
      }
      playgroundType
      pos {
        vendor
      }
      showStoreLocatorOffersButton
      status
      vatNumber
      customerFacingAddress {
        locale
      }
      waitTime {
        queueLength
        firingTimestamp
      }
    }

    fragment OperatingHoursFragment on OperatingHours {
      friClose
      friOpen
      friAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      monClose
      monOpen
      monAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      satClose
      satOpen
      satAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      sunClose
      sunOpen
      sunAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      thrClose
      thrOpen
      thrAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      tueClose
      tueOpen
      tueAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
      wedClose
      wedOpen
      wedAdditionalTimeSlot {
        ...AdditionalTimeSlotFragment
      }
    }

    fragment AdditionalTimeSlotFragment on AdditionalTimeSlot {
      open
      close
    }

    fragment DeliveryOrderAmountLimitFragment on DeliveryOrderAmountLimit {
      deliveryOrderAmountLimit
      deliveryOrderAmountLimitEnabled
      deliveryOrderRepeatedFailureLimitation
      firstDeliveryOrder
      firstDeliveryOrderEnabled
    }

    fragment PaymentMethodsFragment on PaymentMethods {
      name
      paymentMethodBrand
      state
      isOnlinePayment
    }
"""

REF = "ref"

DAYS = ("mon", "tue", "wed", "thr", "fri", "sat", "sun")
CITIES = {
    "2135-114": "Samora Correia",
    "2660-000": "Santo António dos Cavaleiros",
    "2735-479": "Agualva-Cacém",
    "2735-582": "Agualva-Cacém",
    "2785-501": "São Domingos de Rana",
    "2910-618": "Setúbal",
    "3720-253": "Oliveira de Azeméis",
    "3810-414": "Aveiro",
    "4430-117": "Vila Nova de Gaia",
    "4445-416": "Ermesinde",
    "4460-841": "Senhora da Hora",
    "4465-185": "São Mamede de Infesta",
    "4465-216": "São Mamede de Infesta",
    "4470-274": "Maia",
    "4470-558": "Maia",
    "4485-300": "Labruge",
    "4700-287": "Braga",
    "4710-007": "Braga",
    "4760-012": "Vila Nova de Famalicão",
    "4920-273": "Vila Nova de Cerveira",
    "4935-052": "Viana do Castelo",
    "4990-000": "Ponte de Lima",
    "6000-050": "Castelo Branco",
    "7050-243": "Montemor-o-Novo",
    "9950-302": "Madalena",
}


def fetch_data():
    headers = {
        "x-ui-region": "PT",
    }
    payload = {
        "operationName": "GetRestaurants",
        "query": re.sub(r"^\s+", "", DATA_QUERY, flags=re.M).strip(),
        "variables": {
            "input": {
                "coordinates": {
                    "searchRadius": 10000000,
                    "userLat": 38.306893,
                    "userLng": -17.050891,
                },
                "filter": "NEARBY",
                "first": 1000,
                "parallelFlag": False,
                "status": None,
            },
        }
    }
    result = fetch_json_data(DATA_URL, headers=headers, json=payload)
    result = result["data"]["restaurants"]["nodes"]
    return result


def trim_time(v):
    if v:
        v = v[:5]
        if v in ("23:59", "00:01"):
            v = "00:00"
    return v


def schedule_time(v):
    if not v:
        return ""
    schedule = []
    for idx, day in enumerate(DAYS):
        outer = [trim_time(v[f"{day}Open"]), trim_time(v[f"{day}Close"])]
        if not outer[0]:
            continue
        if inner := v[f"{day}AdditionalTimeSlot"]:
            inner = [trim_time(inner["open"]), trim_time(inner["close"])]
            schedule.append({
                "d": idx,
                "t": f"{outer[0]}-{inner[0]},{inner[1]}-{outer[1]}",
            })
        else:
            schedule.append({
                "d": idx,
                "t": f"{outer[0]}-{outer[1]}",
            })
    schedule = [
        {
            "d": sorted([x["d"] for x in g]),
            "t": k
        }
        for k, g in itertools.groupby(sorted(schedule, key=lambda x: x["t"]), lambda x: x["t"])
    ]
    schedule = [
        f"{opening_weekdays(x['d'])} {x['t']}"
        for x in sorted(schedule, key=lambda x: x["d"][0])
    ]
    return "; ".join(schedule)


if __name__ == "__main__":
    new_data = fetch_data()

    old_data = [DiffDict(e) for e in overpass_query('nwr[amenity][amenity!=charging_station][~"^(name|brand)$"~"Burgu?er[ ]?King"](area.country);')]

    for nd in new_data:
        public_id = nd["storeId"]
        tags_to_reset = set()

        d = next((od for od in old_data if od[REF] == public_id), None)
        if d is None:
            coord = [nd["latitude"], nd["longitude"]]
            ds = [x for x in old_data if not x[REF] and distance([x.lat, x.lon], coord) < 250]
            if len(ds) == 1:
                d = ds[0]
        if d is None:
            d = DiffDict()
            d.data["type"] = "node"
            d.data["id"] = f"-{public_id}"
            d.data["lat"] = nd["latitude"] or 38.306893
            d.data["lon"] = nd["longitude"] or -17.050891
            old_data.append(d)

        d[REF] = public_id
        d["amenity"] = "fast_food"
        d["cuisine"] = "burger"
        d["name"] = "Burger King"
        d["brand"] = "Burger King"
        d["brand:wikidata"] = "Q177054"
        d["brand:wikipedia"] = "pt:Burger King"

        # d["delivery"] = "yes" if nd["hasDelivery"] else "no"
        # d["drive_through"] = "yes" if nd["hasDriveThru"] else "no"
        # d["outdoor_seating"] = "yes" if nd["hasCurbside"] else "no"
        # d["takeaway"] = "yes" if nd["hasTakeOut"] else "no"

        if schedule := schedule_time(nd["diningRoomHours"]):
            d["opening_hours"] = schedule
        # if schedule := schedule_time(nd["deliveryHours"]):
        #     d["opening_hours:delivery"] = schedule
        if schedule := schedule_time(nd["driveThruHours"]):
            d["opening_hours:drive_through"] = schedule
        # if schedule := schedule_time(nd["curbsideHours"]):
        #     d["opening_hours:outdoor_seating"] = schedule
        if d["source:opening_hours"] != "survey":
            d["source:opening_hours"] = "website"

        phone = re.sub(r"\D+", "", nd["phoneNumber"] or "")
        if len(phone) == 14 and phone.startswith("00351"):
            phone = phone[5:]
        elif len(phone) == 12 and phone.startswith("351"):
            phone = phone[3:]
        if len(phone) == 9 and phone not in ("000000000", "220000000", "960000000", "999999999"):
            phone = f"+351 {phone[0:3]} {phone[3:6]} {phone[6:9]}"
            if phone[5:6] == "9":
                d["contact:mobile"] = phone
                tags_to_reset.add("contact:phone")
            else:
                d["contact:phone"] = phone
                tags_to_reset.add("contact:mobile")
        else:
            tags_to_reset.add("contact:mobile")
            tags_to_reset.add("contact:phone")
        d["contact:website"] = f"https://www.burgerking.pt/pt/store-locator/store/{nd['id']}"
        d["contact:facebook"] = "burgerkingportugal"
        d["contact:youtube"] = "https://www.youtube.com/@burgerkingportugal3411"
        d["contact:instagram"] = "burgerkingportugal"
        d["contact:tiktok"] = "burgerkingportugal"

        tags_to_reset.update({"phone", "mobile", "website"})

        if d["source:contact"] != "survey":
            d["source:contact"] = "website"

        address = nd["physicalAddress"]
        city = titleize(address["city"])
        postcode = address["postalCode"].replace(" ", "")
        if len(postcode) == 7 and not "-" in postcode:
            postcode = f"{postcode[:4]}-{postcode[4:]}"
        if postcode in ("0", "111", "9999", "PT"):
            postcode = ""
        if not postcode:
            if m := re.fullmatch(r"(.+?)\s+(\d{4}-\d{3})", city):
                city = m[1]
                postcode = m[2]
        if len(postcode) == 4:
            if len(d["addr:postcode"]) == 8 and postcode == d["addr:postcode"][:4]:
                postcode = d["addr:postcode"]
            else:
                postcode += "-000"
        if len(postcode) != 8:
            postcode = ""
        if postcode:
            d["addr:postcode"] = postcode
        d["addr:city"] = CITIES.get(postcode, city)
        if not d["addr:street"] and not d["addr:place"] and not d["addr:suburb"] and not d["addr:housename"]:
          d["x-dld-addr"] = "; ".join([x.strip() for x in (address["address1"], address["address2"]) if x.strip()])

        for key in tags_to_reset:
            if d[key]:
                d[key] = ""

    for d in old_data:
        if d.kind != "old":
            continue
        ref = d[REF]
        if ref and any(nd for nd in new_data if ref == nd["storeId"]):
            continue
        d.kind = "del"

    old_data.sort(key=lambda d: d[REF])

    write_diff("Burger King", REF, old_data, osm=True)
