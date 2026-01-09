import os, json, time, requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
load_dotenv()
import json


API_URL = "https://api.baselinker.com/connector.php"
API_TOKEN = os.getenv("BASELINKER_TOKEN") or os.getenv("API_TOKEN")
INVENTORY_ID = int(os.getenv("NEW_INVENTORY_ID", "0"))

XML_URL = os.getenv("XML_URL", "")

EXTRA_FIELD_ID = 9157  # ERP_ID

SLEEP = 0.25  # żeby nie dobijać limitów

def load_sku_to_id_json(path="sku_to_id.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def bl_call(method: str, params: dict):
    headers = {"X-BLToken": API_TOKEN}
    payload = {"method": method, "parameters": json.dumps(params, ensure_ascii=False)}
    r = requests.post(API_URL, headers=headers, data=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "SUCCESS":
        raise RuntimeError(f"{method} ERROR: {data.get('error_message')} ({data.get('error_code')})")
    return data

def fetch_xml_sku_to_erp():
    r = requests.get(XML_URL, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.content)

    ns = {"g": "http://base.google.com/ns/1.0"}

    sku_to_erp = {}
    for item in root.findall(".//item"):
        sku = (item.findtext("g:mpn", default="", namespaces=ns) or "").strip()
        erp_id = (item.findtext("g:id", default="", namespaces=ns) or "").strip()
        if sku and erp_id:
            sku_to_erp[sku] = erp_id
    return sku_to_erp




def update_extra_fields_only_listed(listed_sku_to_id, xml_sku_to_erp):
    total_listed = len(listed_sku_to_id)
    ok = 0
    no_in_xml = 0
    fail = 0

    print(f"START: aktualizacja ERP_ID tylko dla wystawionych: {total_listed} SKU")
    print("-" * 60)

    text_key = f"extra_field_{EXTRA_FIELD_ID}"

    for idx, (sku, inv_pid) in enumerate(listed_sku_to_id.items(), start=1):
        # ✅ status co 1000 przebiegów (żebyś widział że żyje)
        if idx % 1000 == 0:
            print(f"[{idx}/{total_listed}] status… OK={ok} | brak_w_XML={no_in_xml} | błędy={fail}")

        erp_id = xml_sku_to_erp.get(sku)
        if not erp_id:
            no_in_xml += 1
            continue


        try:
            bl_call("addInventoryProduct", {
                "inventory_id": INVENTORY_ID,
                "product_id": str(inv_pid),
                "text_fields": {
                    text_key: str(erp_id)
                }
            })
            ok += 1

            # print co 50 zapisów
            if ok % 10 == 0:
                print(f"[{idx}/{total_listed}] OK={ok} | brak_w_XML={no_in_xml} | SKU={sku} | ERP_ID={erp_id}")

        except Exception as e:
            fail += 1
            print(f"ERR: SKU={sku} product_id={inv_pid} → {e}")

        time.sleep(SLEEP)

    print("-" * 60)
    print(f"KONIEC ✔  Zapisane: {ok} | Brak w XML: {no_in_xml} | Błędy: {fail}")


if __name__ == "__main__":
    if not API_TOKEN or not INVENTORY_ID or not XML_URL:
        raise SystemExit("Ustaw API_TOKEN, NEW_INVENTORY_ID oraz XML_URL w .env")

    # 1) XML -> mapa sku->erp
    xml_sku_to_erp = fetch_xml_sku_to_erp()
    print(f"Z XML: {len(xml_sku_to_erp)} SKU z ERP_ID")

    # 2) Wystawione -> sku->inventory_product_id
    listed_sku_to_id = load_sku_to_id_json("sku_to_id.json")
    print(f"sku_to_id.json: {len(listed_sku_to_id)} wystawionych SKU")

    # 3) Update tylko wystawionych
    update_extra_fields_only_listed(listed_sku_to_id, xml_sku_to_erp)
