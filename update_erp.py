import os, json, time, requests, threading
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from datetime import timedelta
import time

load_dotenv()

API_URL = "https://api.baselinker.com/connector.php"
API_TOKEN = os.getenv("BASELINKER_TOKEN") or os.getenv("API_TOKEN")
INVENTORY_ID = int(os.getenv("NEW_INVENTORY_ID", "0"))
XML_URL = os.getenv("XML_URL", "")
EXTRA_FIELD_ID = 9157  # ERP_ID

# ustaw pod swój limit
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", "500"))
SAFE_RPM = int(REQUESTS_PER_MINUTE * 0.95)  # np 475
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

class RateLimiter:
    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self.lock = threading.Lock()
        self.calls = deque()

    def wait(self):
        now = time.monotonic()
        with self.lock:
            while self.calls and now - self.calls[0] >= 60:
                self.calls.popleft()

            if len(self.calls) >= self.per_minute:
                sleep_for = 60 - (now - self.calls[0])
            else:
                sleep_for = 0

        if sleep_for > 0:
            time.sleep(sleep_for)

        with self.lock:
            self.calls.append(time.monotonic())

limiter = RateLimiter(SAFE_RPM)

thread_local = threading.local()
def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def bl_call(method: str, params: dict):
    limiter.wait()
    headers = {"X-BLToken": API_TOKEN}
    payload = {"method": method, "parameters": json.dumps(params, ensure_ascii=False)}
    s = get_session()
    r = s.post(API_URL, headers=headers, data=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "SUCCESS":
        raise RuntimeError(f"{method} ERROR: {data.get('error_message')} ({data.get('error_code')})")
    return data

def load_sku_to_id_json(path="sku_to_id.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def update_one(sku: str, inv_pid: str, erp_id: str, text_key: str):
    # addInventoryProduct z product_id = update istniejącego
    bl_call("addInventoryProduct", {
        "inventory_id": INVENTORY_ID,
        "product_id": str(inv_pid),
        "text_fields": {
            text_key: str(erp_id)
        }
    })
    return sku

def update_extra_fields_only_listed_parallel(listed_sku_to_id, xml_sku_to_erp):
    total_listed = len(listed_sku_to_id)
    text_key = f"extra_field_{EXTRA_FIELD_ID}"

    # przygotuj tylko to, co realnie wyślesz (bez braków w XML)
    jobs = []
    no_in_xml = 0
    for sku, inv_pid in listed_sku_to_id.items():
        erp_id = xml_sku_to_erp.get(sku)
        if not erp_id:
            no_in_xml += 1
            continue
        jobs.append((sku, inv_pid, erp_id))

    print(f"START: do wysyłki {len(jobs)} / {total_listed} (brak w XML: {no_in_xml})")

    ok = 0
    fail = 0

    start_time = time.time()


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(update_one, sku, inv_pid, erp_id, text_key) for sku, inv_pid, erp_id in jobs]

        for i, fut in enumerate(as_completed(futures), start=1):
            try:
                sku_done = fut.result()
                ok += 1

                elapsed = time.time() - start_time
                rate = ok / elapsed * 60 if elapsed > 0 else 0

                remaining = len(jobs) - ok
                eta_sec = remaining / (rate / 60) if rate > 0 else 0
                eta = str(timedelta(seconds=int(eta_sec)))

                if ok % 10 == 0:
                    msg = f"[{ok}/{len(jobs)}] ✔ SKU: {sku_done} | {int(rate)}/min | ETA: {eta}"
                    print(msg)

                    with open("update_erp.log", "a", encoding="utf-8") as f:
                        f.write(msg + "\n")

            except Exception as e:
                fail += 1
                err = f"❌ ERROR: {e}"
                print(err)

                with open("update_erp.log", "a", encoding="utf-8") as f:
                    f.write(err + "\n")


    print(f"KONIEC ✔  Zapisane: {ok} | Brak w XML: {no_in_xml} | Błędy: {fail}")

if __name__ == "__main__":
    if not API_TOKEN or not INVENTORY_ID or not XML_URL:
        raise SystemExit("Ustaw API_TOKEN, NEW_INVENTORY_ID oraz XML_URL w .env")

    xml_sku_to_erp = fetch_xml_sku_to_erp()
    listed_sku_to_id = load_sku_to_id_json("sku_to_id.json")
    update_extra_fields_only_listed_parallel(listed_sku_to_id, xml_sku_to_erp)
