import xml.etree.ElementTree as ET
import requests
import time
import json
import logging
import os
from dotenv import load_dotenv
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from urllib.parse import urlparse
from typing import Optional, Tuple
import threading
from collections import deque
from concurrent.futures import as_completed


class RateLimiter:
    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self.lock = threading.Lock()
        self.calls = deque()

    def wait(self):
        now = time.monotonic()
        with self.lock:
            # usuń stare wpisy >60s
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

load_dotenv()

# Konfiguracja
API_TOKEN = os.environ.get('API_TOKEN')  # Wstaw swój token API BaseLinker jako zmienną środowiskową
API_URL = os.environ.get('API_URL')
INVENTORY_ID = os.environ.get('INVENTORY_ID')  # Magazyn BaseLinker (DurczokAPI)
NEW_INVENTORY_ID = os.environ.get('NEW_INVENTORY_ID')  # Wstaw poprawny ID nowego katalogu
PRICE_GROUP_ID = os.environ.get('PRICE_GROUP_ID')  # ID grupy cenowej CZK (API DurczokCZK)
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 80))  # Limit dla dodawania produktów
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 5))  # Liczba równoległych wątków
BATCH_SIZE = REQUESTS_PER_MINUTE  # Partia produktów na minutę
BATCH_INTERVAL = 60  # Odstęp między partiami (60 sekund)
DEFAULT_TAX = 21  # Domyślny VAT (23%)
SKU_TO_ID_FILE = "sku_to_id.json"  # Plik do przechowywania mapowania SKU -> product_id
XML_URL = os.environ.get('XML_URL')  # URL do pliku XML
PAUSE_DURATION = 360  # 6 minut w sekundach

# Konfiguracja logowania
logging.basicConfig(
    filename="add_products.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Globalna zmienna do przechowywania bazy SKU-to-ID w pamięci
sku_to_id_cache = {}

SAFE_RPM = int(REQUESTS_PER_MINUTE * 0.95)  # np. 475 przy 500
limiter = RateLimiter(SAFE_RPM)


thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        thread_local.session = s
    return thread_local.session


def load_sku_to_id() -> Dict[str, str]:
    """Ładuje mapowanie SKU -> product_id z pliku JSON."""
    global sku_to_id_cache
    if os.path.exists(SKU_TO_ID_FILE):
        try:
            with open(SKU_TO_ID_FILE, "r", encoding="utf-8") as f:
                sku_to_id_cache = json.load(f)
            logging.info(f"Załadowano bazę SKU-to-ID z pliku: {len(sku_to_id_cache)} rekordów.")
            print(f"Załadowano bazę SKU-to-ID z pliku: {len(sku_to_id_cache)} rekordów.")
        except Exception as e:
            logging.error(f"Błąd podczas ładowania bazy SKU-to-ID: {str(e)}")
            print(f"Błąd podczas ładowania bazy SKU-to-ID: {str(e)}")
            sku_to_id_cache = {}
    return sku_to_id_cache


def save_sku_to_id():
    try:
        tmp_path = SKU_TO_ID_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(sku_to_id_cache, f, ensure_ascii=False)  # <-- bez indent
        os.replace(tmp_path, SKU_TO_ID_FILE)
        logging.info(f"Zapisano bazę SKU-to-ID do pliku: {len(sku_to_id_cache)} rekordów.")
        print(f"Zapisano bazę SKU-to-ID do pliku: {len(sku_to_id_cache)} rekordów.")
    except Exception as e:
        logging.error(f"Błąd podczas zapisywania bazy SKU-to-ID: {str(e)}")
        print(f"Błąd podczas zapisywania bazy SKU-to-ID: {str(e)}")



def get_valid_storage_id() -> str:
    """Pobiera listę magazynów i sprawdza poprawność INVENTORY_ID."""
    headers = {"X-BLToken": API_TOKEN}
    params = {
        "method": "getStoragesList",
        "parameters": json.dumps({})
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        if response_data.get("status") != "SUCCESS":
            logging.error(f"Błąd pobierania listy magazynów: {response_data.get('error_message', 'Brak szczegółów błędu')}")
            print(f"Błąd pobierania listy magazynów: {response_data.get('error_message', 'Brak szczegółów błędu')}")
            return None
        
        storages = response_data.get("storages", [])
        for storage in storages:
            if storage.get("storage_id") == INVENTORY_ID:
                print(f"Znaleziono poprawny magazyn: {storage.get('name')} (ID: {INVENTORY_ID})")
                logging.info(f"Znaleziono poprawny magazyn: {storage.get('name')} (ID: {INVENTORY_ID})")
                return INVENTORY_ID
        
        logging.error(f"Nie znaleziono magazynu o ID {INVENTORY_ID}. Dostępne magazyny: {storages}")
        print(f"Nie znaleziono magazynu o ID {INVENTORY_ID}. Dostępne magazyny: {storages}")
        return None
    except Exception as e:
        logging.error(f"Błąd podczas pobierania listy magazynów: {str(e)}")
        print(f"Błąd podczas pobierania listy magazynów: {str(e)}")
        return None

def create_category_if_needed(inventory_id: str) -> str:
    """Tworzy kategorię, jeśli nie istnieje, i zwraca jej ID."""
    headers = {"X-BLToken": API_TOKEN}
    params = {
        "method": "getProductCatalogCategories",
        "parameters": json.dumps({"storage_id": INVENTORY_ID, "inventory_id": inventory_id})
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        if response_data.get("status") == "SUCCESS" and response_data.get("categories"):
            category_id = str(response_data["categories"][0]["category_id"])
            print(f"Znaleziono istniejącą kategorię: ID {category_id}")
            logging.info(f"Znaleziono istniejącą kategorię: ID {category_id}")
            return category_id
        
        params = {
            "method": "addProductCatalogCategory",
            "parameters": json.dumps({
                "storage_id": INVENTORY_ID,
                "inventory_id": inventory_id,
                "parent_category_id": "0",
                "name": "Default Category",
                "description": "Domyślna kategoria dla API Durczok CZK"
            }, ensure_ascii=False)
        }
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        if response_data.get("status") == "SUCCESS":
            category_id = str(response_data.get("category_id"))
            print(f"Utworzono nową kategorię: ID {category_id}")
            logging.info(f"Utworzono nową kategorię: ID {category_id}")
            return category_id
        else:
            logging.warning("Nie udało się utworzyć kategorii, użyto category_id=0.")
            print("Nie udało się utworzyć kategorii, użyto category_id=0.")
            return "0"
    except Exception as e:
        logging.error(f"Błąd podczas pobierania/utworzenia kategorii: {str(e)}")
        print(f"Błąd podczas pobierania/utworzenia kategorii: {str(e)}")
        return "0"

def fetch_and_parse_xml() -> List[Dict]:
    """Pobiera i parsuje plik XML z podanego URL lub ścieżki lokalnej (ceny w CZK), formatując nazwę w formacie g:mpn g:brand title."""
    products = []
    try:
        if XML_URL.startswith("file://"):
            # Lokalny plik
            parsed = urlparse(XML_URL)
            file_path = parsed.path
            if os.name == 'nt' and file_path.startswith('/'):
                file_path = file_path[1:]  # Usuń początkowy / dla Windows
            with open(file_path, "r", encoding="utf-8") as f:
                xml_content = f.read()
        else:
            # URL
            response = requests.get(XML_URL)
            response.raise_for_status()
            xml_content = response.content.decode('utf-8')
            xml_content = xml_content.lstrip('\ufeff')  # Usuń BOM jeśli występuje
        
        root = ET.fromstring(xml_content)
        namespace = {"g": "http://base.google.com/ns/1.0"}
        
        for item in root.findall(".//item"):
            # Parsowanie ceny
            price_czk = float(item.find("g:price", namespace).text) if item.find("g:price", namespace) is not None else 0.0
            
            # Parsowanie MPN (używany jako SKU)
            mpn_el = item.find("g:mpn", namespace)
            mpn = (mpn_el.text or "Unknown-MPN").strip() if mpn_el is not None else "Unknown-MPN"
            
            # Parsowanie marki
            brand_el = item.find("g:brand", namespace)
            brand = (brand_el.text or "Unknown-Brand").strip() if brand_el is not None else "Unknown-Brand"
            
            # Parsowanie tytułu - najpierw <title>, jeśli pusty to <description>
            title_elem = item.find("title")
            title = (title_elem.text if title_elem is not None and title_elem.text else "").strip()
            if not title:
                desc_elem = item.find("description")
                title = (desc_elem.text if desc_elem is not None and desc_elem.text else "Unknown-Title").strip()
            
            # Parsowanie zdjęcia
            img_elem = item.find("g:image_link", namespace)
            image_link = img_elem.text.strip() if img_elem is not None and img_elem.text else ""
            
            # Parsowanie dostępności (stan magazynu) - zawsze jest liczbą
            quantity = int(item.find("g:availability", namespace).text) if item.find("g:availability", namespace) is not None else 0
            
            # Parsowanie GTIN - zawsze pojedynczy numer
            gtin_el = item.find("g:gtin", namespace)
            ean = (gtin_el.text or "").strip() if gtin_el is not None else ""
            
            # Parsowanie kategorii z NX_StockCategory
            nx_stock_category = (item.find("NX_StockCategory").text if item.find("NX_StockCategory") is not None else "").strip()
            category = nx_stock_category if nx_stock_category else item.find("g:product_type", namespace).text if item.find("g:product_type", namespace) is not None else ""
            
            # Parsowanie ERP ID z g:id (wewnętrzne ID z ERP) - MUSI być z namespace!
            erp_id = (item.find("g:id", namespace).text if item.find("g:id", namespace) is not None else "").strip()
            
            # Formatowanie nazwy w formacie: g:mpn g:title (bez duplikatu marki, bo marka już jest w MPN)
            formatted_name = f"{mpn} {title}".strip()
            
            
            product = {
                "sku": mpn,
                "name": formatted_name,  # Formatowana nazwa bez duplikatu marki
                "quantity": quantity,
                "price_brutto": round(price_czk, 2),
                "ean": ean,
                "man_name": brand,
                "description": item.find("g:description", namespace).text if item.find("g:description", namespace) is not None else "",
                "category": category,
                "image_link": image_link,
                "erp_id": erp_id
            }
            
            products.append(product)

        return products
    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        print(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        return []
    except ET.ParseError as e:
        logging.error(f"Błąd podczas parsowania XML: {str(e)}")
        print(f"Błąd podczas parsowania XML: {str(e)}")
        return []

def add_product_to_baselinker(product: Dict, storage_id: str, category_id: str, inventory_id: str) -> Optional[Tuple[str, str]]:
    """Wysyła pojedynczy nowy produkt do BaseLinker przez Storage API (ceny w CZK)."""
    headers = {"X-BLToken": API_TOKEN}
    price_brutto_czk = product["price_brutto"]
    price_wholesale_netto_czk = price_brutto_czk / (1 + DEFAULT_TAX / 100)
    
    formatted_product = {
        "storage_id": storage_id,
        "product_id": "0",
        "sku": product["sku"],
        "name": product["name"],
        "quantity": product["quantity"],
        "price_brutto": price_brutto_czk,
        "price_wholesale_netto": round(price_wholesale_netto_czk, 2),
        "tax_rate": DEFAULT_TAX,
        "ean": product["ean"],
        "man_name": product["man_name"],
        "description": product["description"],
        "category_id": category_id,
        "location": "",
        "weight": 1.0,
        "images": {"0": f"url:{product['image_link']}"} if product.get("image_link") else {}
    }
    
    # Przygotowanie extra_fields z ERP_ID
    erp_id = product.get("erp_id", "").strip()
    if erp_id:
        try:
            # Wartość musi być numerem (integer), nie string!
            erp_id_int = int(erp_id)
            formatted_product["extra_fields"] = {"9157": erp_id_int}
        except ValueError:
            logging.warning(f"ERP_ID '{erp_id}' nie jest liczbą dla SKU {product['sku']}")
    

    params = {
        "method": "addProduct",
        "parameters": json.dumps(formatted_product, ensure_ascii=False)
    }
    
    try:
        limiter.wait()
        session = get_session()
        response = session.post(API_URL, headers=headers, data=params, timeout=60)

        response_data = response.json()
        

        
        if response_data.get("status") != "SUCCESS":
            error_message = response_data.get("error_message", "Brak szczegółów błędu")
            logging.error(f"Błąd API (addProduct) dla SKU {product['sku']}: {error_message}")
            print(f"Błąd API (addProduct) dla SKU {product['sku']}: {error_message}")
            
            # Sprawdzenie, czy błąd to przekroczenie limitu zapytań
            if "Query limit exceeded, token blocked until" in error_message:
                print(f"Wykryto przekroczenie limitu zapytań. Pauza na 12 minut...")
                logging.info(f"Wykryto przekroczenie limitu zapytań. Pauza na 12 minut...")
                time.sleep(PAUSE_DURATION)
                print(f"Pauza zakończona. Wznawianie pracy...")
                logging.info(f"Pauza zakończona. Wznawianie pracy...")
                # Ponowna próba dodania produktu po odczekaniu
                response = requests.post(API_URL, headers=headers, data=params)
                response_data = response.json()
                if response_data.get("status") != "SUCCESS":
                    logging.error(f"Ponowna próba nieudana dla SKU {product['sku']}: {response_data.get('error_message', 'Brak szczegółów błędu')}")
                    print(f"Ponowna próba nieudana dla SKU {product['sku']}: {response_data.get('error_message', 'Brak szczegółów błędu')}")
                    return None
        
        # Sprawdź czy product_id istnieje i nie jest None
        product_id = response_data.get("product_id")
        if product_id and str(product_id) != "0" and str(product_id).lower() != "none":
            product_id_str = str(product_id)
            logging.info(f"Pomyślnie dodano produkt: SKU={product['sku']} -> ID={product_id_str}")
            print(f"Pomyślnie dodano produkt: SKU={product['sku']} -> ID={product_id_str}")

            # ✅ zwróć wynik do wątku głównego
            return (product["sku"], product_id_str)
        else:
            logging.error(f"Brak product_id lub product_id=None w odpowiedzi API dla SKU {product['sku']}: {response_data}")
            print(f"Brak product_id lub product_id=None w odpowiedzi API dla SKU {product['sku']}: {response_data}")
            return None
    except Exception as e:
        logging.error(f"Błąd podczas wysyłania żądania (addProduct) dla SKU {product['sku']}: {str(e)}")
        print(f"Błąd podczas wysyłania żądania (addProduct) dla SKU {product['sku']}: {str(e)}")
        return None

def add_products_from_xml():
    """Główna funkcja dodawania produktów z pliku XML online (ceny w CZK) z użyciem partii."""
    load_sku_to_id()
    
    storage_id = get_valid_storage_id()
    if not storage_id:
        logging.error("Nie można kontynuować: nieprawidłowy ID magazynu.")
        print("Nie można kontynuować: nieprawidłowy ID magazynu. Sprawdź API_TOKEN i INVENTORY_ID.")
        return
    
    category_id = create_category_if_needed(NEW_INVENTORY_ID)
    
    products = fetch_and_parse_xml()
    if not products:
        logging.error("Brak produktów do przetworzenia.")
        print("Brak produktów do przetworzenia. Sprawdź URL XML Lub jego składnię.")
        return
    
    new_products = [p for p in products if p["sku"] not in sku_to_id_cache]
    if not new_products:
        logging.info("Brak nowych produktów do dodania.")
        print("Brak nowych produktów do dodania.")
        return
    
    print(f"Znaleziono {len(new_products)} nowych produktów do dodania.")
    logging.info(f"Znaleziono {len(new_products)} nowych produktów do dodania.")
    
    # Przetwarzanie w partiach po BATCH_SIZE (500) produktów na minutę
    failed_products = []
    iterator = iter(new_products)
    batch_number = 1
    
    while True:
        batch = list(islice(iterator, BATCH_SIZE))
        if not batch:
            break
        
        print(f"Przetwarzanie partii {batch_number} ({len(batch)} produktów)...")
        logging.info(f"Przetwarzanie partii {batch_number} ({len(batch)} produktów)...")
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_product = {
                executor.submit(add_product_to_baselinker, product, storage_id, category_id, NEW_INVENTORY_ID): product
                for product in batch
            }

            batch_added = 0

            for future in as_completed(future_to_product):
                res = future.result()
                if res is None:
                    failed_products.append(product)
                else:
                    sku, product_id = res
                    sku_to_id_cache[sku] = product_id
                    batch_added += 1
                    if batch_added % 500 == 0:
                        save_sku_to_id()
                        print(f"Zapis pośredni: {batch_added} dodanych w partii {batch_number}")



        # ✅ zapis raz po partii
        if batch_added > 0:
            save_sku_to_id()
            print(f"Partia {batch_number}: dopisano {batch_added} nowych SKU do sku_to_id.json")

        

        
        batch_number += 1
    
    if failed_products:
        with open("failed_products_add.json", "w", encoding="utf-8") as f:
            json.dump(failed_products, f, ensure_ascii=False, indent=2)
        logging.warning(f"Nieudane produkty zapisano do failed_products_add.json ({len(failed_products)} produktów).")
        print(f"Nieudane produkty zapisano do failed_products_add.json ({len(failed_products)} produktów).")
    else:
        print("Wszystkie nowe produkty dodano pomyślnie!")

if __name__ == "__main__":
    add_products_from_xml()