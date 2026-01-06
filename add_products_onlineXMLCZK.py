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

load_dotenv()

# Konfiguracja
API_TOKEN = os.environ.get('API_TOKEN')  # Wstaw swój token API BaseLinker jako zmienną środowiskową
API_URL = "https://api.baselinker.com/connector.php"
INVENTORY_ID = "bl_1"  # Magazyn BaseLinker (DurczokAPI)
NEW_INVENTORY_ID = "17695"  # Wstaw poprawny ID nowego katalogu
PRICE_GROUP_ID = 15432  # ID grupy cenowej CZK (API DurczokCZK)
REQUESTS_PER_MINUTE = 80  # Limit dla dodawania produktów
MAX_WORKERS = 5  # Liczba równoległych wątków
BATCH_SIZE = REQUESTS_PER_MINUTE  # Partia produktów na minutę
BATCH_INTERVAL = 60  # Odstęp między partiami (60 sekund)
DEFAULT_TAX = 21  # Domyślny VAT (23%)
SKU_TO_ID_FILE = "sku_to_id.json"  # Plik do przechowywania mapowania SKU -> product_id
XML_URL = os.environ.get('XML_URL')  # URL do pliku XML
PAUSE_DURATION = 360  # 12 minut w sekundach

# Konfiguracja logowania
logging.basicConfig(
    filename="add_products.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Globalna zmienna do przechowywania bazy SKU-to-ID w pamięci
sku_to_id_cache = {}

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
    """Zapisuje mapowanie SKU -> product_id do pliku JSON."""
    try:
        with open(SKU_TO_ID_FILE, "w", encoding="utf-8") as f:
            json.dump(sku_to_id_cache, f, ensure_ascii=False, indent=2)
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
    """Pobiera i parsuje plik XML z podanego URL (ceny w CZK), formatując nazwę w formacie g:mpn g:brand title."""
    products = []
    try:
        response = requests.get(XML_URL)
        response.raise_for_status()
        xml_content = response.content.decode("utf-8")
        
        root = ET.fromstring(xml_content)
        namespace = {"g": "http://base.google.com/ns/1.0"}
        
        for item in root.findall(".//item"):
            price_czk = float(item.find("g:price", namespace).text) if item.find("g:price", namespace) is not None and item.find("g:price", namespace).text.replace(".", "").isdigit() else 0.0
            mpn = (item.find("g:mpn", namespace).text if item.find("g:mpn", namespace) is not None else "Unknown-MPN").strip()
            brand = (item.find("g:brand", namespace).text if item.find("g:brand", namespace) is not None else "Unknown-Brand").strip()
            title = (item.find("title").text if item.find("title") is not None else "Unknown-Title").strip()
            
            # Formatowanie nazwy w formacie: g:mpn g:brand title
            formatted_name = f"{mpn} {brand} {title}".strip()
            
            # Logowanie wartości dla debugowania
            logging.info(f"Parsowanie produktu: SKU={mpn}, MPN={mpn}, Brand={brand}, Title={title}, Sformatowana nazwa={formatted_name}")
            print(f"Parsowanie produktu: SKU={mpn}, MPN={mpn}, Brand={brand}, Title={title}, Sformatowana nazwa={formatted_name}")
            
            product = {
                "sku": mpn,
                "name": formatted_name,  # Używamy sformatowanej nazwy
                "quantity": int(item.find("g:availability", namespace).text) if item.find("g:availability", namespace) is not None and item.find("g:availability", namespace).text.isdigit() else 0,
                "price_brutto": round(price_czk, 2),
                "ean": item.find("g:gtin", namespace).text if item.find("g:gtin", namespace) is not None else "",
                "man_name": brand,
                "description": item.find("g:description", namespace).text if item.find("g:description", namespace) is not None else "",
                "category": item.find("g:product_type", namespace).text if item.find("g:product_type", namespace) is not None else ""
            }
            
            products.append(product)
        
        logging.info(f"Pomyślnie sparsowano {len(products)} produktów z XML online (ceny w CZK).")
        print(f"Pomyślnie sparsowano {len(products)} produktów z XML online (ceny w CZK).")
        return products
    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        print(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        return []
    except ET.ParseError as e:
        logging.error(f"Błąd podczas parsowania XML: {str(e)}")
        print(f"Błąd podczas parsowania XML: {str(e)}")
        return []

def add_product_to_baselinker(product: Dict, storage_id: str, category_id: str, inventory_id: str) -> bool:
    """Wysyła pojedynczy nowy produkt do BaseLinker przez API (ceny w CZK)."""
    headers = {"X-BLToken": API_TOKEN}
    price_brutto_czk = product["price_brutto"]
    price_wholesale_netto_czk = price_brutto_czk / (1 + DEFAULT_TAX / 100)
    formatted_product = {
        "storage_id": storage_id,
        "inventory_id": inventory_id,
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
        "weight": 0.0,
        "price_group_id": PRICE_GROUP_ID
    }
    
    # Logowanie pełnego payloadu przed wysłaniem
    logging.info(f"Pełny payload wysyłany do BaseLinker: {json.dumps(formatted_product, ensure_ascii=False)}")
    print(f"Pełny payload wysyłany do BaseLinker: {json.dumps(formatted_product, ensure_ascii=False)}")
    
    print(f"Wysyłanie produktu: SKU={product['sku']}, Nazwa={product['name']}, Cena brutto (CZK)={price_brutto_czk}, Grupa cenowa={PRICE_GROUP_ID}, Inventory ID={inventory_id}")
    logging.info(f"Wysyłanie produktu: SKU={product['sku']}, Nazwa={product['name']}, Cena brutto (CZK)={price_brutto_czk}, Grupa cenowa={PRICE_GROUP_ID}, Inventory ID={inventory_id}")
    params = {
        "method": "addProduct",
        "parameters": json.dumps(formatted_product, ensure_ascii=False)
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
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
                    return False
        
        product_id = str(response_data.get("product_id"))
        if product_id:
            sku_to_id_cache[product["sku"]] = product_id
            save_sku_to_id()
            logging.info(f"Pomyślnie dodano produkt: SKU={product['sku']}, Product ID={product_id}, Inventory ID={inventory_id}")
            print(f"Pomyślnie dodano produkt: SKU={product['sku']}, Product ID={product_id}, Inventory ID={inventory_id}")
            return True
        else:
            logging.error(f"Brak product_id w odpowiedzi API dla SKU {product['sku']}: {response_data}")
            print(f"Brak product_id w odpowiedzi API dla SKU {product['sku']}: {response_data}")
            return False
    except Exception as e:
        logging.error(f"Błąd podczas wysyłania żądania (addProduct) dla SKU {product['sku']}: {str(e)}")
        print(f"Błąd podczas wysyłania żądania (addProduct) dla SKU {product['sku']}: {str(e)}")
        return False

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
        print("Brak produktów do przetworzenia. Sprawdź URL XML.")
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
            for future in future_to_product:
                if not future.result():
                    failed_products.append(future_to_product[future])
        
        # Oblicz czas przetworzenia partii
        elapsed_time = time.time() - start_time
        print(f"Partia {batch_number} zakończona w {elapsed_time:.2f} sekund.")
        logging.info(f"Partia {batch_number} zakończona w {elapsed_time:.2f} sekund.")
        
        # Poczekaj do końca minuty, aby zmieścić się w limicie
        if elapsed_time < BATCH_INTERVAL:
            sleep_time = BATCH_INTERVAL - elapsed_time
            print(f"Czekam {sleep_time:.2f} sekund przed następną partią...")
            logging.info(f"Czekam {sleep_time:.2f} sekund przed następną partią...")
            time.sleep(sleep_time)
        
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