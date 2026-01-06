import xml.etree.ElementTree as ET
import requests
import time
import json
import logging
import os
from dotenv import load_dotenv
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

load_dotenv()

# Konfiguracja
API_TOKEN = os.environ.get('API_TOKEN')  # Wstaw swój token API BaseLinker jako zmienną środowiskową
API_URL = os.environ.get('API_URL')
INVENTORY_ID = os.environ.get('INVENTORY_ID')  # Poprawny ID magazynu BaseLinker (DurczokAPI), do zmiany na nowy inventory_id
NEW_INVENTORY_ID = os.environ.get('NEW_INVENTORY_ID')  # Wstaw ID nowego katalogu z add_new_inventory.py
PRICE_GROUP_ID = os.environ.get('PRICE_GROUP_ID') # ID grupy cenowej CZK (API DurczokCZK)
BATCH_SIZE = 1000  # Optymalizacja dla 1000 produktów na zapytanie
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 5))  # Liczba równoległych wątków
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 80))
SLEEP_TIME = 60 / (REQUESTS_PER_MINUTE / MAX_WORKERS)  # Czas między żądaniami dla każdego wątku
DEFAULT_TAX = 21
SKU_TO_ID_FILE = "sku_to_id.json"  # Plik do przechowywania mapowania SKU -> product_id
XML_URL = os.environ.get('XML_URL')  # URL do pliku XML

# Konfiguracja logowania
logging.basicConfig(
    filename="update_products.log",
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

def get_category_id(inventory_id: str) -> str:
    """Pobiera listę kategorii BaseLinker i zwraca pierwszą dostępną lub 0."""
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
            print(f"Znaleziono domyślną kategorię: ID {category_id}")
            logging.info(f"Znaleziono domyślną kategorię: ID {category_id}")
            return category_id
        else:
            logging.warning("Brak dostępnych kategorii w BaseLinker, użyto category_id=0.")
            print("Brak dostępnych kategorii w BaseLinker, użyto category_id=0.")
            return "0"
    except Exception as e:
        logging.error(f"Błąd podczas pobierania kategorii: {str(e)}")
        print(f"Błąd podczas pobierania kategorii: {str(e)}")
        return "0"

def fetch_and_parse_xml() -> List[Dict]:
    """Pobiera i parsuje plik XML z podanego URL (ceny w CZK)."""
    products = []
    try:
        # Pobranie XML z URL
        response = requests.get(XML_URL)
        response.raise_for_status()  # Sprawdzenie, czy żądanie się powiodło
        xml_content = response.content.decode("utf-8")
        
        # Parsowanie XML
        root = ET.fromstring(xml_content)
        namespace = {"g": "http://base.google.com/ns/1.0"}
        
        for item in root.findall(".//item"):
            price_czk = float(item.find("g:price", namespace).text) if item.find("g:price", namespace) is not None and item.find("g:price", namespace).text.replace(".", "").isdigit() else 0.0
            product = {
                "sku": item.find("g:mpn", namespace).text if item.find("g:mpn", namespace) is not None else "",
                "name": item.find("title").text if item.find("title") is not None else "",
                "quantity": int(item.find("g:availability", namespace).text) if item.find("g:availability", namespace) is not None and item.find("g:availability", namespace).text.isdigit() else 0,
                "price_brutto": round(price_czk, 2),  # Cena już w CZK, zaokrąglona do 2 miejsc
                "ean": item.find("g:gtin", namespace).text if item.find("g:gtin", namespace) is not None else "",
                "man_name": item.find("g:brand", namespace).text if item.find("g:brand", namespace) is not None else "",
                "description": item.find("g:description", namespace).text if item.find("g:description", namespace) is not None else "",
                "category": item.find("g:product_type", namespace).text if item.find("g:product_type", namespace) is not None else ""
            }
            # Walidacja nazwy produktu
            if not product["name"] or len(product["name"].strip()) < 3:
                product["name"] = product["sku"] or "Unknown Product"
                logging.warning(f"Nieprawidłowa nazwa produktu dla SKU {product['sku']}, użyto: {product['name']}")
                print(f"Nieprawidłowa nazwa produktu dla SKU {product['sku']}, użyto: {product['name']}")
            
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

def update_product_quantity_in_baselinker(products: List[Dict], storage_id: str, sku_to_id: Dict[str, str], inventory_id: str) -> bool:
    """Aktualizuje stany produktów w BaseLinker przez API."""
    headers = {"X-BLToken": API_TOKEN}
    formatted_products = []
    
    for product in products:
        product_id = sku_to_id.get(product["sku"], "0")
        if product_id != "0":  # Aktualizuj tylko jeśli produkt istnieje
            formatted_products.append([int(product_id), 0, product["quantity"]])
            print(f"Aktualizacja stanu produktu: SKU={product['sku']}, Product ID={product_id}, Stan={product['quantity']}")
    
    if not formatted_products:
        return True  # Brak produktów do aktualizacji
    
    params = {
        "method": "updateProductsQuantity",
        "parameters": json.dumps({
            "storage_id": storage_id,
            "inventory_id": inventory_id,  # Użycie nowego katalogu
            "products": formatted_products
        }, ensure_ascii=False)
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        
        if response_data.get("status") == "SUCCESS":
            logging.info(f"Pomyślnie zaktualizowano stany {len(formatted_products)} produktów.")
            print(f"Pomyślnie zaktualizowano stany {len(formatted_products)} produktów.")
            return True
        else:
            logging.error(f"Błąd API (updateProductsQuantity): {response_data.get('error_message', 'Brak szczegółów błędu')}")
            print(f"Błąd API (updateProductsQuantity): {response_data.get('error_message', 'Brak szczegółów błędu')}")
            return False
    except Exception as e:
        logging.error(f"Błąd podczas wysyłania żądania (updateProductsQuantity): {str(e)}")
        print(f"Błąd podczas wysyłania żądania (updateProductsQuantity): {str(e)}")
        return False

def update_product_prices_in_baselinker(products: List[Dict], storage_id: str, sku_to_id: Dict[str, str], inventory_id: str) -> bool:
    """Aktualizuje ceny produktów w BaseLinker przez API (ceny w CZK)."""
    headers = {"X-BLToken": API_TOKEN}
    formatted_products = []
    
    for product in products:
        product_id = sku_to_id.get(product["sku"], "0")
        if product_id != "0":  # Aktualizuj tylko jeśli produkt istnieje
            price_brutto_czk = product["price_brutto"]  # Cena już w CZK
            formatted_product = {
                "product_id": int(product_id),
                "variant_id": 0,
                "price_brutto": price_brutto_czk,
                "tax_rate": DEFAULT_TAX,
                "price_group_id": PRICE_GROUP_ID  # Ustawienie grupy cenowej CZK
            }
            formatted_products.append(formatted_product)
            print(f"Aktualizacja ceny produktu: SKU={product['sku']}, Product ID={product_id}, Cena (CZK)={price_brutto_czk}, Grupa cenowa={PRICE_GROUP_ID}")
            logging.info(f"Aktualizacja ceny produktu: SKU={product['sku']}, Product ID={product_id}, Cena (CZK)={price_brutto_czk}, Grupa cenowa={PRICE_GROUP_ID}")
    
    if not formatted_products:
        return True  # Brak produktów do aktualizacji
    
    params = {
        "method": "updateProductsPrices",
        "parameters": json.dumps({
            "storage_id": storage_id,
            "inventory_id": inventory_id,  # Użycie nowego katalogu
            "products": formatted_products
        }, ensure_ascii=False)
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        
        if response_data.get("status") == "SUCCESS":
            logging.info(f"Pomyślnie zaktualizowano ceny {len(formatted_products)} produktów.")
            print(f"Pomyślnie zaktualizowano ceny {len(formatted_products)} produktów.")
            return True
        else:
            logging.error(f"Błąd API (updateProductsPrices): {response_data.get('error_message', 'Brak szczegółów błędu')}")
            print(f"Błąd API (updateProductsPrices): {response_data.get('error_message', 'Brak szczegółów błędu')}")
            return False
    except Exception as e:
        logging.error(f"Błąd podczas wysyłania żądania (updateProductsPrices): {str(e)}")
        print(f"Błąd podczas wysyłania żądania (updateProductsPrices): {str(e)}")
        return False

def process_batch(batch: List[Dict], queue: Queue, storage_id: str, sku_to_id: Dict[str, str], inventory_id: str):
    """Przetwarza partię produktów i umieszcza wyniki w kolejce."""
    existing_products = [p for p in batch if p["sku"] in sku_to_id]
    print(f"Przetwarzanie partii ({len(existing_products)} produktów)...")
    
    if existing_products:
        # Najpierw aktualizacja stanów
        success_quantity = update_product_quantity_in_baselinker(existing_products, storage_id, sku_to_id, inventory_id)
        # Następnie aktualizacja cen
        success_prices = update_product_prices_in_baselinker(existing_products, storage_id, sku_to_id, inventory_id)
        # Zapis wyniku tylko jeśli obie operacje się powiodły
        success = success_quantity and success_prices
        queue.put((success, existing_products, "update"))

def update_products_from_xml():
    """Główna funkcja aktualizacji produktów z pliku XML online (ceny w CZK)."""
    # Załaduj bazę SKU-to-ID
    load_sku_to_id()
    
    # Sprawdzenie poprawności magazynu
    storage_id = get_valid_storage_id()
    if not storage_id:
        logging.error("Nie można kontynuować: nieprawidłowy ID magazynu.")
        print("Nie można kontynuować: nieprawidłowy ID magazynu. Sprawdź API_TOKEN i INVENTORY_ID.")
        return
    
    # Pobieranie kategorii dla nowego katalogu
    get_category_id(NEW_INVENTORY_ID)
    
    # Parsowanie XML z URL
    products = fetch_and_parse_xml()
    if not products:
        logging.error("Brak produktów do przetworzenia.")
        print("Brak produktów do przetworzenia. Sprawdź URL XML.")
        return
    
    # Podział na partie
    batches = [products[i:i + BATCH_SIZE] for i in range(0, len(products), BATCH_SIZE)]
    logging.info(f"Podzielono na {len(batches)} partii po {BATCH_SIZE} produktów.")
    print(f"Podzielono na {len(batches)} partii po {BATCH_SIZE} produktów.")
    
    # Kolejka do przechowywania wyników
    result_queue = Queue()
    
    # Przetwarzanie partii w wątkach
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch in batches:
            executor.submit(process_batch, batch, result_queue, storage_id, sku_to_id_cache, NEW_INVENTORY_ID)
            time.sleep(SLEEP_TIME)
    
    # Logowanie wyników
    failed_products = []
    while not result_queue.empty():
        success, batch, action = result_queue.get()
        if not success:
            failed_products.extend(batch)
            logging.warning(f"Nieudane {action} dla {len(batch)} produktów.")
            print(f"Nieudane {action} dla {len(batch)} produktów.")
    
    # Zapisanie nieudanych produktów do osobnego pliku
    if failed_products:
        with open("failed_products_update.json", "w", encoding="utf-8") as f:
            json.dump(failed_products, f, ensure_ascii=False, indent=2)
        logging.warning(f"Nieudane produkty zapisano do failed_products_update.json ({len(failed_products)} produktów).")
        print(f"Nieudane produkty zapisano do failed_products_update.json ({len(failed_products)} produktów).")
    else:
        print("Wszystkie produkty zaktualizowano pomyślnie!")

if __name__ == "__main__":
    update_products_from_xml()