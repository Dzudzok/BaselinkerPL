import xml.etree.ElementTree as ET
import requests
import time
import json
import logging
import os
from typing import List, Dict

# Konfiguracja
API_TOKEN = "3007084-3028072-5KCONQEJOMDU02OJ8U06VM5NWAVBXURMHZ6OF0HVTRQMMGMNWIOF3L171TPYCBV3"  # Wstaw swój token API BaseLinker
API_URL = "https://api.baselinker.com/connector.php"
INVENTORY_ID = "bl_1"  # Poprawny ID magazynu BaseLinker
REQUESTS_PER_MINUTE = 480  # Limit dla dodawania produktów
SLEEP_TIME = 60 / REQUESTS_PER_MINUTE  # = 0.3s między zapytaniami
DEFAULT_TAX = 23  # Domyślny VAT (23%)
SKU_TO_ID_FILE = "sku_to_id.json"  # Plik do przechowywania mapowania SKU -> product_id
XML_URL = "https://exports.conviu.com/open/u70usd1xo1y5fknhd1xm8kixkdsya8po/writer/lb5423mwbhnc1lw6664gdwzwv09thmjz.xml"  # URL do pliku XML

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

def get_category_id() -> str:
    """Pobiera listę kategorii BaseLinker i zwraca pierwszą dostępną lub 0."""
    headers = {"X-BLToken": API_TOKEN}
    params = {
        "method": "getProductCatalogCategories",
        "parameters": json.dumps({"storage_id": INVENTORY_ID})
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
    """Pobiera i parsuje plik XML z podanego URL."""
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
            product = {
                "sku": item.find("g:mpn", namespace).text if item.find("g:mpn", namespace) is not None else "",
                "name": item.find("title").text if item.find("title") is not None else "",
                "quantity": int(item.find("g:availability", namespace).text) if item.find("g:availability", namespace) is not None and item.find("g:availability", namespace).text.isdigit() else 0,
                "price_brutto": float(item.find("g:price", namespace).text) if item.find("g:price", namespace) is not None and item.find("g:price", namespace).text.replace(".", "").isdigit() else 0.0,
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
        
        logging.info(f"Pomyślnie sparsowano {len(products)} produktów z XML online.")
        print(f"Pomyślnie sparsowano {len(products)} produktów z XML online.")
        return products
    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        print(f"Błąd podczas pobierania XML z URL {XML_URL}: {str(e)}")
        return []
    except ET.ParseError as e:
        logging.error(f"Błąd podczas parsowania XML: {str(e)}")
        print(f"Błąd podczas parsowania XML: {str(e)}")
        return []

def add_product_to_baselinker(product: Dict, storage_id: str, category_id: str) -> bool:
    """Wysyła pojedynczy nowy produkt do BaseLinker przez API."""
    headers = {"X-BLToken": API_TOKEN}
    price_wholesale_netto = product["price_brutto"] / (1 + DEFAULT_TAX / 100)
    formatted_product = {
        "storage_id": storage_id,
        "product_id": "0",
        "sku": product["sku"],
        "name": product["name"],
        "quantity": product["quantity"],
        "price_brutto": product["price_brutto"],
        "price_wholesale_netto": round(price_wholesale_netto, 2),
        "tax_rate": DEFAULT_TAX,
        "ean": product["ean"],
        "man_name": product["man_name"],
        "description": product["description"],
        "category_id": category_id,
        "location": "",
        "weight": 0.0
    }
    
    print(f"Wysyłanie produktu: SKU={product['sku']}, Nazwa={product['name']}")
    params = {
        "method": "addProduct",
        "parameters": json.dumps(formatted_product, ensure_ascii=False)
    }
    
    try:
        response = requests.post(API_URL, headers=headers, data=params)
        response_data = response.json()
        
        if response_data.get("status") != "SUCCESS":
            logging.error(f"Błąd API (addProduct) dla SKU {product['sku']}: {response_data.get('error_message', 'Brak szczegółów błędu')}")
            print(f"Błąd API (addProduct) dla SKU {product['sku']}: {response_data.get('error_message', 'Brak szczegółów błędu')}")
            return False
        
        product_id = str(response_data.get("product_id"))
        if product_id:
            sku_to_id_cache[product["sku"]] = product_id
            save_sku_to_id()
            logging.info(f"Pomyślnie dodano produkt: SKU={product['sku']}, Product ID={product_id}")
            print(f"Pomyślnie dodano produkt: SKU={product['sku']}, Product ID={product_id}")
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
    """Główna funkcja dodawania produktów z pliku XML online."""
    # Załaduj bazę SKU-to-ID
    load_sku_to_id()
    
    # Sprawdzenie poprawności magazynu
    storage_id = get_valid_storage_id()
    if not storage_id:
        logging.error("Nie można kontynuować: nieprawidłowy ID magazynu.")
        print("Nie można kontynuować: nieprawidłowy ID magazynu. Sprawdź API_TOKEN i INVENTORY_ID.")
        return
    
    # Pobieranie kategorii
    category_id = get_category_id()
    
    # Parsowanie XML z URL
    products = fetch_and_parse_xml()
    if not products:
        logging.error("Brak produktów do przetworzenia.")
        print("Brak produktów do przetworzenia. Sprawdź URL XML.")
        return
    
    # Filtracja nowych produktów (nieobecnych w bazie)
    new_products = [p for p in products if p["sku"] not in sku_to_id_cache]
    if not new_products:
        logging.info("Brak nowych produktów do dodania.")
        print("Brak nowych produktów do dodania.")
        return
    
    # Dodawanie nowych produktów
    failed_products = []
    for product in new_products:
        if not add_product_to_baselinker(product, storage_id, category_id):
            failed_products.append(product)
        time.sleep(SLEEP_TIME)  # Respektowanie limitu 100/min
    
    # Zapisanie nieudanych produktów
    if failed_products:
        with open("failed_products_add.json", "w", encoding="utf-8") as f:
            json.dump(failed_products, f, ensure_ascii=False, indent=2)
        logging.warning(f"Nieudane produkty zapisano do failed_products_add.json ({len(failed_products)} produktów).")
        print(f"Nieudane produkty zapisano do failed_products_add.json ({len(failed_products)} produktów).")
    else:
        print("Wszystkie nowe produkty dodano pomyślnie!")

if __name__ == "__main__":
    add_products_from_xml()