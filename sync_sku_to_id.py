import requests
import json
import logging
import os
import time
from typing import Dict

# Konfiguracja
API_TOKEN = "3007084-3028072-5KCONQEJOMDU02OJ8U06VM5NWAVBXURMHZ6OF0HVTRQMMGMNWIOF3L171TPYCBV3"  # Wstaw swój token API BaseLinker
API_URL = "https://api.baselinker.com/connector.php"
INVENTORY_ID = "bl_1"  # Poprawny ID magazynu BaseLinker
SKU_TO_ID_FILE = "sku_to_id.json"  # Plik do przechowywania mapowania SKU -> product_id
REQUESTS_PER_MINUTE = 500  # Limit zapytań na minutę
SLEEP_TIME = 60 / REQUESTS_PER_MINUTE  # Czas między żądaniami (0.12s)

# Konfiguracja logowania
logging.basicConfig(
    filename="sync_sku_to_id.log",
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
    else:
        logging.warning("Plik SKU-to-ID nie istnieje. Inicjalizowanie pustej bazy.")
        print("Plik SKU-to-ID nie istnieje. Inicjalizowanie pustej bazy.")
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

def get_products_from_baselinker(storage_id: str) -> Dict[str, str]:
    """Pobiera listę wszystkich produktów z BaseLinker dla podanego magazynu z paginacją."""
    headers = {"X-BLToken": API_TOKEN}
    current_skus = {}
    page = 1
    while True:
        params = {
            "method": "getProductsList",
            "parameters": json.dumps({
                "storage_id": storage_id,
                "page": page,
                "include_variants": False  # Pobierz tylko główne produkty (bez wariantów)
            })
        }
        
        try:
            response = requests.post(API_URL, headers=headers, data=params)
            response_data = response.json()
            
            if response_data.get("status") != "SUCCESS":
                logging.error(f"Błąd pobierania produktów z BaseLinker (strona {page}): {response_data.get('error_message', 'Brak szczegółów błędu')}")
                print(f"Błąd pobierania produktów z BaseLinker (strona {page}): {response_data.get('error_message', 'Brak szczegółów błędu')}")
                return current_skus
            
            products = response_data.get("products", [])
            if not products:  # Brak kolejnych produktów – koniec paginacji
                break
                
            for product in products:
                sku = product.get("sku", "")
                product_id = product.get("product_id", "")
                if sku and product_id:
                    current_skus[sku] = product_id
            
            logging.info(f"Pobrano {len(products)} produktów z BaseLinker (strona {page}).")
            print(f"Pobrano {len(products)} produktów z BaseLinker (strona {page}).")
            page += 1
            time.sleep(SLEEP_TIME)  # Respektowanie limitu 500/min
        except Exception as e:
            logging.error(f"Błąd podczas pobierania produktów z BaseLinker (strona {page}): {str(e)}")
            print(f"Błąd podczas pobierania produktów z BaseLinker (strona {page}): {str(e)}")
            return current_skus
    
    logging.info(f"Łącznie pobrano {len(current_skus)} produktów z BaseLinker.")
    print(f"Łącznie pobrano {len(current_skus)} produktów z BaseLinker.")
    return current_skus

def sync_sku_to_id():
    """Synchronizuje sku_to_id.json z aktualnym stanem produktów w BaseLinker."""
    global sku_to_id_cache
    # Załaduj istniejącą bazę
    load_sku_to_id()
    
    # Sprawdzenie poprawności magazynu
    storage_id = get_valid_storage_id()
    if not storage_id:
        logging.error("Nie można kontynuować: nieprawidłowy ID magazynu.")
        print("Nie można kontynuować: nieprawidłowy ID magazynu. Sprawdź API_TOKEN i INVENTORY_ID.")
        return
    
    # Pobierz aktualną listę produktów z BaseLinker
    current_products = get_products_from_baselinker(storage_id)
    
    # Aktualizuj bazę SKU-to-ID
    initial_count = len(sku_to_id_cache)
    updated_count = 0
    new_entries = 0
    
    for sku, product_id in current_products.items():
        if sku not in sku_to_id_cache:
            sku_to_id_cache[sku] = product_id
            new_entries += 1
        elif sku_to_id_cache[sku] != product_id:
            sku_to_id_cache[sku] = product_id
            updated_count += 1
    
    # Usuń SKU, które nie istnieją w aktualnej liście z BaseLinker
    removed_count = sum(1 for sku in list(sku_to_id_cache.keys()) if sku not in current_products)
    if removed_count > 0:
        sku_to_id_cache = {sku: product_id for sku, product_id in sku_to_id_cache.items() if sku in current_products}
    
    # Zapisanie bazy, jeśli były zmiany lub baza była pusta
    if new_entries > 0 or updated_count > 0 or removed_count > 0 or initial_count == 0:
        if new_entries > 0:
            logging.info(f"Dodano {new_entries} nowych SKU do bazy SKU-to-ID.")
            print(f"Dodano {new_entries} nowych SKU do bazy SKU-to-ID.")
        if updated_count > 0:
            logging.info(f"Zaktualizowano {updated_count} istniejących SKU w bazie SKU-to-ID.")
            print(f"Zaktualizowano {updated_count} istniejących SKU w bazie SKU-to-ID.")
        if removed_count > 0:
            logging.info(f"Usunięto {removed_count} nieistniejących SKU z bazy SKU-to-ID.")
            print(f"Usunięto {removed_count} nieistniejących SKU z bazy SKU-to-ID.")
        save_sku_to_id()
    else:
        logging.info("Brak zmian w bazie SKU-to-ID – wszystkie SKU są aktualne.")
        print("Brak zmian w bazie SKU-to-ID – wszystkie SKU są aktualne.")

if __name__ == "__main__":
    sync_sku_to_id()