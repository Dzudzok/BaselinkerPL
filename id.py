import requests

API_TOKEN = "6008178-6011029-R5ZC2W5BZKP2JRBFBLDFO5203A3I68OUA8HLLUB7T6XYOQLSOM89E6321EIL2H38"

url = "https://api.baselinker.com/connector.php"

payload = {
    "token": API_TOKEN,
    "method": "getInventoryExtraFields",
    "parameters": "{}"
}

response = requests.post(url, data=payload)
response.raise_for_status()

data = response.json()

if data.get("status") != "SUCCESS":
    print("❌ Błąd API:", data)
    exit(1)

print("✅ Extra fields w BaseLinkerze:\n")

for field in data.get("extra_fields", []):
    field_id = field.get("extra_field_id")
    name = field.get("name")
    field_type = field.get("type", "n/a")  # <-- bezpieczne

    print(f"ID: {field_id} | Nazwa: {name} | Typ: {field_type}")
