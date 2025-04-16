import json
import os
import hashlib
import hmac
import base64
import tempfile
import difflib
import logging
import requests
import time 
from datetime import datetime
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build

# === CONFIG ===
TRIGGER_TAG = "pc"
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "")

SHOP_DOMAIN_TO_SHEET = {
    "fdd92b-2e.myshopify.com": os.getenv("SHEET_IRRANOVA_ID"),
    "nouralibas.myshopify.com": os.getenv("SHEET_IRRAKIDS_ID")
}

STORES = [
    {
        "name": "irranova",
        "spreadsheet_id": os.getenv("SHEET_IRRANOVA_ID"),
        "shop_domain": "fdd92b-2e.myshopify.com",
        "api_key": os.getenv("SHOPIFY_API_KEY_IRRANOVA"),
        "password": os.getenv("SHOPIFY_PASSWORD_IRRANOVA")
    },
    {
        "name": "irrakids",
        "spreadsheet_id": os.getenv("SHEET_IRRAKIDS_ID"),
        "shop_domain": "nouralibas.myshopify.com",
        "api_key": os.getenv("SHOPIFY_API_KEY_IRRAKIDS"),
        "password": os.getenv("SHOPIFY_PASSWORD_IRRAKIDS")
    }
]

CITY_ALIAS_PATH = "city_aliases.json"
CITY_LIST_PATH = "cities_bigdelivery.txt"

# === LOGGER ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True  # ensures log output shows on Render
)

# === LOAD CITY ALIASES AND LIST ===
def load_alias_map(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"⚠️ Failed to load alias map: {e}")
        return {}

def load_cities(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception as e:
        logging.warning(f"⚠️ Failed to load cities list: {e}")
        return []

CITY_ALIASES = load_alias_map(CITY_ALIAS_PATH)
VALID_CITIES = load_cities(CITY_LIST_PATH)

# === GOOGLE SHEETS AUTH ===
encoded_credentials = os.getenv("GOOGLE_CREDENTIALS_BASE64")
if not encoded_credentials:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS_BASE64 env variable")

with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_cred_file:
    temp_cred_file.write(base64.b64decode(encoded_credentials))
    temp_cred_file_path = temp_cred_file.name

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_file(
    temp_cred_file_path, scopes=SCOPES
)
sheets_service = build("sheets", "v4", credentials=credentials)

# === FASTAPI APP ===
app = FastAPI()

# === HELPERS ===
def verify_shopify_webhook(data, hmac_header):
    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
        data,
        hashlib.sha256
    ).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)

def format_phone(phone: str) -> str:
    if not phone:
        return ""
    cleaned = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if cleaned.startswith("+212"):
        return "0" + cleaned[4:]
    elif cleaned.startswith("212"):
        return "0" + cleaned[3:]
    elif cleaned.startswith("0"):
        return cleaned
    return cleaned

def get_corrected_city(input_city, address_hint=""):
    city_clean = input_city.strip().lower()
    if city_clean in CITY_ALIASES:
        corrected = CITY_ALIASES[city_clean]
        return corrected, f"✅ Matched alias: '{input_city}' → '{corrected}'"
    matches = difflib.get_close_matches(city_clean, VALID_CITIES, n=1, cutoff=0.85)
    if matches:
        corrected = matches[0].title()
        return corrected, f"✅ Fuzzy matched: '{input_city}' → '{corrected}'"
    for city in VALID_CITIES:
        if city in address_hint.lower():
            return city.title(), f"✅ Guessed from address: '{input_city}' → '{city.title()}'"
    return input_city, f"🛑 Could not match: '{input_city}'"

def is_fulfilled(order_id, shop_domain, api_key, password):
    try:
        url = f"https://{api_key}:{password}@{shop_domain}/admin/api/2023-04/orders.json?name={order_id}"
        response = requests.get(url)
        orders = response.json().get("orders", [])
        return orders and orders[0].get("fulfillment_status") == "fulfilled"
    except Exception as e:
        logging.error(f"⚠️ Failed to fetch order {order_id} from {shop_domain}: {e}")
        return False

def sync_unfulfilled_rows(store):
    logging.info(f"🔍 Syncing unfulfilled rows for store: {store['name']}")
    spreadsheet_id = store["spreadsheet_id"]

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="Sheet1!A:L"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        return

    cache = {}
    last_api_call = 0
    rows_to_move = []

    for idx, row in list(enumerate(rows[1:], start=2)):
        order_id = row[1] if len(row) > 1 else ""
        current_status = row[11].strip().upper() if len(row) > 11 else ""

        if not order_id:
            continue

        if order_id in cache:
            shopify_order = cache[order_id]
        else:
            elapsed = time.time() - last_api_call
            if elapsed < 1:
                time.sleep(1 - elapsed)
            try:
                url = f"https://{store['api_key']}:{store['password']}@{store['shop_domain']}/admin/api/2023-04/orders.json?name={order_id}"
                response = requests.get(url)
                orders = response.json().get("orders", [])
                if not orders:
                    continue
                shopify_order = orders[0]
                cache[order_id] = shopify_order
                last_api_call = time.time()
            except Exception as e:
                continue

        shopify_status = ""
        if shopify_order.get("cancelled_at"):
            shopify_status = "CANCELLED"
        elif shopify_order.get("fulfillment_status") == "fulfilled":
            shopify_status = "FULFILLED"

        if shopify_status and shopify_status != current_status:
            update_range = f"Sheet1!L{idx}"
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=update_range,
                valueInputOption="USER_ENTERED",
                body={"values": [[shopify_status]]}
            ).execute()
            logging.info(f"✅ Updated {order_id} → {shopify_status}")

    # Sort rows by status: FULFILLED first, then unfulfilled
    sorted_rows = [rows[0]]  # Header
    fulfilled = []
    unfulfilled = []
    for row in rows[1:]:
        status = row[11].strip().upper() if len(row) > 11 else ""
        (fulfilled if status in ["FULFILLED", "CANCELLED"] else unfulfilled).append(row)

    sorted_rows += fulfilled + unfulfilled

    # Replace whole sheet
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range="Sheet1"
    ).execute()

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
        body={"values": sorted_rows}
    ).execute()

    logging.info(f"✅ Reordered {len(fulfilled)} fulfilled and {len(unfulfilled)} unfulfilled orders")

@app.post("/webhook/orders-updated")
async def webhook_orders_updated(
    request: Request,
    x_shopify_shop_domain: str = Header(None),
    x_shopify_hmac_sha256: str = Header(None)
):
    if not x_shopify_shop_domain or x_shopify_shop_domain not in SHOP_DOMAIN_TO_SHEET:
        raise HTTPException(status_code=400, detail="Unknown or missing shop domain")

    spreadsheet_id = SHOP_DOMAIN_TO_SHEET[x_shopify_shop_domain]
    body = await request.body()
    order = json.loads(body)

    order_id = order.get("name", "").strip()
    logging.info(f"🔔 Webhook received for order: {order_id}")

    tags_str = order.get("tags", "")
    current_tags = [t.strip().lower() for t in tags_str.split(",")]

    # === MARK EXISTING ROWS BASED ON STATUS OR TAG "ch" ===
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:K"
        ).execute()
        rows = result.get("values", [])
        for idx, row in enumerate(rows[1:], start=2):  # Start from row 2
            if len(row) > 1 and row[1] == order_id:
                status = ""
                if order.get("cancelled_at"):
                    status = "CANCELLED"
                elif order.get("fulfillment_status") == "fulfilled":
                    status = "FULFILLED"
                if status:
                    update_range = f"Sheet1!L{idx}"
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=update_range,
                        valueInputOption="USER_ENTERED",
                        body={"values": [[status]]}
                    ).execute()
                    logging.info(f"🎨 Updated row {order_id} → {status}")
                break
    except Exception as e:
        logging.error(f"❌ Failed to mark status for {order_id}: {e}")

    # === EXPORT ONLY IF: Has 'pc' tag + not fulfilled/cancelled + not already in sheet ===
    if TRIGGER_TAG not in current_tags:
        logging.info(f"🚫 Skipping {order_id} — no '{TRIGGER_TAG}' tag")
        return JSONResponse(content={"skipped": True})

    # Check current sheet to avoid duplicate export
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:L"
        ).execute()
        rows = result.get("values", [])
        existing_order_ids = set()
        for row in rows[1:]:  # Skip header
            if len(row) > 1:
                existing_order_ids.add(row[1].strip())
    except Exception as e:
        logging.error(f"❌ Failed to load existing orders: {e}")
        return JSONResponse(content={"error": "sheet read failed"})

    # If order already exists in the sheet, skip it
    if order_id.strip() in existing_order_ids:
        logging.info(f"⚠️ Order {order_id} already exists in sheet — skipping")
        return JSONResponse(content={"skipped": True})

    # Skip if fulfilled, cancelled, or closed
    if order.get("fulfillment_status") == "fulfilled" or order.get("cancelled_at") or order.get("closed_at"):
        logging.info(f"🚫 Order {order_id} is fulfilled/cancelled/closed — skipping")
        return JSONResponse(content={"skipped": True})


    # === EXPORT NEW ORDER ===
    try:
        created_at = datetime.strptime(order["created_at"], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M')
        shipping_address = order.get("shipping_address", {})
        shipping_name = shipping_address.get("name", "")
        shipping_phone = format_phone(shipping_address.get("phone", ""))
        shipping_address1 = shipping_address.get("address1", "")
        original_city = shipping_address.get("city", "")
        corrected_city, note = get_corrected_city(original_city, shipping_address1)
        if isinstance(corrected_city, list):
            corrected_city = str(corrected_city[0])  # Just take the first value
        total_price = order.get("total_outstanding") or order.get("presentment_total_price_set", {}).get("shop_money", {}).get("amount", "")
        notes = order.get("note", "")
        tags = order.get("tags", "")
        line_items = ", ".join([
            f"{item['quantity']}x {item.get('variant_title', item['title'])}"
            for item in order.get("line_items", [])
        ])

        row = [
            created_at,
            order_id,
            shipping_name,
            shipping_phone,
            shipping_address1,
            total_price,
            corrected_city,
            line_items,
            notes,
            tags,
            note
        ]
        row = (row + [""] * 12)[:12]

        # === Append the row ===
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]}
        ).execute()

        # === Force default (white) background for the newly inserted row ===
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A:L"
            ).execute()

            new_row_index = len(result.get("values", []))  # Index of last row added

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateCells": {
                                "range": {
                                    "sheetId": 0,
                                    "startRowIndex": new_row_index - 1,
                                    "endRowIndex": new_row_index
                                },
                                "fields": "userEnteredFormat"
                            }
                        }
                    ]
                }
            ).execute()
        except Exception as e:
            logging.warning(f"⚠️ Failed to clear formatting for new row: {e}")

        logging.info(f"✅ Exported order {order_id}")
    except Exception as e:
        logging.error(f"❌ Error exporting order {order_id}: {e}")

    # === SYNC OTHER UNMARKED FULFILLED ORDERS ===
    for store in STORES:
        if store["shop_domain"] == x_shopify_shop_domain:
            sync_unfulfilled_rows(store)
            logging.info(f"🔄 Finished syncing old orders for {store['name']}")

    return JSONResponse(content={"success": True})
