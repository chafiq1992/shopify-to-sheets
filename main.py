import json
import os
import base64
import tempfile
import logging
import requests
from datetime import datetime
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
import certifi
import sqlite3

# === CONFIG ===
TRIGGER_TAG = "pc"
EXTRACTED_TAG = "1"
SHOP_DOMAIN_TO_SHEET = {
    "fdd92b-2e.myshopify.com": os.getenv("SHEET_IRRANOVA_ID")
}

STORES = [
    {
        "name": "irranova",
        "spreadsheet_id": os.getenv("SHEET_IRRANOVA_ID"),
        "shop_domain": "fdd92b-2e.myshopify.com",
        "api_key": os.getenv("SHOPIFY_API_KEY_IRRANOVA"),
        "password": os.getenv("SHOPIFY_PASSWORD_IRRANOVA")
    }
]

# === DATABASE INIT ===
DB_FILE = "orders.db"


# === LOGGER ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", force=True)

# === GOOGLE SHEETS AUTH ===
encoded_credentials = os.getenv("GOOGLE_CREDENTIALS_BASE64")
if not encoded_credentials:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS_BASE64 env variable")

with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_cred_file:
    temp_cred_file.write(base64.b64decode(encoded_credentials))
    temp_cred_file_path = temp_cred_file.name

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_file(temp_cred_file_path, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=credentials)

# === FASTAPI APP ===
app = FastAPI()

# === HELPERS ===

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            order_id TEXT UNIQUE,
            shipping_name TEXT,
            shipping_phone TEXT,
            shipping_address1 TEXT,
            total_price TEXT,
            city TEXT,
            line_items TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()


def format_price(price):
    try:
        return str(int(float(price)))
    except:
        return str(price)

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

def add_tag_to_order(order_id, store):
    try:
        # 🛠 Correct URL directly by ID
        url = f"https://{store['api_key']}:{store['password']}@{store['shop_domain']}/admin/api/2023-04/orders/{order_id}.json"
        response = requests.get(url, verify=certifi.where())
        order = response.json().get("order")

        if not order:
            logging.error(f"❌ Order {order_id} not found")
            return

        current_tags = order.get("tags", "")
        tag_list = [tag.strip() for tag in current_tags.split(",") if tag.strip()]

        if EXTRACTED_TAG not in tag_list:
            tag_list.append(EXTRACTED_TAG)

            update_url = f"https://{store['api_key']}:{store['password']}@{store['shop_domain']}/admin/api/2023-04/orders/{order_id}.json"
            payload = {
                "order": {
                    "id": order["id"],
                    "tags": ", ".join(tag_list)
                }
            }
            update_response = requests.put(update_url, json=payload, verify=certifi.where())

            if update_response.status_code == 200:
                logging.info(f"✅ Added tag '1' to order {order_id}")
            else:
                logging.error(f"❌ Failed to add tag to {order_id}: {update_response.text}")
        else:
            logging.info(f"ℹ️ Tag '1' already exists for order {order_id}, no update needed.")

    except Exception as e:
        logging.error(f"❌ Exception while tagging order {order_id}: {e}")


@app.post("/webhook/orders-updated")
async def webhook_orders_updated(request: Request):
    body = await request.body()
    order = json.loads(body)
    order_id = str(order.get("name", "")).strip()
    logging.info(f"🔔 Webhook received for order: {order_id}")

    fulfillment_status = (order.get("fulfillment_status") or "").lower()
    cancelled = order.get("cancelled_at")
    closed = order.get("closed_at")
    financial_status = (order.get("financial_status") or "").lower()
    tags_str = order.get("tags", "")
    tags = [t.strip().lower() for t in tags_str.split(",")]

    if (
        fulfillment_status != "fulfilled" and
        not cancelled and
        not closed and
        financial_status in ["paid", "pending", "unpaid"] and
        TRIGGER_TAG in tags and
        EXTRACTED_TAG not in tags
    ):
        logging.info(f"✅ Order {order_id} passed filters — exporting and tagging...")

        try:
            # Extract order fields
            spreadsheet_id = SHOP_DOMAIN_TO_SHEET["fdd92b-2e.myshopify.com"]

            created_at = datetime.strptime(order["created_at"], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M')
            shipping_address = order.get("shipping_address", {})
            shipping_name = shipping_address.get("name", "")
            shipping_phone = format_phone(shipping_address.get("phone", ""))
            shipping_address1 = shipping_address.get("address1", "")
            city = shipping_address.get("city", "")
            raw_price = order.get("total_outstanding") or order.get("presentment_total_price_set", {}).get("shop_money", {}).get("amount", "")
            total_price = format_price(raw_price)
            line_items = ", ".join([
                f"{item['quantity']}x {item.get('variant_title', item['title'])}"
                for item in order.get("line_items", [])
            ])

            # Save to Google Sheets
            row = [
                created_at,
                order_id,
                shipping_name,
                shipping_phone,
                shipping_address1,
                total_price,
                city,
                line_items
            ]
            row = (row + [""] * 12)[:12]

            sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]}
            ).execute()

            # Save to SQLite
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO orders (
                    created_at, order_id, shipping_name, shipping_phone,
                    shipping_address1, total_price, city, line_items
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (created_at, order_id, shipping_name, shipping_phone, shipping_address1, total_price, city, line_items))
            conn.commit()
            conn.close()
            logging.info(f"✅ Order {order_id} saved to database")

            # Tag order
            store = STORES[0]
            add_tag_to_order(order_id, store)

        except Exception as e:
            logging.error(f"❌ Failed to export order {order_id}: {e}")

    else:
        logging.info(f"🚫 Order {order_id} skipped — conditions not met")

    return JSONResponse(content={"success": True})
