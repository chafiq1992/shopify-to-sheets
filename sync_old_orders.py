import os
import base64
import json
import requests
import logging
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build

# === STORES CONFIG ===
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

# === GOOGLE SHEETS AUTH ===
encoded_credentials = os.getenv("GOOGLE_CREDENTIALS_BASE64")
if not encoded_credentials:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS_BASE64")

with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_cred_file:
    temp_cred_file.write(base64.b64decode(encoded_credentials))
    creds_path = temp_cred_file.name

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_file(
    creds_path, scopes=SCOPES
)
sheets_service = build("sheets", "v4", credentials=credentials)

# === COLOR FUNCTION ===
def apply_green_background(sheet_id, row_index):
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": 0,
                        "startRowIndex": row_index - 1,
                        "endRowIndex": row_index,
                        "startColumnIndex": 0,
                        "endColumnIndex": 12
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.8,
                                "green": 1.0,
                                "blue": 0.8
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            }
        ]
    }
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body=body
    ).execute()

# === SHOPIFY FULFILLMENT CHECK ===
def is_fulfilled(order_id, shop_domain, api_key, password):
    try:
        url = f"https://{api_key}:{password}@{shop_domain}/admin/api/2023-04/orders.json?name={order_id}"
        response = requests.get(url)
        orders = response.json().get("orders", [])
        return orders and orders[0].get("fulfillment_status") == "fulfilled"
    except Exception as e:
        logging.error(f"⚠️ Failed to fetch {order_id} from {shop_domain}: {e}")
        return False

# === MAIN FUNCTION ===
def sync_fulfilled_orders(store):
    print(f"\n📦 Syncing store: {store['name']}")

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=store["spreadsheet_id"],
        range="Sheet1!A:L"
    ).execute()

    rows = result.get("values", [])
    if not rows:
        print("⚠️ Sheet is empty.")
        return

    for idx, row in enumerate(rows[1:], start=2):  # Skip header
        order_id = row[1] if len(row) > 1 else ""
        col_l = row[11].strip().upper() if len(row) > 11 else ""

        if order_id and col_l != "FULFILLED":
            print(f"🔄 Checking order {order_id} (Row {idx})...")
            if is_fulfilled(order_id, store["shop_domain"], store["api_key"], store["password"]):
                update_range = f"Sheet1!L{idx}"
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=store["spreadsheet_id"],
                    range=update_range,
                    valueInputOption="USER_ENTERED",
                    body={"values": [["FULFILLED"]]}
                ).execute()
                apply_green_background(store["spreadsheet_id"], idx)
                print(f"✅ Order {order_id} marked and colored as FULFILLED.")
            else:
                print(f"🕒 Order {order_id} still unfulfilled.")

if __name__ == "__main__":
    for store in STORES:
        sync_fulfilled_orders(store)
