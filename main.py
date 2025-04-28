import json
import os
import base64
import tempfile
import requests
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
import urllib3

# Disable SSL Warnings (optional but Shopify API uses verified SSL in prod)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIG ===
TRIGGER_TAG = "pc"
EXTRACTED_TAG = "1"

SHOP_DOMAIN = "fdd92b-2e.myshopify.com"
SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY_IRRANOVA")
SHOPIFY_API_PASSWORD = os.getenv("SHOPIFY_PASSWORD_IRRANOVA")
GOOGLE_SHEET_ID = os.getenv("SHEET_IRRANOVA_ID")

# Google Sheets Setup
encoded_credentials = os.getenv("GOOGLE_CREDENTIALS_BASE64")
if not encoded_credentials:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS_BASE64 env variable")
with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_cred_file:
    temp_cred_file.write(base64.b64decode(encoded_credentials))
credentials = service_account.Credentials.from_service_account_file(temp_cred_file.name, scopes=["https://www.googleapis.com/auth/spreadsheets"])
sheets_service = build("sheets", "v4", credentials=credentials)

# FastAPI App
app = FastAPI()

# === HELPERS ===

def fetch_orders():
    url = f"https://{SHOP_DOMAIN}/admin/api/2023-07/orders.json?status=open&financial_status=any&fulfillment_status=unfulfilled"
    response = requests.get(url, auth=(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD), verify=False)
    response.raise_for_status()
    return response.json().get("orders", [])

def format_phone(phone):
    if not phone:
        return ""
    phone = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone.startswith("+212"):
        return "0" + phone[4:]
    elif phone.startswith("212"):
        return "0" + phone[3:]
    return phone

def format_price(price):
    try:
        return str(int(float(price)))
    except:
        return str(price)

def add_tag_to_order(order_id, existing_tags):
    if EXTRACTED_TAG not in existing_tags:
        new_tags = existing_tags + [EXTRACTED_TAG]
        tag_string = ", ".join(new_tags)
        update_url = f"https://{SHOP_DOMAIN}/admin/api/2023-07/orders/{order_id}.json"
        payload = {"order": {"id": order_id, "tags": tag_string}}
        response = requests.put(update_url, auth=(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD), json=payload, verify=False)
        response.raise_for_status()

def export_to_sheet(row_data):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_data]}
    ).execute()

# === MAIN HANDLER ===

@app.post("/webhook/orders-updated")
async def webhook_orders_updated(request: Request):
    orders = fetch_orders()

    for order in orders:
        # Skip orders missing key fields
        if not order.get("tags"):
            continue

        tags = [t.strip() for t in order["tags"].split(",")]
        if (TRIGGER_TAG in tags) and (EXTRACTED_TAG not in tags):
            financial_status = (order.get("financial_status") or "").lower()
            if financial_status not in ["paid", "pending", "unpaid"]:
                continue

            if order.get("cancelled_at") or order.get("closed_at"):
                continue

            # Prepare order info
            created_at = datetime.strptime(order["created_at"], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M")
            order_name = order.get("name", "")
            shipping = order.get("shipping_address", {})
            shipping_name = shipping.get("name", "")
            shipping_phone = format_phone(shipping.get("phone", ""))
            shipping_address1 = shipping.get("address1", "")
            city = shipping.get("city", "")
            raw_price = order.get("total_outstanding") or order.get("presentment_total_price_set", {}).get("shop_money", {}).get("amount", "")
            total_price = format_price(raw_price)
            line_items = ", ".join([f"{item['quantity']}x {item.get('variant_title', item['title'])}" for item in order.get("line_items", [])])

            # Export to Google Sheet
            row = [created_at, order_name, shipping_name, shipping_phone, shipping_address1, total_price, city, line_items]
            row = (row + [""] * 12)[:12]
            export_to_sheet(row)

            # Tag the order after successful export
            add_tag_to_order(order["id"], tags)

    return JSONResponse(content={"success": True})
