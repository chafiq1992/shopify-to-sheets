import json
import os
import hashlib
import hmac
import base64
import tempfile
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

# === GOOGLE SHEETS AUTH FROM BASE64 ENV ===
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
    if phone.startswith("+212"):
        return "0" + phone[4:]
    elif phone.startswith("212"):
        return "0" + phone[3:]
    return phone

def delete_row_by_order_id(spreadsheet_id, order_id):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="Sheet1!A:J"
    ).execute()

    rows = result.get("values", [])
    if not rows:
        print("⚠️ Sheet is empty.")
        return

    header = rows[0]
    data_rows = rows[1:]
    new_data_rows = []
    found = False

    for row in data_rows:
        if len(row) > 1 and row[1] == order_id:
            found = True
            continue
        new_data_rows.append(row)

    if found:
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A2:J"
        ).execute()

        if new_data_rows:
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A2",
                valueInputOption="USER_ENTERED",
                body={"values": new_data_rows}
            ).execute()

        print(f"🗑️ Deleted row for order ID: {order_id}")
    else:
        print(f"⚠️ No row found for order ID: {order_id}")

# === WEBHOOK ENDPOINT ===
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

    # Uncomment when going live
    # if not verify_shopify_webhook(body, x_shopify_hmac_sha256):
    #     raise HTTPException(status_code=401, detail="Invalid HMAC")

    order = json.loads(body)
    tag_list = [t.strip().lower() for t in order.get("tags", "").split(",")]
    order_id = order.get("name", "")

    if TRIGGER_TAG in tag_list:
        try:
            created_at = datetime.strptime(order["created_at"], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M')
            shipping_address = order.get("shipping_address", {})

            shipping_name = shipping_address.get("name", "")
            shipping_phone = format_phone(shipping_address.get("phone", ""))
            shipping_address1 = shipping_address.get("address1", "")
            shipping_city = shipping_address.get("city", "")
            total_price = order.get("total_price", "")
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
                shipping_city,
                total_price,
                line_items,
                notes,
                tags
            ]
            row = (row + [""] * 10)[:10]  # Always A–J columns

            existing_orders = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A:J"
            ).execute().get("values", [])

            order_ids = [r[1] for r in existing_orders[1:] if len(r) > 1]

            if order_id in order_ids:
                print(f"⚠️ Order {order_id} already exists — skipping. Store: {x_shopify_shop_domain}")
            else:
                sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="Sheet1!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row]}
                ).execute()
                print(f"✅ Row added to {x_shopify_shop_domain}:", row)

        except Exception as e:
            print("❌ Error processing order:", e)

    else:
        delete_row_by_order_id(spreadsheet_id, order_id)

    return JSONResponse(content={"success": True})

# === HEALTH CHECK ENDPOINT ===
@app.get("/ping")
async def ping():
    return {"status": "ok"}
