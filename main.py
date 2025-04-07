import json
import os
import hashlib
import hmac
import base64
import tempfile
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build

# === CONFIG ===
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
SHEET_RANGE = "Sheet1!A2:G"
SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "")

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

# === HELPER FUNCTION ===
def verify_shopify_webhook(data, hmac_header):
    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
        data,
        hashlib.sha256
    ).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)

# === PAYLOAD MODEL ===
class ShopifyOrderWebhook(BaseModel):
    id: int
    created_at: str
    tags: str
    customer: dict

# === WEBHOOK ENDPOINT ===
@app.post("/webhook/orders-updated")
async def webhook_orders_updated(
    request: Request,
    x_shopify_hmac_sha256: str = Header(None)
):
    body = await request.body()

    # 🔒 Uncomment to enable webhook security
    # if not verify_shopify_webhook(body, x_shopify_hmac_sha256):
    #     raise HTTPException(status_code=401, detail="Invalid HMAC")

    data = json.loads(body)
    order = ShopifyOrderWebhook(**data)

    tag_list = [t.strip().lower() for t in order.tags.split(",")]

    if "vip" in tag_list:
        row = [
            order.customer.get("first_name", ""),
            order.customer.get("last_name", ""),
            order.customer.get("email", ""),
            order.customer.get("phone", ""),
            order.id,
            order.tags,
            order.created_at,
        ]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_RANGE,
            valueInputOption="USER_ENTERED",
            body={"values": [row]}
        ).execute()
        print("✅ Row added to Google Sheet:", row)

    return JSONResponse(content={"success": True})

# === MANUAL TRIGGER (OPTIONAL) ===
@app.get("/export-customers")
async def manual_export():
    return {"message": "Manual export not implemented yet"}

# === HEALTH CHECK ENDPOINT ===
@app.get("/ping")
async def ping():
    return {"status": "ok"}
