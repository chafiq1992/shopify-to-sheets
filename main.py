# irrakidsi-shopify-image/main.py (updated with resize and clearer price tag)

import os
import re
import hashlib
import requests
import json
from fastapi import FastAPI, Request
from PIL import Image, ImageDraw, ImageFont
from requests.utils import parse_header_links
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import uvicorn

# Shopify credentials
API_KEY = os.getenv("SHOPIFY_API_KEY", "your_api_key")
PASSWORD = os.getenv("SHOPIFY_PASSWORD", "your_password")
STORE_URL = os.getenv("SHOPIFY_STORE_URL", "https://yourstore.myshopify.com")

# Google Drive base folder
GDRIVE_BASE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "your-folder-id")
SERVICE_ACCOUNT_FILE = os.getenv("GDRIVE_CREDENTIALS", "service_account.json")

# Local base path
BASE_IMAGE_DIR = os.getenv("IRRAKIDS_IMAGE_DIR", "C:/Irrakids Stock")

app = FastAPI()

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)

folder_cache = {}
size_pattern = re.compile(r'\b(?:XS|S|M|L|XL|XXL|XXXL)\b|\d+')

def sanitize_directory_name(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_or_create_drive_folder(name, parent_id):
    key = f"{parent_id}/{name}"
    if key in folder_cache:
        return folder_cache[key]

    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])
    if items:
        folder_id = items[0]['id']
    else:
        metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = drive_service.files().create(body=metadata, fields='id').execute()
        folder_id = folder['id']
    folder_cache[key] = folder_id
    return folder_id

def delete_from_drive(size_folder, gender_folder, filename):
    try:
        size_id = get_or_create_drive_folder(size_folder, GDRIVE_BASE_FOLDER_ID)
        gender_id = get_or_create_drive_folder(gender_folder, size_id)
        query = f"name='{filename}' and '{gender_id}' in parents and trashed = false"
        results = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        for file in results.get('files', []):
            drive_service.files().delete(fileId=file['id']).execute()
            print(f"🗑️ Deleted {filename} from Drive")
    except Exception as e:
        print(f"⚠️ Error deleting {filename} from Drive: {e}")

def cleanup_drive_folders():
    try:
        size_folders = drive_service.files().list(q=f"'{GDRIVE_BASE_FOLDER_ID}' in parents and trashed = false and mimeType='application/vnd.google-apps.folder'", fields="files(id, name)").execute().get('files', [])
        for size_folder in size_folders:
            gender_folders = drive_service.files().list(q=f"'{size_folder['id']}' in parents and trashed = false and mimeType='application/vnd.google-apps.folder'", fields="files(id, name)").execute().get('files', [])
            for gender_folder in gender_folders:
                images = drive_service.files().list(q=f"'{gender_folder['id']}' in parents and trashed = false", fields="files(id, name)").execute().get('files', [])
                if not images:
                    drive_service.files().delete(fileId=gender_folder['id']).execute()
                    print(f"🗑️ Deleted empty gender folder {gender_folder['name']} under {size_folder['name']}")
            children = drive_service.files().list(q=f"'{size_folder['id']}' in parents and trashed = false", fields="files(id)").execute().get('files', [])
            if not children:
                drive_service.files().delete(fileId=size_folder['id']).execute()
                print(f"🗑️ Deleted empty size folder {size_folder['name']}")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")

def add_price_to_image(image_path, price):
    try:
        img = Image.open(image_path).convert("RGB").resize((800, 800))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("arial.ttf", 60)  # Larger font
        except IOError:
            font = ImageFont.load_default()
        price_text = f"{int(float(price))} DH"
        bbox = draw.textbbox((0, 0), price_text, font=font)
        text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x, y = img.width - text_width - 30, img.height - text_height - 30
        draw.rectangle([x - 15, y - 15, x + text_width + 15, y + text_height + 15], fill="#004AAD")
        draw.text((x, y), price_text, font=font, fill="white")
        img.save(image_path, "JPEG")
    except Exception as e:
        print(f"❌ Error adding price to image {image_path}: {e}")

def download_image_if_new(image_url, image_path):
    try:
        response = requests.get(image_url)
        image_data = response.content
        new_hash = hashlib.md5(image_data).hexdigest()
        if os.path.exists(image_path):
            with open(image_path, "rb") as f:
                existing_hash = hashlib.md5(f.read()).hexdigest()
            if existing_hash == new_hash:
                return False
        with open(image_path, "wb") as f:
            f.write(image_data)
        return True
    except:
        return False

def upload_to_drive_nested(file_path, size_folder, gender_folder, filename):
    try:
        size_folder_id = get_or_create_drive_folder(size_folder, GDRIVE_BASE_FOLDER_ID)
        gender_folder_id = get_or_create_drive_folder(gender_folder, size_folder_id)
        file_metadata = {
            'name': filename,
            'parents': [gender_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='image/jpeg')
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"☁️ Uploaded {filename} → {size_folder}/{gender_folder} [ID: {file.get('id')}]")
    except Exception as e:
        print(f"⚠️ Upload failed for {filename}: {e}")

def process_product(product):
    product_tags = product.get("tags", "").lower()
    is_girls = "girls" in product_tags
    is_boys = "boys" in product_tags

    for variant in product.get("variants", []):
        variant_id = variant.get("id")
        image_id = variant.get("image_id")
        inventory = variant.get("inventory_quantity", 0)
        price = variant.get("price", "0")
        option_values = [variant.get('option1', ''), variant.get('option2', ''), variant.get('option3', '')]
        size_option = next((sanitize_directory_name(v) for v in option_values if size_pattern.search(v)), "default")

        size_dir = os.path.join(BASE_IMAGE_DIR, size_option)
        girls_dir = os.path.join(size_dir, "girls") if is_girls else None
        boys_dir = os.path.join(size_dir, "boys") if is_boys else None

        for folder in filter(None, [girls_dir, boys_dir]):
            create_directory(folder)

        image_name = f"{variant_id}.jpg"
        image_url = next((img.get("src") for img in product.get("images", []) if img.get("id") == image_id), None)

        if inventory > 0 and image_url:
            for folder in filter(None, [girls_dir, boys_dir]):
                image_path = os.path.join(folder, image_name)
                changed = download_image_if_new(image_url, image_path)
                if changed or not os.path.exists(image_path):
                    add_price_to_image(image_path, price)
                    gender = os.path.basename(folder)
                    upload_to_drive_nested(image_path, size_option, gender, image_name)
                    print(f"✅ Updated variant {variant_id} at {image_path}")
        else:
            for folder in filter(None, [girls_dir, boys_dir]):
                image_path = os.path.join(folder, image_name)
                if os.path.exists(image_path):
                    os.remove(image_path)
                    print(f"❌ Removed out-of-stock variant image: {image_path}")
                    gender = os.path.basename(folder)
                    delete_from_drive(size_option, gender, image_name)

@app.post("/webhook")
async def webhook_listener(request: Request):
    payload = await request.json()
    print("🔔 Webhook received")
    if "products" in payload:
        for product in payload["products"]:
            process_product(product)
    elif "id" in payload:
        process_product(payload)
    cleanup_drive_folders()
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
