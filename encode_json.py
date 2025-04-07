import base64

with open("pristine-lodge-383303-cde3fcdc0a72.json", "rb") as f:
    encoded = base64.b64encode(f.read()).decode("utf-8")

with open("encoded.txt", "w") as f:
    f.write(encoded)

print("✅ Credentials encoded successfully to encoded.txt")
