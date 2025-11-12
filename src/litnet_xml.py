import requests
import gzip
import os

token = "hvk0E_1RzMAF1xZZzifVSTXk-k2CIJ-_"
url = f"https://api.litnet.com/v1/feeds/get-all?user_token={token}"
os.makedirs("data/raw", exist_ok=True)

resp = requests.get(url)
data = resp.json()
xml_url = data["books"]
print(f"Ссылка на XML: {xml_url}")

gz_path = "data/raw/all.xml.gz"
r = requests.get(xml_url)
with open(gz_path, "wb") as f:
    f.write(r.content)
print(f"Скачан архив: {gz_path}")

xml_path = "data/raw/all.xml"
with gzip.open(gz_path, "rb") as f_in:
    with open(xml_path, "wb") as f_out:
        f_out.write(f_in.read())
print(f"Распакован XML: {xml_path}")
