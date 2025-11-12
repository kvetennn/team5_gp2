from config.src.log_bootstrap import setup_logging, get_logger
setup_logging()
logger = get_logger("scraper.litnet_api")

import requests
import gzip
import os

token = "hvk0E_1RzMAF1xZZzifVSTXk-k2CIJ-_"
url = f"https://api.litnet.com/v1/feeds/get-all?user_token={token}"
os.makedirs("data/raw", exist_ok=True)
logger.info("Старт запроса к API Litnet: endpoint=/v1/feeds/get-all token=%s", token[:4] + "…")

resp = requests.get(url)
logger.info("Ответ API получен: status=%s, headers_len=%d", getattr(resp, "status_code", "?"), len(resp.headers or {}))

data = resp.json()
xml_url = data["books"]
print(f"Ссылка на XML: {xml_url}")
logger.info("Получена ссылка на XML из JSON: %s", xml_url)

gz_path = "data/raw/all.xml.gz"
r = requests.get(xml_url)
logger.info("Загрузка архива с XML: %s; status=%s", xml_url, getattr(r, "status_code", "?"))

with open(gz_path, "wb") as f:
    f.write(r.content)
logger.info("Скачан архив: %s (размер: %d байт)", gz_path, len(r.content or b""))

print(f"Скачан архив: {gz_path}")

xml_path = "data/raw/all.xml"
with gzip.open(gz_path, "rb") as f_in:
    with open(xml_path, "wb") as f_out:
        f_out.write(f_in.read())
logger.info("Архив распакован в файл: %s", xml_path)

print(f"Распакован XML: {xml_path}")
