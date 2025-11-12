from config.src.log_bootstrap import setup_logging, get_logger
setup_logging()
logger = get_logger("scraper.litnet_xml")

import gzip
import xml.etree.ElementTree as ET
import pandas as pd
import json
import os

os.makedirs("data/raw", exist_ok=True)
logger.info("Гарантирована папка для данных: data/raw")

gz_path = "data/raw/all.xml.gz"
max_books = 5000
books = []

title_tags = ["title", "name"]
author_tags = ["author"]
genre_tags = ["genre", "genres_list", "category", "categories"]
price_tags = ["price"]
year_tags = ["year"]
description_tags = ["description"]
age_tags = ["age"]

genre_mapping = {}

logger.info("Начинаю одно проходное сканирование для построения словаря жанров из %s", gz_path)
with gzip.open(gz_path, "rt", encoding="utf-8") as f:
    context = ET.iterparse(f, events=("end",))
    for event, elem in context:
        if elem.tag in ("category", "genre"):
            genre_id = elem.attrib.get("id")
            genre_name = elem.text.strip() if elem.text else None
            if genre_id and genre_name:
                genre_mapping[genre_id] = genre_name
        elem.clear()

print(f"Найдено уникальных жанров: {len(genre_mapping)}")
logger.info("Найдено уникальных жанров: %d", len(genre_mapping))

def get_first_text(elem, tags):
    for tag in tags:
        child = elem.find(tag)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return None

logger.info("Начинаю выборку книг (максимум %d) из %s", max_books, gz_path)
with gzip.open(gz_path, "rt", encoding="utf-8") as f:
    context = ET.iterparse(f, events=("end",))
    for event, elem in context:
        if elem.tag == "offer":
            genre_text = get_first_text(elem, genre_tags)
            genre_names = []
            if genre_text:
                for gid in genre_text.split(","):
                    gid = gid.strip()
                    if gid in genre_mapping:
                        genre_names.append(genre_mapping[gid])

            book = {
                "id": elem.attrib.get("id"),
                "available": elem.attrib.get("available"),
                "rate": elem.attrib.get("rate"),
                "title": get_first_text(elem, title_tags),
                "author": get_first_text(elem, author_tags),
                "genre": ", ".join(genre_names) if genre_names else None,
                "price": get_first_text(elem, price_tags),
                "year": get_first_text(elem, year_tags),
                "description": get_first_text(elem, description_tags),
                "age": get_first_text(elem, age_tags)
            }
            books.append(book)

            # Периодическая диагностика в логах (не влияет на логику)
            if len(books) % 500 == 0:
                logger.info("Собрано записей: %d (последняя: id=%r, title=%r)", len(books), book.get("id"), book.get("title"))

            if len(books) >= max_books:
                logger.info("Достигнут лимит выборки: %d", max_books)
                break
            elem.clear()

print(f"Выбрано книг: {len(books)}")
logger.info("Итоговая выборка: %d книг", len(books))

json_path = "data/raw/litnet_books.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(books, f, ensure_ascii=False, indent=2)
logger.info("Сохранён JSON: %s (записей: %d)", json_path, len(books))

csv_path = "data/raw/litnet_books_dataset.csv"
df = pd.DataFrame(books)
df.to_csv(csv_path, index=False, encoding="utf-8")
logger.info("Сохранён CSV: %s (строк: %d, столбцов: %d)", csv_path, len(df), df.shape[1])
