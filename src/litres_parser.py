from config.src.log_bootstrap import setup_logging, get_logger
setup_logging()
logger = get_logger("scraper.litres")

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin, urlparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# общие условия
START_URL = "https://www.litres.ru/popular/?art_types=text_book&only_selfpublished_arts=true"
MAX_PAGES = 5
MAX_BOOKS = 15000
OUTPUT_CSV = "litres_books_parallel.csv"
DELAY_BETWEEN_REQUESTS = 0.3
NUM_THREADS = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# загружаю html страницы по url
def get_page(url: str) -> str | None:
    try:
        logger.debug("HTTP GET %s", url)
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.error("GET failed for %s: %s", url, e, exc_info=True)
        return None

def extract_book_links_from_catalog(html: str, base_url: str) -> set[str]:
    soup = BeautifulSoup(html, "lxml")
    links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        p = urlparse(full)
        if "litres.ru" in p.netloc and re.search(r"/book(?:/|$)", p.path):
            clean = p.scheme + "://" + p.netloc + p.path
            links.add(clean)
    logger.debug("Extracted %d links from %s", len(links), base_url)
    return links

def parse_book_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    data = {
        "url": url,
        "title": None,
        "authors": None,
        "rating": None,
        "rating_count": None,
        "reviews_count": None,
        "price": None,
        "genres": None,
        "age_limit": None,
        "release_date": None,
        "written_date": None,
        "pages": None,
        "isbn": None,
        "copyright_holder": None,
        "formats": None,
        "description": None,
    }

    # ищу все скрипты с типом application/ld+json
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            jd = json.loads(script.string)
        except Exception:
            continue

        if isinstance(jd, list):
            jd = next((obj for obj in jd if obj.get("@type") == "Book"), None)
        elif jd.get("@type") != "Book":
            jd = None

        if jd:
            data["title"] = jd.get("name")
            author = jd.get("author")
            if isinstance(author, list):
                data["authors"] = ", ".join(a.get("name", "") for a in author)
            elif isinstance(author, dict):
                data["authors"] = author.get("name")
            agg = jd.get("aggregateRating")
            if agg:
                data["rating"] = agg.get("ratingValue")
                data["rating_count"] = agg.get("ratingCount")
            offers = jd.get("offers")
            if isinstance(offers, dict):
                data["price"] = offers.get("price")

    # жанры
    genres = soup.select("a[data-test='book-genre-link'], a[href*='/genre/']")
    if genres:
        data["genres"] = ", ".join(g.get_text(strip=True) for g in genres)

    # количество оценок
    marks_el = soup.select_one('[data-testid="book-factoids__marks"]')
    if marks_el:
        text = marks_el.get_text(strip=True)
        match = re.search(r'\d+', text.replace(" ", ""))
        if match:
            data["rating_count"] = int(match.group())

    # количество отзывов
    reviews_el = soup.select_one('[data-testid="book-factoids__reviews"] span')
    if reviews_el:
        text = reviews_el.get_text(strip=True)
        match = re.search(r'\d+', text.replace(" ", ""))
        if match:
            data["reviews_count"] = int(match.group())

    # доп. характеристики
    char_block = soup.select_one('div[data-testid="book-characteristics__wrapper"]')
    if char_block:
        for item in char_block.select('div.ddd308de'):
            label = item.select_one("div.ae1c618c span")
            value_span = item.find_all("span")[-1] if item.find_all("span") else None
            value = value_span.get_text(strip=True) if value_span else None
            if not label or not value:
                continue
            label_text = label.get_text(strip=True)

            if "Возрастное ограничение" in label_text:
                data["age_limit"] = value
            elif "Дата выхода на Литрес" in label_text:
                data["release_date"] = value
            elif "Дата написания" in label_text:
                data["written_date"] = value
            elif "Объем" in label_text:
                data["pages"] = value
            elif "ISBN" in label_text:
                data["isbn"] = value
            elif "Правообладатель" in label_text:
                value = item.find_all("span")[-1].get_text(strip=True)
                data["copyright_holder"] = value
            elif "Формат скачивания" in label_text:
                formats = [a.get_text(strip=True) for a in item.select("a")]
                data["formats"] = ", ".join(formats)

    # описание
    desc = None

    description_block = soup.select_one('div[data-testid="book-description__text"]')
    if description_block:
        desc = description_block.get_text(" ", strip=True)

    if not desc:
        alt_desc_block = soup.select_one('div._86af713b')
        if alt_desc_block:
            paragraphs = alt_desc_block.find_all("p")
            if paragraphs:
                desc = " ".join(p.get_text(" ", strip=True) for p in paragraphs)

    if not desc:
        long_desc_block = soup.select_one('div.ac83cc29')
        if long_desc_block:
            paragraphs = long_desc_block.find_all("p")
            if paragraphs:
                desc = " ".join(p.get_text(" ", strip=True) for p in paragraphs)

    if desc:
        desc = re.sub(r'(Далее|Свернуть)\s*$', '', desc).strip()

    data["description"] = desc

    logger.debug("Parsed book: title=%r url=%s", data["title"], url)
    return data


logger.info(
    "Старт парсинга каталога Литрес: start_url=%s, max_pages=%d, max_books=%d, threads=%d",
    START_URL, MAX_PAGES, MAX_BOOKS, NUM_THREADS
)

all_book_links: set[str] = set()

# собираю ссылки на книги со страниц
for p in range(1, MAX_PAGES + 1):
    page_url = f"{START_URL}&page={p}"
    html = get_page(page_url)
    if not html:
        logger.warning("Не удалось получить страницу каталога: %s", page_url)
        continue

    links = extract_book_links_from_catalog(html, page_url)
    print(f"\nСтраница {p}: найдено {len(links)} ссылок, всего уникальных: {len(all_book_links) + len(links)}")
    logger.info("Каталог p=%d: найдено %d ссылок (накоплено уникальных ~%d)", p, len(links), len(all_book_links) + len(links))

    all_book_links.update(links)
    time.sleep(DELAY_BETWEEN_REQUESTS)
    if len(all_book_links) >= MAX_BOOKS:
        logger.info("Достигнут лимит ссылок MAX_BOOKS=%d — остановка сбора каталога", MAX_BOOKS)
        break

all_book_links = list(all_book_links)[:MAX_BOOKS]
print(f"\nВсего уникальных ссылок для парсинга: {len(all_book_links)}")
logger.info("Итого к парсингу: %d ссылок", len(all_book_links))

# параллельный парсинг книг
collected: list[dict] = []

def fetch_book(url: str) -> dict | None:
    html = get_page(url)
    if not html:
        logger.warning("Пропущена книга (нет html): %s", url)
        return None
    try:
        return parse_book_page(html, url)
    except Exception as e:
        logger.exception("Ошибка парсинга книги %s: %s", url, e)
        return None

with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = {executor.submit(fetch_book, url): url for url in all_book_links}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            collected.append(result)
        if i % 50 == 0:
            logger.info("Пройдено %d/%d книг, сохранено %d (временный сейв)", i, len(all_book_links), len(collected))
            pd.DataFrame(collected).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

df = pd.DataFrame(collected)
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
logger.info("Готово. Сохранён CSV: %s (строк: %d)", OUTPUT_CSV, len(df))
print(df.head())
