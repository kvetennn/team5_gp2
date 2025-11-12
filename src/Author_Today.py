from src.log_bootstrap import setup_logging, get_logger
setup_logging()
logger = get_logger("scraper.author_today")

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import pandas as pd

def parse_author_today():
    base_url = "https://author.today/work/genre/all/ebook"
    params = {
        'sorting': 'likes',
        'state': 'finished', 
        'eg': '',
        'fnd': 'false'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    logger.info("Старт парсинга Author.Today: base_url=%s, sorting=%s, state=%s", base_url, params['sorting'], params['state'])

    all_books = []
    page = 1
    max_pages = 400
    
    
    while page <= max_pages and len(all_books) < 10000:
        print(f"Страница {page}...")
        logger.info("Обрабатываю страницу %d (накоплено записей: %d)", page, len(all_books))
        
        try:
            params['page'] = page
            logger.debug("HTTP GET %s, params=%s", base_url, params)
            response = requests.get(base_url, params=params, headers=headers, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            cards = soup.find_all('div', class_='book-row')
            logger.debug("Найдено карточек на странице: %d", 0 if not cards else len(cards))
            
            if not cards:
                logger.info("Карточек не найдено — останавливаюсь на странице %d", page)
                break
                
            for card in cards:
                book_data = extract_book_data(card)
                if book_data['title']:
                    all_books.append(book_data)
            
            logger.info("Страница %d обработана. Всего записей: %d", page, len(all_books))
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Ошибка: {e}")
            logger.exception("Ошибка на странице %d: %s", page, e)
            break
    
    if all_books:
        filename = f'author_today_books_{len(all_books)}.csv'
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['title', 'authors', 'categories', 'date', 'symbols_count', 'a4_sheets', 
                         'views', 'likes', 'comments', 'reviews', 'price', 'cycle', 'exclusive', 'annotation']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_books)
        
        print(f"Готово! Собрано {len(all_books)} книг")
        print(f"Файл: {filename}")
        logger.info("Сохранён CSV: %s (строк: %d)", filename, len(all_books))
    else:
        logger.warning("Данные не собраны — all_books пуст")

    logger.info("Завершение парсинга Author.Today. Итоговое количество: %d", len(all_books))
    return all_books

def extract_book_data(card):
    title_elem = card.select_one('.book-title a')
    title = title_elem.get_text(strip=True) if title_elem else ""
    
    author_elems = card.select('.book-author a')
    authors = ', '.join([a.get_text(strip=True) for a in author_elems]) if author_elems else ""
    
    category_elems = card.select('.book-genres a')
    categories = ', '.join([cat.get_text(strip=True) for cat in category_elems]) if category_elems else ""
    
    date_elem = card.select_one('[data-time]')
    date = date_elem.get('data-time', '') if date_elem else ""
    
    details_text = card.get_text()
    symbols_match = re.search(r'(\d[\d\s]*)зн', details_text)
    symbols_count = symbols_match.group(1).replace(' ', '').replace('\xa0', '') if symbols_match else ""
    
    sheets_match = re.search(r'(\d+,\d+)\s*а\.л', details_text)
    a4_sheets = sheets_match.group(1) if sheets_match else ""
    
    views_elem = card.select_one('.book-stats span:has(i.icon-eye)')
    views = views_elem.get_text(strip=True) if views_elem else ""
    
    likes_elem = card.select_one('.like-count')
    likes = likes_elem.get_text(strip=True) if likes_elem else ""
    
    comments_elem = card.select_one('.book-stats a[href*="#comments"]')
    comments = comments_elem.get_text(strip=True) if comments_elem else ""
    
    reviews_elem = card.select_one('.book-stats a[href*="/reviews"]')
    reviews = reviews_elem.get_text(strip=True) if reviews_elem else ""
    
    price_elem = card.select_one('.text-bold.text-success')
    price = price_elem.get_text(strip=True) if price_elem else ""
    if not price and 'Свободный доступ' in details_text:
        price = 'Бесплатно'
    
    cycle_elem = card.select_one('a[href*="/work/series/"]')
    cycle = cycle_elem.get_text(strip=True) if cycle_elem else ""
    
    exclusive_text = card.get_text()
    exclusive = "Да" if "Эксклюзив" in exclusive_text else "Нет"
    
    annotation_elem = card.select_one('.annotation')
    annotation = annotation_elem.get_text(strip=True) if annotation_elem else ""
    annotation = ' '.join(annotation.split())
    
    # Доп. диагностический лог на уровне DEBUG
    logger.debug("Книга распознана: title=%r, authors=%r", title, authors)

    return {
        'title': title,
        'authors': authors,
        'categories': categories,
        'date': date,
        'symbols_count': symbols_count,
        'a4_sheets': a4_sheets,
        'views': views,
        'likes': likes,
        'comments': comments,
        'reviews': reviews,
        'price': price,
        'cycle': cycle,
        'exclusive': exclusive,
        'annotation': annotation
    }

books = parse_author_today()
