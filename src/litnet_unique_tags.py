from src.log_bootstrap import setup_logging, get_logger
setup_logging()
logger = get_logger("scraper.litnet_unique_tags")

import gzip
import xml.etree.ElementTree as ET

xml = 'data/raw/all.xml.gz'
logger.info("Старт анализа уникальных тегов/атрибутов в файле: %s", xml)

with gzip.open(xml, 'rb') as f:
    tree = ET.parse(f)
    root = tree.getroot()
logger.info("XML распакован и разобран. Корневой тег: %s", root.tag)

unique_tags = set()
unique_attrs = set()

def traverse(element):
    unique_tags.add(element.tag)
    for attr in element.attrib:
        unique_attrs.add(attr)
    for child in element:
        traverse(child)

traverse(root)
logger.info("Собраны множества: %d уникальных тегов, %d уникальных атрибутов",
            len(unique_tags), len(unique_attrs))

print("Все уникальные теги:")
for tag in sorted(unique_tags):
    print(tag)

print("\nВсе уникальные атрибуты:")
for attr in sorted(unique_attrs):
    print(attr)

logger.info("Вывод списков в stdout завершён. Тегов: %d, атрибутов: %d",
            len(unique_tags), len(unique_attrs))
