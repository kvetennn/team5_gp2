import gzip
import xml.etree.ElementTree as ET

xml = 'data/raw/all.xml.gz'

with gzip.open(xml, 'rb') as f:
    tree = ET.parse(f)
    root = tree.getroot()

unique_tags = set()
unique_attrs = set()

def traverse(element):
    unique_tags.add(element.tag)
    for attr in element.attrib:
        unique_attrs.add(attr)
    for child in element:
        traverse(child)

traverse(root)

print("Все уникальные теги:")
for tag in sorted(unique_tags):
    print(tag)

print("\nВсе уникальные атрибуты:")
for attr in sorted(unique_attrs):
    print(attr)
