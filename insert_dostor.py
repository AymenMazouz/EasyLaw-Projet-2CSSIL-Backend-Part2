#
#          Insert Dostor Data
#

from elasticsearch import Elasticsearch
from es_index_settings import index_settings
import json
import dotenv
import os

dotenv.load_dotenv()

client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", os.environ.get("ES_PASSWORD")),
    verify_certs=False,
)

with open("data/dostor.json", encoding="utf-8") as file:
    data = json.load(file)

translations = {
    "رقم الباب": "section_number",
    "اسم الباب": "section_name",
    "رقم الفصل": "chapter_number",
    "اسم الفصل": "chapter_name",
    "رقم المادة": "article_number",
    "نص المادة": "article_text",
}

# configure mappings
mappings = {
    "mappings": {
        "properties": {
            "article_number": {"type": "long"},
            "article_text": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "chapter_name": {"type": "keyword", "ignore_above": 256},
            "chapter_number": {"type": "long"},
            "section_name": {"type": "keyword", "ignore_above": 256},
            "section_number": {"type": "long"},
        }
    }
}
index_settings["mappings"] = mappings["mappings"]


def translate(object):
    result = {}
    for key in object:
        if key in translations:
            result[translations[key]] = object[key]
        else:
            raise Exception(f"Unknown key: {key}")
    return result


# clear data
# client.indices.delete(index="dostor")


client.indices.create(index="dostor", body=index_settings)
for article in data:
    processed_article = translate(article)
    client.index(index="dostor", document=processed_article)

client.indices.refresh(index="dostor")
print("Data indexed successfully!")
