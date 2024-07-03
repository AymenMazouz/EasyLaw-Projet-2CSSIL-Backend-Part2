#
#           Insert Conseil d'etat Data
#

from elasticsearch import Elasticsearch
import json
from es_index_settings import index_settings
import os
import dotenv

dotenv.load_dotenv()

client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", os.environ.get("ES_PASSWORD")),
    verify_certs=False,
)

with open("data/conseil_etat.json", encoding="utf-8") as file:
    data = json.load(file)

translations = {
    "رقم القرار": "number",
    "الغرفة": "chamber",
    "القسم": "section",
    "تاريخ القرار": "date",
    "التكييف": "procedure",
    "الموضوع": "subject",
    "المبدأ": "principle",
    "رابط القرار PDF": "pdf_link",
}


mappings = {
    "properties": {
        "chamber": {"type": "keyword", "ignore_above": 256},
        "date": {"type": "date"},
        "number": {"type": "long"},
        "pdf_link": {
            "type": "text",
            "index": False,
        },
        "principle": {
            "type": "text",
        },
        "procedure": {"type": "keyword", "ignore_above": 256},
        "section": {"type": "keyword", "ignore_above": 256},
        "subject": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
    }
}

index_settings["mappings"] = mappings


def translate_decision(decision):
    result = {}
    for key in decision:
        if key in translations:
            result[translations[key]] = decision[key]
    return result


# clear data
# client.indices.delete(index="conseil")

client.indices.create(index="conseil", body=index_settings)
for dec in data:
    processed_decision = translate_decision(dec)
    processed_decision["number"] = int(processed_decision["number"])
    client.index(index="conseil", document=processed_decision)


client.indices.refresh(index="conseil")
print("Data indexed successfully!")
