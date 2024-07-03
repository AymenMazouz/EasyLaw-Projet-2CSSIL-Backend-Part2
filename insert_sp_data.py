#
#           Insert Supreme Court Data
#

from elasticsearch import Elasticsearch
import json
from es_index_settings import index_settings
import re

client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "gRakNDvDTcg+puCVov5W"),
    ca_certs="http_ca.crt",
)

with open("data/supreme_court.json", encoding="utf-8") as file:
    data = json.load(file)

translations = {
    "رقم القرار:": "number",
    "تاريخ القرار:": "date",
    "الموضوع:": "subject",
    "الأطراف:": "parties",
    "الكلمات الأساسية:": "keywords",
    "المرجع القانوني:": "reference",
    "المبدأ:": "principle",
    "وجه الطعن المثار من الطاعن المرتبط بالمبدأ:": "ground_of_appeal",
    "رد المحكمة العليا عن الوجه المرتبط بالمبدأ:": "supreme_court_response",
    "منطوق القرار:": "verdict",
    "الرئيس:": "president",
    "المستشار المقرر:": "reporting_judge",
}


def translate_decision(decision):
    result = {}
    for key in decision:
        if key in translations:
            result[translations[key]] = decision[key]
    return result


# clear data
client.indices.delete(index="supreme-court")

client.indices.create(index="supreme-court", body=index_settings)
for key in data:
    processed_decision = translate_decision(data[key])
    processed_decision["keywords"] = re.split(
        r"\s*[-–]\s*", processed_decision.get("keywords", "")
    )
    processed_decision["number"] = int(processed_decision["number"])
    client.index(index="supreme-court", document=processed_decision)


client.indices.refresh(index="supreme-court")
print("Data indexed successfully!")
