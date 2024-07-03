import psycopg2
from es_index_settings import index_settings
from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch, helpers

load_dotenv()
# Establish a connection to the database
db_conn = psycopg2.connect(
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST"),
)

es_client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", os.environ.get("ES_PASSWORD")),
    verify_certs=False,
)

mappings = {
    "properties": {
        "content": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "field": {
            "type": "keyword",
            "ignore_above": 256,
        },
        "journal_date": {"type": "date"},
        "journal_num": {"type": "long"},
        "journal_page": {"type": "long"},
        "long_content": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "ministry": {
            "type": "keyword",
            "ignore_above": 256,
        },
        "page_fixed": {"type": "boolean"},
        "signature_date": {"type": "date"},
        "text_number": {
            "type": "keyword",
            "ignore_above": 256,
        },
        "text_type": {
            "type": "keyword",
            "ignore_above": 256,
        },
    }
}
index_settings["mappings"] = mappings
es_client.indices.delete(index="laws")
es_client.indices.create(index="laws", body=index_settings)

cur = db_conn.cursor()
cur2 = db_conn.cursor()


def generate_laws_data():
    cur.execute("SELECT * FROM laws;")
    row = cur.fetchone()
    while row:
        journal_year = row[3].year
        journal_num = row[4]
        cur2.execute(
            f"SELECT link FROM official_newspaper WHERE year='{journal_year}' AND number = '{journal_num}';"
        )
        journal_row = cur2.fetchone()

        yield {
            "_index": "laws",
            "_id": row[0],
            "_source": {
                "text_type": row[1],
                "text_number": row[2],
                "journal_date": row[3],
                "journal_num": row[4],
                "journal_page": row[5],
                "signature_date": row[6],
                "ministry": row[7],
                "content": row[8],
                "field": row[9],
                "long_content": row[10],
                "page_fixed": row[11],
                "journal_link": (
                    f"{journal_row[0]}#page={row[5]}" if journal_row else ""
                ),
            },
        }
        row = cur.fetchone()


# index data
helpers.bulk(es_client, generate_laws_data())

# Close the cursor and connection
cur.close()
db_conn.close()
