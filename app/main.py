from google.cloud import bigquery
from pymongo import MongoClient
from config import *

def fetch_bigquery_data():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """

    query_job = client.query(query)
    return query_job.result()

def load_to_mongo(rows):
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    batch = []
    count = 0

    for row in rows:
        record = dict(row)
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch)
            count += len(batch)
            print(f"Inserted {count} records")
            batch = []

    if batch:
        collection.insert_many(batch)
        print(f"Inserted final batch")

def main():
    print("Starting BigQuery → MongoDB load...")
    rows = fetch_bigquery_data()
    load_to_mongo(rows)
    print("Data load completed successfully!")

if __name__ == "__main__":
    main()
