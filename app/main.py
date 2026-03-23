from google.cloud import bigquery
from pymongo import MongoClient, errors
from config import *
from datetime import date, datetime


def convert_types(record):
    """
    Convert date fields to datetime for MongoDB insertion.
    """
    for key, value in record.items():
        if isinstance(value, date) and not isinstance(value, datetime):
            record[key] = datetime.combine(value, datetime.min.time())
    return record


def fetch_bigquery_data():
    """
    Fetch data from BigQuery table.
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM {PROJECT_ID}.{DATASET}.{TABLE}
    """

    query_job = client.query(query)
    return query_job.result()


def load_to_mongo(rows):
    """
    Load rows into MongoDB in batches, skipping duplicates.
    """
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    batch = []
    count = 0

    for row in rows:
        record = convert_types(dict(row))
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            try:
                collection.insert_many(batch, ordered=False)
                count += len(batch)
                print(f"Inserted {count} records")
            except errors.BulkWriteError as bwe:
                print("Some duplicates were skipped:", bwe.details)
            batch = []

    # Insert any remaining records
    if batch:
        try:
            collection.insert_many(batch, ordered=False)
            count += len(batch)
            print(f"Inserted final {len(batch)} records")
        except errors.BulkWriteError as bwe:
            print("Some duplicates were skipped in final batch:", bwe.details)

    print(f"Total records inserted: {count}")


def main():
    print("Starting BigQuery → MongoDB load...")
    rows = fetch_bigquery_data()
    load_to_mongo(rows)
    print("Data load completed successfully!")


if __name__ == "__main__":
    main()
