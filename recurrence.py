import csv
import gzip
import logging
import boto3
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm

LOG_FILE = "/data/scripts/EXPORT/log/recurrence.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MONGO_URI = "ENTER YOUR MONGO URI HERE"
DATABASE_NAME = "PPS"
COLLECTION_NAME = "recurrence"

BASE_FIELDS = ["groupId", "recurrenceType", "household", "originalSek", "created"]
ARRAY_FIELD = "episodeIdentifiers"

S3_BUCKET = "astrostgeks-pdo-deployer-s3-volume"

timestamp = datetime.now().strftime("%d-%m-%Y_%H%M%S")
OUTPUT_CSV = f"/data/scripts/EXPORT/recurrence-{timestamp}.csv.gz"
OUTPUT_CSV_GZ = f"{OUTPUT_CSV}.gz"
S3_KEY = f"spatil/recurrence-{timestamp}.csv.gz"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def export_to_csv():
    try:
        logging.info("===== EXPORT SCRIPT STARTED =====")
        total_docs = collection.count_documents({})
        logging.info(f"Total documents to export: {total_docs}")

        max_show = 0
        cursor = collection.find({}, projection=BASE_FIELDS + [ARRAY_FIELD])
        for doc in cursor:
            max_show = max(max_show, len(doc.get(ARRAY_FIELD, [])))

        logging.info(f"Maximum showId elements in any document: {max_show}")

        show_headers = [f"episodeId.{i}" for i in range(max_show)]
        NEW_HEADER = BASE_FIELDS + show_headers

        with gzip.open(OUTPUT_CSV_GZ, "wt", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(NEW_HEADER)

            cursor = collection.find({}, projection=BASE_FIELDS + [ARRAY_FIELD])
            for doc in tqdm(cursor, total=total_docs):
                row = [doc.get(f, "") for f in BASE_FIELDS]
                show_row = doc.get(ARRAY_FIELD, []) + [""] * (max_show - len(doc.get(ARRAY_FIELD, [])))
                writer.writerow(row + show_row)

        logging.info(f"CSV export completed and compressed: {OUTPUT_CSV_GZ}")

        s3 = boto3.client("s3")
        s3.upload_file(OUTPUT_CSV_GZ, S3_BUCKET, S3_KEY)
        logging.info(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

        logging.info("===== EXPORT SCRIPT COMPLETED SUCCESSFULLY =====")

    except Exception as e:
        logging.exception(f"ERROR OCCURRED: {e}")
        print(f"Error occurred. Check log file: {LOG_FILE}")

if __name__ == "__main__":
    export_to_csv()
