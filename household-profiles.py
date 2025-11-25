import subprocess
import gzip
import logging
import boto3
from datetime import datetime
import shutil

LOG_FILE = "/data/scripts/EXPORT/log/household-profiles.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MONGO_URI = "ENTER YOUR MONGO URI HERE"
COLLECTION_NAME = "userProfile"
FIELDS = "upmMetadata.householdId,userProfileId,profileName,profileAvatar,ageRange,profileType,preferences.nickName,createDate,lastUpdateDate"
NEW_HEADER = "householdId,profileId,name,avatar,ageRange,profileType,nickname,createDate,lastUpdateDate"

S3_BUCKET = "astrostgeks-pdo-deployer-s3-volume"

timestamp = datetime.now().strftime("%d-%m-%Y_%H%M%S")
OUTPUT_CSV = f"/data/scripts/EXPORT/household-profiles-{timestamp}.csv.gz"
OUTPUT_CSV_GZ = f"{OUTPUT_CSV}.gz"
S3_KEY = f"spatil/household-profiles-{timestamp}.csv.gz"


def export_with_mongoexport():
    try:
        logging.info("===== EXPORT SCRIPT STARTED =====")

        cmd = [
            "mongoexport",
            "--uri", MONGO_URI,
            "--collection", COLLECTION_NAME,
            "--type=csv",
            "--fields", FIELDS,
            "--out", OUTPUT_CSV
        ]

        logging.info(f"Running mongoexport command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        logging.info(f"mongoexport completed: {OUTPUT_CSV}")

        with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        lines[0] = NEW_HEADER + "\n"

        with open(OUTPUT_CSV, 'w', encoding='utf-8') as f:
            f.writelines(lines)

        logging.info(f"Header replaced successfully: {NEW_HEADER}")

        with open(OUTPUT_CSV, 'rb') as f_in, gzip.open(OUTPUT_CSV_GZ, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        logging.info(f"CSV compressed to: {OUTPUT_CSV_GZ}")

        s3 = boto3.client("s3")
        s3.upload_file(OUTPUT_CSV_GZ, S3_BUCKET, S3_KEY)
        logging.info(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

        logging.info("===== EXPORT SCRIPT COMPLETED SUCCESSFULLY =====")

    except subprocess.CalledProcessError as e:
        logging.exception(f"mongoexport failed: {e}")
        print(f"mongoexport failed. Check log file: {LOG_FILE}")

    except Exception as e:
        logging.exception(f"ERROR OCCURRED: {e}")
        print(f"Error occurred. Check log file: {LOG_FILE}")


if __name__ == "__main__":
    export_with_mongoexport()
