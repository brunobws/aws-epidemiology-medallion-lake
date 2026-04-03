####################################################################
# Author: Bruno William da Silva
# Date: 25/03/2026
#
# Description:
#   Lambda responsible for downloading arbovirus notification data
#   from SINAN/OpenDataSUS public S3 bucket into our Bronze layer.
#   Downloads ZIP files containing CSV with individual notification
#   records for dengue, chikungunya and zika.
#
#   NOTE: The OpenDataSUS REST API is limited to 20 records/page,
#   making it unusable for bulk data. This Lambda downloads the
#   CSV files directly from the CKAN S3 bucket instead.
#
#   Source: https://opendatasus.saude.gov.br/dataset/arboviroses-dengue
#
# Environment Variables:
#   S3_BUCKET     : (required) Target S3 bucket name
#   ENV           : (required) Execution environment (e.g.: hlg, prd)
#   TIMEOUT       : (optional) Request timeout in seconds (default: 120)
#   MAX_RETRIES   : (optional) Max retry attempts on failed requests (default: 3)
#   RETRY_BACKOFF : (optional) Exponential backoff multiplier in seconds (default: 2.0)
#
#   E-mails to alert:
#    Configure recipients in the 'notification_params' DynamoDB table
#
# Trigger:
#   - AWS Step Functions
#   - Manual trigger for testing
####################################################################

######### imports ################
import os
import io
import json
import time
import zipfile
import urllib3
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

# Custom
from utils import AwsManager, Dynamo
from logs import Logs
from support import split_target_table
#######################################

############### Config globals var ####################################

JOB_NAME      = "BronzeS3CaptureSinan"
TARGET_TABLE  = "sinan_tb_notificacoes"

S3_BUCKET     = os.environ["S3_BUCKET"]
ENV           = os.getenv("ENV", "")
S3_BUCKET     = f"{S3_BUCKET}-{ENV}" 

# OpenDataSUS CKAN S3 bucket - direct CSV download URLs
# Pattern: https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/{Disease}/csv/{PREFIX}{YY}.csv.zip
SOURCE_BASE_URL = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN"

# Disease configuration: (disease_name, url_path, file_prefix)
# NOTE: Zika removed from OpenDataSUS — files return 403 (Access Denied)
DISEASE_CONFIG = [
    ("dengue",       "Dengue",       "DENGBR"),
    ("chikungunya",  "Chikungunya",  "CHIKBR"),
]

TIMEOUT       = int(os.getenv("TIMEOUT", 120))                     # Request timeout in seconds (larger for big files)
MAX_RETRIES   = int(os.getenv("MAX_RETRIES", 3))                   # Max retry attempts on failed requests
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", 2.0))             # Exponential backoff multiplier (attempt^n seconds)

#####################################################################



##################### Custom classes and instances #########################


logger = Logs(job_name=JOB_NAME, target_table=TARGET_TABLE, layer="bronze", env=ENV, technology="lambda")

dynamo = Dynamo(job_name=JOB_NAME,
                logger=logger,
                trgt_tbl=TARGET_TABLE)

response = dynamo.get_dynamo_records(dynamo_table='notification_params',
                                     id_value=TARGET_TABLE,
                                     id_column='trgt_tbl')

email_on_failure, email_on_warning, email_on_success = dynamo.get_email_notif(response, layer='ingestion')

manager = AwsManager(job_name=JOB_NAME, logger=logger, destination=email_on_failure, target_table=TARGET_TABLE)


is_critical = response.get('critical', False)
logger.add_info(critical=is_critical)


http = urllib3.PoolManager()

########################################################################




####################### Download and extract helpers #################################

def download_bytes(url: str) -> bytes:
    """Downloads a file from URL with exponential-backoff retry. Returns raw bytes."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http.request("GET", url, timeout=TIMEOUT, preload_content=True)

            if resp.status == 200:
                print(f"[INFO] Downloaded {len(resp.data) / (1024*1024):.1f} MB from {url}")
                return resp.data

            if resp.status in (404, 403):
                print(f"[WARNING] File not available ({resp.status}): {url}")
                return None

            wait = RETRY_BACKOFF ** attempt
            print(f"[WARNING] HTTP {resp.status} on attempt {attempt}/{MAX_RETRIES}. Retrying in {wait:.1f}s.")
            time.sleep(wait)

        except urllib3.exceptions.HTTPError as e:
            wait = RETRY_BACKOFF ** attempt
            print(f"[WARNING] Connection error on attempt {attempt}/{MAX_RETRIES}: {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for: {url}")


def extract_csv_from_zip(zip_bytes: bytes) -> str:
    """Extracts the first CSV file from a ZIP archive in memory.

    Returns:
        str: CSV file content as string
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]

        if not csv_files:
            raise ValueError("No CSV file found inside ZIP archive")

        csv_name = csv_files[0]
        print(f"[INFO] Extracting CSV from ZIP: {csv_name}")

        csv_bytes = zf.read(csv_name)

        # SINAN CSVs use latin-1 encoding
        return csv_bytes.decode('latin-1')


def build_source_url(url_path: str, file_prefix: str, year: int) -> str:
    """Builds the full download URL for a SINAN dataset.

    Args:
        url_path:    Disease path in URL (e.g. "Dengue")
        file_prefix: File name prefix (e.g. "DENGBR")
        year:        4-digit year (e.g. 2025)

    Returns:
        Full URL like: https://s3.../SINAN/Dengue/csv/DENGBR25.csv.zip
    """
    yy = str(year)[-2:]  # 2025 -> "25"
    return f"{SOURCE_BASE_URL}/{url_path}/csv/{file_prefix}{yy}.csv.zip"

####################################################################


######################## S3 Upload #################################

def upload_csv_to_s3(csv_content: str, disease: str, year: int) -> Tuple[str, str]:
    """Uploads extracted CSV content to S3 Bronze layer.

    Returns:
        Tuple[str, str]: (ingestion_date, filename)
    """
    now            = datetime.now(tz=timezone.utc)
    ingestion_date = now.strftime('%Y-%m-%d')
    filename       = f"{disease}_{year}.csv"
    key            = f"sinan/tb_notificacoes/ingestion_date={ingestion_date}/{filename}"

    manager.s3.put_s3_file(
        bucket=S3_BUCKET,
        key=key,
        body=csv_content
    )

    # Estimate row count from line count (minus header)
    line_count = csv_content.count('\n')
    print(f"[INFO] Uploaded {disease}/{year} (~{line_count} lines) -> s3://{S3_BUCKET}/{key}")

    return ingestion_date, filename

####################################################################


def lambda_handler(event, context):
    print("[INFO] Starting ingestion from SINAN/OpenDataSUS (CSV download)")

    try:
        # Determine which years to download
        current_year = datetime.now(tz=timezone.utc).year
        years = event.get("years", [current_year, current_year - 1])
        print(f"[INFO] Years to download: {years}")

        all_files     = []
        total_lines   = 0
        ingestion_date = None

        for disease_name, url_path, file_prefix in DISEASE_CONFIG:
            for year in years:
                url = build_source_url(url_path, file_prefix, year)
                print(f"[INFO] ──── Downloading {disease_name}/{year}: {url} ────")

                # Download ZIP file
                zip_bytes = download_bytes(url)

                if zip_bytes is None:
                    print(f"[WARNING] Skipping {disease_name}/{year} — file not found")
                    continue

                # Extract CSV from ZIP
                csv_content = extract_csv_from_zip(zip_bytes)
                line_count  = csv_content.count('\n')
                total_lines += line_count

                # Upload CSV to S3 Bronze
                ingestion_date, filename = upload_csv_to_s3(csv_content, disease_name, year)
                all_files.append({"disease": disease_name, "year": year, "filename": filename, "lines": line_count})

                print(f"[INFO] {disease_name}/{year} done: ~{line_count} lines")

        if not all_files:
            logger.warning(warning_msg='no SINAN files were downloaded')
            logger.write_log()
            raise ValueError("No SINAN files were downloaded. Aborting.")

        # Use the last file uploaded as the reference filename for the pipeline
        last_filename = all_files[-1]["filename"]

        logger.add_info(file_name=last_filename, count=total_lines, files=all_files)
        logger.write_log()

        print(f"[INFO] Ingestion completed. Files: {len(all_files)}, Total lines: {total_lines}")

        return {
            "statusCode"     : 200,
            "message"        : "Ingestion completed successfully",
            "total_records"  : total_lines,
            "ingestion_date" : ingestion_date,
            "filename"       : last_filename,
            "files"          : all_files,
        }

    except Exception as e:
        print(f"[ERROR] Lambda execution failed: {e}")
        logger.error(error_msg="Lambda execution failed", error_desc=str(e))

        manager.ses.send_email_on_failure(
            target_table=TARGET_TABLE,
            description=str(e),
            destination=email_on_failure)

        raise
