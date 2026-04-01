####################################################################
# Author: Bruno William da Silva
# Date: 25/03/2026
#
# Description:
#   Lambda responsible for ingesting population estimates from the
#   IBGE SIDRA API into S3 (Bronze layer).
#   Returns estimated population for all municipalities in Sao Paulo
#   state (table 6579, variable 9324).
#   Documentation: https://apisidra.ibge.gov.br/home/ajuda
#
# Environment Variables:
#   S3_BUCKET     : (required) Target S3 bucket name
#   ENV           : (required) Execution environment (e.g.: hlg, prd)
#   TIMEOUT       : (optional) Request timeout in seconds (default: 30)
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
import json
import time
import urllib3
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

# Custom
from utils import AwsManager, Dynamo
from logs import Logs
from support import split_target_table
#######################################

############### Config globals var ####################################

JOB_NAME      = "BronzeApiCaptureIbgePopulacao"
TARGET_TABLE  = "ibge_tb_populacao"

S3_BUCKET     = os.environ["S3_BUCKET"]
ENV           = os.getenv("ENV", "")
S3_BUCKET     = f"{S3_BUCKET}-{ENV}" 

# IBGE SIDRA API - Population estimates for all SP municipalities
# Table 6579 = Estimativas da populacao residente
# Variable 9324 = Populacao residente estimada
# n6/in n3 35 = All municipalities (n6) within SP state (n3 code 35)
# p/last = Most recent period
BASE_URL      = "https://apisidra.ibge.gov.br/values/t/6579/n6/in%20n3%2035/v/9324/p/last/f/a/h/y"

TIMEOUT       = int(os.getenv("TIMEOUT", 30))                      # Request timeout in seconds
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




####################### API client #################################

def get_json(url: str) -> Any:
    """GET request with exponential-backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http.request("GET", url, timeout=TIMEOUT)

            if resp.status == 200:
                return json.loads(resp.data.decode("utf-8"))

            wait = RETRY_BACKOFF ** attempt
            print(f"[WARNING] HTTP {resp.status} on attempt {attempt}/{MAX_RETRIES}. Retrying in {wait:.1f}s.")
            time.sleep(wait)

        except urllib3.exceptions.HTTPError as e:
            wait = RETRY_BACKOFF ** attempt
            print(f"[WARNING] Connection error on attempt {attempt}/{MAX_RETRIES}: {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for: {url}")


def parse_sidra_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Parses a SIDRA API data record into a flat structure.

    SIDRA response fields:
        D1C = Municipality code (geocode)
        D1N = Municipality name (with UF suffix, e.g. "Sao Paulo (SP)")
        V   = Population value (string)
        D3C = Reference year code (string)

    Returns:
        Flat dict with: cd_geocode, nm_municipio, vl_populacao, dt_ano_referencia
    """
    # Remove the " (SP)" suffix from municipality name
    nm_raw = raw.get("D1N", "")
    nm_clean = nm_raw.rsplit(" (", 1)[0] if " (" in nm_raw else nm_raw

    return {
        "cd_geocode":        int(raw.get("D1C", 0)),
        "nm_municipio":      nm_clean,
        "vl_populacao":      int(raw.get("V", 0)),
        "dt_ano_referencia": int(raw.get("D3C", 0)),
    }


def fetch_all_populacao() -> List[Dict[str, Any]]:
    """Fetches population estimates for all SP municipalities from SIDRA API."""

    print(f"[INFO] Fetching population data from SIDRA API")
    raw_data = get_json(BASE_URL)

    if not raw_data or len(raw_data) < 2:
        logger.warning(warning_msg='empty api response')
        logger.write_log()
        raise ValueError("SIDRA API returned empty or insufficient response. Aborting ingestion.")

    # First element (index 0) is the header/metadata row — skip it
    data_rows = raw_data[1:]

    print(f"[INFO] Raw data rows received (excluding header): {len(data_rows)}")

    # Parse each data row into flat structure
    records = [parse_sidra_record(row) for row in data_rows]

    print(f"[INFO] Total population records parsed: {len(records)}")
    return records

####################################################################


######################## S3 Upload #################################

def upload_to_s3(data: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Serializes records to JSON and uploads to S3 partitioned by ingestion date.

    Returns:
        Tuple[str, str]: (ingestion_date, filename)
    """

    now            = datetime.now(tz=timezone.utc)
    ingestion_date = now.strftime('%Y-%m-%d')
    filename       = f"data_{now.strftime('%H%M%S')}.json"
    key            = f"ibge/tb_populacao/ingestion_date={ingestion_date}/{filename}"

    logger.add_info(file_name=filename, count=len(data))

    manager.s3.put_s3_file(
        bucket=S3_BUCKET,
        key=key,
        body=json.dumps(data, indent=2, ensure_ascii=False)
    )

    print(f"[INFO] Uploaded {len(data)} records -> s3://{S3_BUCKET}/{key}")

    return ingestion_date, filename

####################################################################


def lambda_handler(event, context):
    print("[INFO] Starting ingestion from IBGE SIDRA API (SP population estimates)")

    try:

        populacao                  = fetch_all_populacao()
        ingestion_date, filename   = upload_to_s3(populacao)

        logger.write_log()

        print(f"[INFO] Ingestion completed successfully. Records: {len(populacao)}, Date: {ingestion_date}")

        return {
            "statusCode"     : 200,
            "message"        : "Ingestion completed successfully",
            "total_records"  : len(populacao),
            "ingestion_date" : ingestion_date,
            "filename"       : filename,
        }

    except Exception as e:
        print(f"[ERROR] Lambda execution failed: {e}")
        logger.error(error_msg="Lambda execution failed", error_desc=str(e))

        manager.ses.send_email_on_failure(
            target_table=TARGET_TABLE,
            description=str(e),
            destination=email_on_failure)

        raise