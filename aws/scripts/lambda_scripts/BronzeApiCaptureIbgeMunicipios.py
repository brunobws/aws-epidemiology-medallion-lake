####################################################################
# Author: Bruno William da Silva
# Date: 25/03/2026
#
# Description:
#   Lambda responsible for ingesting municipality reference data
#   from the IBGE Localidades API into S3 (Bronze layer).
#   Returns the full list of municipalities for Sao Paulo state
#   with geocodes, names, microregion and mesoregion.
#   Documentation: https://servicodados.ibge.gov.br/api/docs/localidades
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

JOB_NAME      = "BronzeApiCaptureIbgeMunicipios"
TARGET_TABLE  = "ibge_tb_municipios"

S3_BUCKET     = os.environ["S3_BUCKET"]
ENV           = os.getenv("ENV", "")
S3_BUCKET = f'{S3_BUCKET}-{ENV}'

# IBGE Localidades API - Municipalities for Sao Paulo state (UF code 35)
BASE_URL      = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/35/municipios"

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


def flatten_municipio(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Flattens the nested IBGE municipality JSON into a flat record.

    Raw IBGE structure:
        {
            "id": 3550308,
            "nome": "Sao Paulo",
            "microrregiao": {
                "id": 35061,
                "nome": "Sao Paulo",
                "mesorregiao": {
                    "id": 3515,
                    "nome": "Metropolitana de Sao Paulo",
                    ...
                }
            },
            "regiao-imediata": { ... }
        }

    Returns:
        Flat dict with: cd_geocode, nm_municipio, nm_microrregiao, nm_mesorregiao
    """
    microrregiao = raw.get("microrregiao", {})
    mesorregiao  = microrregiao.get("mesorregiao", {})

    return {
        "cd_geocode":      raw.get("id"),
        "nm_municipio":    raw.get("nome"),
        "nm_microrregiao": microrregiao.get("nome"),
        "nm_mesorregiao":  mesorregiao.get("nome"),
    }


def fetch_all_municipios() -> List[Dict[str, Any]]:
    """Fetches all SP municipalities from the IBGE Localidades API."""

    print(f"[INFO] Fetching municipalities from: {BASE_URL}")
    raw_data = get_json(BASE_URL)

    if not raw_data:
        logger.warning(warning_msg='empty api response')
        logger.write_log()
        raise ValueError("IBGE API returned empty response. Aborting ingestion.")

    print(f"[INFO] Raw records received: {len(raw_data)}")

    # Flatten nested JSON structure into flat records
    records = [flatten_municipio(item) for item in raw_data]

    print(f"[INFO] Total municipalities flattened: {len(records)}")
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
    key            = f"ibge/tb_municipios/ingestion_date={ingestion_date}/{filename}"

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
    print("[INFO] Starting ingestion from IBGE Localidades API (SP municipalities)")

    try:

        municipios                 = fetch_all_municipios()
        ingestion_date, filename   = upload_to_s3(municipios)

        logger.write_log()

        print(f"[INFO] Ingestion completed successfully. Records: {len(municipios)}, Date: {ingestion_date}")

        return {
            "statusCode"     : 200,
            "message"        : "Ingestion completed successfully",
            "total_records"  : len(municipios),
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