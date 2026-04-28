####################################################################
# Author: Bruno William da Silva
# Date: 25/03/2026
#
# Description:
#   Lambda responsible for ingesting epidemiological alert data from
#   the InfoDengue API (Fiocruz) into S3 (Bronze layer).
#   Fetches weekly alerts for all SP municipalities across three
#   diseases: dengue, chikungunya and zika.
#   Documentation: https://info.dengue.mat.br/services/api
#
# Environment Variables:
#   S3_BUCKET         : (required) Target S3 bucket name
#   ENV               : (required) Execution environment (e.g.: hlg, prd)
#   TIMEOUT           : (optional) Request timeout in seconds (default: 30)
#   MAX_RETRIES       : (optional) Max retry attempts per request (default: 3)
#   RETRY_BACKOFF     : (optional) Exponential backoff multiplier in seconds (default: 2.0)
#   REQUEST_DELAY_MS  : (optional) Delay between API calls in ms (default: 50)
#   LOOKBACK_WEEKS    : (optional) Number of weeks to look back (default: 4)
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
import math
import urllib3
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple

# Custom
from utils import AwsManager, Dynamo
from logs import Logs
from support import split_target_table
#######################################

############### Config globals var ####################################

JOB_NAME      = "BronzeApiCaptureInfoDengue"
TARGET_TABLE  = "infodengue_tb_alertas"

S3_BUCKET     = os.environ["S3_BUCKET"]
ENV           = os.getenv("ENV", "")
S3_BUCKET     = f"{S3_BUCKET}-{ENV}" 

# InfoDengue API base URL
BASE_URL      = "https://info.dengue.mat.br/api/alertcity"

# Diseases to fetch
DISEASES      = ["dengue", "chikungunya", "zika"]

TIMEOUT          = int(os.getenv("TIMEOUT", 30))                    # Request timeout in seconds
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", 3))                 # Max retry attempts on failed requests
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF", 2.0))           # Exponential backoff multiplier (attempt^n seconds)
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", 50))           # Delay between API calls in milliseconds
LOOKBACK_WEEKS   = int(os.getenv("LOOKBACK_WEEKS", 4))              # Weeks to look back for data capture

# IBGE API to fetch municipality geocodes (fallback if not passed in event)
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/35/municipios"

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


http = urllib3.PoolManager(num_pools=10, maxsize=10)

########################################################################




####################### Epidemiological week helpers #############################

def get_epi_week_range() -> Tuple[int, int, int, int]:
    """Calculates the epidemiological week range for the lookback window.

    Returns:
        Tuple[int, int, int, int]: (ew_start, ew_end, ey_start, ey_end)
    """
    now   = datetime.now(tz=timezone.utc)
    start = now - timedelta(weeks=LOOKBACK_WEEKS)

    # ISO week number maps closely to epidemiological week in Brazil
    ey_start = start.isocalendar()[0]
    ew_start = start.isocalendar()[1]
    ey_end   = now.isocalendar()[0]
    ew_end   = now.isocalendar()[1]

    return ew_start, ew_end, ey_start, ey_end

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


def fetch_geocodes(event: Dict[str, Any]) -> List[int]:
    """Retrieves the list of SP municipality geocodes.

    Checks the event payload first (passed from Step Functions).
    Falls back to calling the IBGE API directly.
    """
    # Try to get geocodes from the event (passed by Step Functions)
    geocodes = event.get("geocodes", [])
    if geocodes:
        print(f"[INFO] Using {len(geocodes)} geocodes from event payload")
        return geocodes

    # Fallback: fetch from IBGE API
    print(f"[INFO] No geocodes in event. Fetching from IBGE API: {IBGE_MUNICIPIOS_URL}")
    raw_data = get_json(IBGE_MUNICIPIOS_URL)

    if not raw_data:
        raise ValueError("IBGE API returned empty response. Cannot fetch geocodes.")

    geocodes = [item["id"] for item in raw_data]
    print(f"[INFO] Fetched {len(geocodes)} geocodes from IBGE API")
    return geocodes


def flatten_alert_record(raw: Dict[str, Any], disease: str, geocode: int) -> Dict[str, Any]:
    """Flattens an InfoDengue alert record into the target schema.

    InfoDengue fields:
        SE              = Epidemiological week ID (e.g. 202452)
        data_iniSE      = Week start date (Unix timestamp in milliseconds)
        casos           = Reported cases
        casos_est       = Estimated cases (nowcasting)
        casos_est_min/max = 95% confidence interval
        p_rt1           = Probability Rt > 1
        Rt              = Effective reproduction number
        p_inc100k       = Incidence per 100k
        nivel           = Alert level (1=green, 2=yellow, 3=orange, 4=red)
        pop             = Population (string)
        municipio_nome  = Municipality name
        tempmin/med/max = Temperature stats
        umidmin/med/max = Humidity stats
        receptivo       = Climate receptivity flag
        transmissao     = Active transmission flag
    """
    se_val    = raw.get("SE", 0)
    nr_ano    = se_val // 100
    nr_semana = se_val % 100
    ts_ms = raw.get("data_iniSE", 0)
    dt_semana = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d") if ts_ms else None
    return {
        "cd_geocode":              geocode,
        "ds_doenca":               disease,
        "dt_semana_epidemiologica": dt_semana,
        "nr_semana_epi":           nr_semana,
        "nr_ano_epi":              nr_ano,
        "nr_nivel_alerta":         raw.get("nivel"),
        "vl_casos_estimados":      raw.get("casos_est"),
        "vl_casos_estimados_min":  raw.get("casos_est_min"),
        "vl_casos_estimados_max":  raw.get("casos_est_max"),
        "vl_casos":                raw.get("casos"),
        "vl_rt":                   raw.get("Rt"),
        "vl_incidencia":           raw.get("p_inc100k"),
        "vl_temp_min":             raw.get("tempmin"),
        "vl_temp_max":             raw.get("tempmax"),
        "vl_umid_min":             raw.get("umidmin"),
        "vl_umid_max":             raw.get("umidmax"),
        "vl_receptividade":        raw.get("receptivo"),
        "vl_transmissao":          raw.get("transmissao"),
        "nm_municipio":            raw.get("municipio_nome"),
    }


def fetch_alerts_for_disease(geocodes: List[int], disease: str,
                              ew_start: int, ew_end: int,
                              ey_start: int, ey_end: int) -> Tuple[List[Dict], int]:
    """Fetches alert data for a single disease across all municipalities.

    Returns:
        Tuple[List[Dict], int]: (all_records, failure_count)
    """
    all_records = []
    failures    = 0

    total = len(geocodes)

    for i, geocode in enumerate(geocodes):
        url = (
            f"{BASE_URL}?geocode={geocode}&disease={disease}&format=json"
            f"&ew_start={ew_start}&ew_end={ew_end}"
            f"&ey_start={ey_start}&ey_end={ey_end}"
        )

        try:
            raw_alerts = get_json(url)

            if raw_alerts:
                records = [flatten_alert_record(r, disease, geocode) for r in raw_alerts]
                all_records.extend(records)

        except Exception as e:
            failures += 1
            print(f"[WARNING] Failed to fetch {disease} for geocode {geocode} ({i+1}/{total}): {e}")

        # Rate limiting between requests
        if REQUEST_DELAY_MS > 0:
            time.sleep(REQUEST_DELAY_MS / 1000.0)

        # Progress log every 100 municipalities
        if (i + 1) % 100 == 0:
            print(f"[INFO] {disease}: processed {i+1}/{total} municipalities ({len(all_records)} records so far)")

    print(f"[INFO] {disease}: completed. Records: {len(all_records)}, Failures: {failures}/{total}")
    return all_records, failures


def fetch_all_alerts(event: Dict[str, Any]) -> Tuple[List[Dict], Dict[str, int]]:
    """Fetches alert data for all diseases and municipalities.

    Returns:
        Tuple[List[Dict], Dict[str, int]]: (all_records, failure_summary)
    """
    geocodes = fetch_geocodes(event)
    ew_start, ew_end, ey_start, ey_end = get_epi_week_range()

    print(f"[INFO] Epi week range: {ey_start}W{ew_start} to {ey_end}W{ew_end}")
    print(f"[INFO] Municipalities: {len(geocodes)}, Diseases: {len(DISEASES)}")
    print(f"[INFO] Total API calls expected: {len(geocodes) * len(DISEASES)}")

    all_records     = []
    failure_summary = {}

    for disease in DISEASES:
        print(f"[INFO] ──── Starting fetch for disease: {disease} ────")
        records, failures = fetch_alerts_for_disease(
            geocodes, disease, ew_start, ew_end, ey_start, ey_end
        )
        all_records.extend(records)
        failure_summary[disease] = failures

    total_failures = sum(failure_summary.values())
    print(f"[INFO] All diseases completed. Total records: {len(all_records)}, Total failures: {total_failures}")

    if total_failures > 0:
        logger.add_info(failures=failure_summary)
        print(f"[WARNING] Failure summary: {failure_summary}")

    return all_records, failure_summary

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
    key            = f"infodengue/tb_alertas/ingestion_date={ingestion_date}/{filename}"

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
    print("[INFO] Starting ingestion from InfoDengue API (SP epidemiological alerts)")

    try:

        alerts, failure_summary    = fetch_all_alerts(event)

        if not alerts:
            logger.warning(warning_msg='no alert records collected from InfoDengue API')
            logger.write_log()
            raise ValueError("No records collected from InfoDengue API. Aborting.")

        ingestion_date, filename   = upload_to_s3(alerts)

        # Log warning if there were partial failures but data was still collected
        total_failures = sum(failure_summary.values())
        if total_failures > 0:
            logger.warning(warning_msg=f"Partial failures during ingestion: {failure_summary}")

        logger.write_log()

        print(f"[INFO] Ingestion completed successfully. Records: {len(alerts)}, Date: {ingestion_date}")

        return {
            "statusCode"      : 200,
            "message"         : "Ingestion completed successfully",
            "total_records"   : len(alerts),
            "ingestion_date"  : ingestion_date,
            "filename"        : filename,
            "failure_summary" : failure_summary,
        }

    except Exception as e:
        print(f"[ERROR] Lambda execution failed: {e}")
        logger.error(error_msg="Lambda execution failed", error_desc=str(e))

        manager.ses.send_email_on_failure(
            target_table=TARGET_TABLE,
            description=str(e),
            destination=email_on_failure)

        raise
