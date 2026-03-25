####################################################################
# Tests for aws/lambda_scripts/BronzeApiCaptureInfoDengue.py
#
# Strategy: the Lambda executes AWS calls at module-level (Logs,
# Dynamo, AwsManager). We inject mocks into sys.modules BEFORE
# importing the Lambda so no real AWS calls are made.
# All tests run fully offline — no credentials required.
####################################################################

import sys
import os
import json
import re
from unittest.mock import MagicMock, patch
import pytest
import urllib3

# ── Environment variables required by the Lambda at import time ──────────────
os.environ.setdefault("S3_BUCKET", "test-bucket")
os.environ.setdefault("ENV", "test")

# ── Add Lambda directory to path (flat imports: from utils import ...) ────────
_LAMBDA_DIR  = os.path.join(os.path.dirname(__file__), "..", "aws", "lambda_scripts")
_MODULES_DIR = os.path.join(os.path.dirname(__file__), "..", "aws", "modules")
sys.path.insert(0, os.path.abspath(_LAMBDA_DIR))
sys.path.insert(0, os.path.abspath(_MODULES_DIR))

# ── Stub custom AWS modules before the Lambda is imported ────────────────────
_mock_logger  = MagicMock()
_mock_dynamo  = MagicMock()
_mock_manager = MagicMock()

_mock_dynamo.get_dynamo_records.return_value = {}
_mock_dynamo.get_email_notif.return_value    = ([], [], [])

_mock_logs_module    = MagicMock()
_mock_utils_module   = MagicMock()
_mock_support_module = MagicMock()

_mock_logs_module.Logs.return_value         = _mock_logger
_mock_utils_module.Dynamo.return_value      = _mock_dynamo
_mock_utils_module.AwsManager.return_value  = _mock_manager

sys.modules.setdefault("logs",    _mock_logs_module)
sys.modules.setdefault("utils",   _mock_utils_module)
sys.modules.setdefault("support", _mock_support_module)

# ── Safe to import  ────────────────────────────────────────────
import BronzeApiCaptureInfoDengue as lm


########## Sample InfoDengue API response ##########

SAMPLE_ALERT_RAW = {
    "SE": 202512,
    "data_iniSE": 1711065600000,
    "casos": 150,
    "casos_est": 180.5,
    "casos_est_min": 160.2,
    "casos_est_max": 200.8,
    "p_rt1": 0.85,
    "Rt": 1.2,
    "p_inc100k": 12.5,
    "nivel": 3,
    "pop": "1200000",
    "municipio_geocodigo": 3550308,
    "municipio_nome": "São Paulo",
    "tempmin": 20.1,
    "tempmed": 25.3,
    "tempmax": 30.5,
    "umidmin": 55.0,
    "umidmed": 70.0,
    "umidmax": 85.0,
    "receptivo": 1,
    "transmissao": 1
}

SAMPLE_IBGE_MUNICIPIOS = [
    {"id": 3550308, "nome": "São Paulo"},
    {"id": 3509502, "nome": "Campinas"},
    {"id": 3548500, "nome": "Santos"}
]


########## get_epi_week_range ##########

class TestGetEpiWeekRange:

    def test_returns_four_integer_values(self):
        ew_start, ew_end, ey_start, ey_end = lm.get_epi_week_range()

        assert isinstance(ew_start, int)
        assert isinstance(ew_end, int)
        assert isinstance(ey_start, int)
        assert isinstance(ey_end, int)

    def test_weeks_are_in_valid_range(self):
        ew_start, ew_end, _, _ = lm.get_epi_week_range()

        assert 1 <= ew_start <= 53
        assert 1 <= ew_end <= 53

    def test_years_are_reasonable(self):
        _, _, ey_start, ey_end = lm.get_epi_week_range()

        assert ey_start >= 2020
        assert ey_end >= ey_start


########## flatten_alert_record ##########

class TestFlattenAlertRecord:

    def test_flattens_correctly(self):
        result = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "dengue")

        assert result["cd_geocode"]              == 3550308
        assert result["ds_doenca"]               == "dengue"
        assert result["nr_semana_epi"]           == 12
        assert result["nr_ano_epi"]              == 2025
        assert result["nr_nivel_alerta"]         == 3
        assert result["vl_casos"]                == 150
        assert result["vl_casos_estimados"]      == 180.5
        assert result["vl_rt"]                   == 1.2
        assert result["vl_incidencia"]           == 12.5

    def test_sets_disease_from_parameter(self):
        result_dengue = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "dengue")
        result_chik   = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "chikungunya")

        assert result_dengue["ds_doenca"] == "dengue"
        assert result_chik["ds_doenca"]   == "chikungunya"

    def test_parses_epi_week_from_se_field(self):
        raw = {**SAMPLE_ALERT_RAW, "SE": 202401}
        result = lm.flatten_alert_record(raw, "zika")

        assert result["nr_ano_epi"]    == 2024
        assert result["nr_semana_epi"] == 1

    def test_converts_timestamp_to_date_string(self):
        result = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "dengue")
        # data_iniSE=1711065600000 is 2024-03-22 UTC
        assert result["dt_semana_epidemiologica"] is not None
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", result["dt_semana_epidemiologica"])

    def test_includes_climate_data(self):
        result = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "dengue")

        assert result["vl_temp_min"]  == 20.1
        assert result["vl_temp_max"]  == 30.5
        assert result["vl_umid_min"]  == 55.0
        assert result["vl_umid_max"]  == 85.0

    def test_returns_all_expected_fields(self):
        result = lm.flatten_alert_record(SAMPLE_ALERT_RAW, "dengue")
        expected_keys = {
            "cd_geocode", "ds_doenca", "dt_semana_epidemiologica",
            "nr_semana_epi", "nr_ano_epi", "nr_nivel_alerta",
            "vl_casos_estimados", "vl_casos_estimados_min", "vl_casos_estimados_max",
            "vl_casos", "vl_rt", "vl_incidencia",
            "vl_temp_min", "vl_temp_max", "vl_umid_min", "vl_umid_max",
            "vl_receptividade", "vl_transmissao", "nm_municipio"
        }
        assert set(result.keys()) == expected_keys


########## fetch_geocodes ##########

class TestFetchGeocodes:

    def test_uses_geocodes_from_event_when_present(self):
        event = {"geocodes": [3550308, 3509502]}
        result = lm.fetch_geocodes(event)
        assert result == [3550308, 3509502]

    def test_falls_back_to_ibge_api_when_event_empty(self):
        with patch.object(lm, "get_json", return_value=SAMPLE_IBGE_MUNICIPIOS):
            result = lm.fetch_geocodes({})

        assert len(result) == 3
        assert 3550308 in result

    def test_raises_when_ibge_api_returns_empty(self):
        with patch.object(lm, "get_json", return_value=[]):
            with pytest.raises(ValueError, match="empty response"):
                lm.fetch_geocodes({})


########## fetch_alerts_for_disease ##########

class TestFetchAlertsForDisease:

    def test_returns_records_and_failure_count(self):
        with patch.object(lm, "get_json", return_value=[SAMPLE_ALERT_RAW]), \
             patch("BronzeApiCaptureInfoDengue.time.sleep"):
            records, failures = lm.fetch_alerts_for_disease(
                [3550308, 3509502], "dengue", 1, 12, 2025, 2025
            )

        assert len(records) == 2  # 1 record per municipality
        assert failures == 0

    def test_continues_on_single_municipality_failure(self):
        def side_effect(url):
            if "3509502" in url:
                raise RuntimeError("API error")
            return [SAMPLE_ALERT_RAW]

        with patch.object(lm, "get_json", side_effect=side_effect), \
             patch("BronzeApiCaptureInfoDengue.time.sleep"):
            records, failures = lm.fetch_alerts_for_disease(
                [3550308, 3509502], "dengue", 1, 12, 2025, 2025
            )

        assert len(records) == 1  # Only Sao Paulo succeeded
        assert failures == 1

    def test_handles_empty_api_response_gracefully(self):
        with patch.object(lm, "get_json", return_value=[]), \
             patch("BronzeApiCaptureInfoDengue.time.sleep"):
            records, failures = lm.fetch_alerts_for_disease(
                [3550308], "dengue", 1, 12, 2025, 2025
            )

        assert len(records) == 0
        assert failures == 0


########## upload_to_s3 ##########

class TestUploadToS3:

    def setup_method(self):
        lm.manager.s3.put_s3_file.reset_mock()

    def test_calls_s3_put_once(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        lm.manager.s3.put_s3_file.assert_called_once()

    def test_key_contains_infodengue_path(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert "infodengue/tb_alertas/" in kwargs["key"]

    def test_key_contains_ingestion_date_partition(self):
        ingestion_date, _ = lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert f"ingestion_date={ingestion_date}" in kwargs["key"]

    def test_returns_filename_with_json_extension(self):
        _, filename = lm.upload_to_s3([{"cd_geocode": 1}])
        assert filename.startswith("data_") and filename.endswith(".json")


########## lambda_handler ##########

class TestLambdaHandler:

    def test_returns_200_on_success(self):
        alerts = [{"cd_geocode": 1, "ds_doenca": "dengue"}]
        failure_summary = {"dengue": 0, "chikungunya": 0, "zika": 0}

        with patch.object(lm, "fetch_all_alerts", return_value=(alerts, failure_summary)), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["statusCode"] == 200

    def test_response_includes_failure_summary(self):
        alerts = [{"cd_geocode": 1}]
        failure_summary = {"dengue": 2, "chikungunya": 0, "zika": 1}

        with patch.object(lm, "fetch_all_alerts", return_value=(alerts, failure_summary)), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["failure_summary"]["dengue"] == 2

    def test_raises_when_no_records_collected(self):
        with patch.object(lm, "fetch_all_alerts", return_value=([], {})):
            with pytest.raises(ValueError, match="No records"):
                lm.lambda_handler({}, None)

    def test_raises_and_sends_email_on_failure(self):
        lm.manager.ses.send_email_on_failure.reset_mock()

        with patch.object(lm, "fetch_all_alerts", side_effect=RuntimeError("API down")):
            with pytest.raises(RuntimeError):
                lm.lambda_handler({}, None)

        lm.manager.ses.send_email_on_failure.assert_called_once()
