####################################################################
# Tests for aws/lambda_scripts/BronzeApiCaptureIbgePopulacao.py
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
import BronzeApiCaptureIbgePopulacao as lm


########## Sample SIDRA API response ##########

SAMPLE_SIDRA_RAW = [
    {
        "NC": "Nível Territorial (Código)",
        "NN": "Nível Territorial",
        "MC": "Unidade de Medida (Código)",
        "MN": "Unidade de Medida",
        "V": "Valor",
        "D1C": "Município (Código)",
        "D1N": "Município",
        "D2C": "Variável (Código)",
        "D2N": "Variável",
        "D3C": "Ano (Código)",
        "D3N": "Ano"
    },
    {
        "NC": "6",
        "NN": "Município",
        "MC": "45",
        "MN": "Pessoas",
        "V": "11904961",
        "D1C": "3550308",
        "D1N": "São Paulo (SP)",
        "D2C": "9324",
        "D2N": "População residente estimada",
        "D3C": "2025",
        "D3N": "2025"
    },
    {
        "NC": "6",
        "NN": "Município",
        "MC": "45",
        "MN": "Pessoas",
        "V": "1230539",
        "D1C": "3509502",
        "D1N": "Campinas (SP)",
        "D2C": "9324",
        "D2N": "População residente estimada",
        "D3C": "2025",
        "D3N": "2025"
    },
    {
        "NC": "6",
        "NN": "Município",
        "MC": "45",
        "MN": "Pessoas",
        "V": "4953",
        "D1C": "3500105",
        "D1N": "Adamantina (SP)",
        "D2C": "9324",
        "D2N": "População residente estimada",
        "D3C": "2025",
        "D3N": "2025"
    }
]


########## get_json ##########

class TestGetJson:

    def test_returns_parsed_json_on_200(self):
        mock_resp        = MagicMock()
        mock_resp.status = 200
        mock_resp.data   = json.dumps([{"V": "100"}]).encode()

        with patch.object(lm.http, "request", return_value=mock_resp):
            result = lm.get_json("https://example.com")

        assert result == [{"V": "100"}]

    def test_raises_after_all_retries_on_5xx(self):
        mock_resp        = MagicMock()
        mock_resp.status = 500

        with patch.object(lm.http, "request", return_value=mock_resp), \
             patch("BronzeApiCaptureIbgePopulacao.time.sleep"):
            with pytest.raises(RuntimeError, match="attempts failed"):
                lm.get_json("https://example.com")

    def test_retries_correct_number_of_times(self):
        mock_resp        = MagicMock()
        mock_resp.status = 503

        with patch.object(lm.http, "request", return_value=mock_resp) as mock_req, \
             patch("BronzeApiCaptureIbgePopulacao.time.sleep"):
            with pytest.raises(RuntimeError):
                lm.get_json("https://example.com")

        assert mock_req.call_count == lm.MAX_RETRIES


########## parse_sidra_record ##########

class TestParseSidraRecord:

    def test_parses_sao_paulo_correctly(self):
        result = lm.parse_sidra_record(SAMPLE_SIDRA_RAW[1])

        assert result["cd_geocode"]        == 3550308
        assert result["nm_municipio"]      == "São Paulo"
        assert result["vl_populacao"]      == 11904961
        assert result["dt_ano_referencia"] == 2025

    def test_parses_campinas_correctly(self):
        result = lm.parse_sidra_record(SAMPLE_SIDRA_RAW[2])

        assert result["cd_geocode"]   == 3509502
        assert result["nm_municipio"] == "Campinas"
        assert result["vl_populacao"] == 1230539

    def test_removes_uf_suffix_from_name(self):
        result = lm.parse_sidra_record(SAMPLE_SIDRA_RAW[3])
        assert result["nm_municipio"] == "Adamantina"

    def test_returns_all_four_fields(self):
        result = lm.parse_sidra_record(SAMPLE_SIDRA_RAW[1])
        expected_keys = {"cd_geocode", "nm_municipio", "vl_populacao", "dt_ano_referencia"}
        assert set(result.keys()) == expected_keys

    def test_casts_values_to_correct_types(self):
        result = lm.parse_sidra_record(SAMPLE_SIDRA_RAW[1])
        assert isinstance(result["cd_geocode"], int)
        assert isinstance(result["nm_municipio"], str)
        assert isinstance(result["vl_populacao"], int)
        assert isinstance(result["dt_ano_referencia"], int)


########## fetch_all_populacao ##########

class TestFetchAllPopulacao:

    def test_returns_parsed_records_skipping_header(self):
        with patch.object(lm, "get_json", return_value=SAMPLE_SIDRA_RAW):
            result = lm.fetch_all_populacao()

        # 4 total rows - 1 header = 3 data records
        assert len(result) == 3
        assert result[0]["cd_geocode"] == 3550308
        assert result[1]["nm_municipio"] == "Campinas"

    def test_raises_when_api_returns_empty(self):
        with patch.object(lm, "get_json", return_value=[]):
            with pytest.raises(ValueError, match="empty or insufficient"):
                lm.fetch_all_populacao()

    def test_raises_when_api_returns_only_header(self):
        with patch.object(lm, "get_json", return_value=[SAMPLE_SIDRA_RAW[0]]):
            with pytest.raises(ValueError, match="empty or insufficient"):
                lm.fetch_all_populacao()

    def test_raises_when_api_returns_none(self):
        with patch.object(lm, "get_json", return_value=None):
            with pytest.raises(ValueError, match="empty or insufficient"):
                lm.fetch_all_populacao()


########## upload_to_s3 ##########

class TestUploadToS3:

    def setup_method(self):
        lm.manager.s3.put_s3_file.reset_mock()

    def test_calls_s3_put_once(self):
        lm.upload_to_s3([{"cd_geocode": 1, "vl_populacao": 100}])
        lm.manager.s3.put_s3_file.assert_called_once()

    def test_uses_correct_bucket(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert kwargs["bucket"] == "test-bucket"

    def test_key_contains_ibge_populacao_path(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert "ibge/tb_populacao/" in kwargs["key"]

    def test_key_contains_ingestion_date_partition(self):
        ingestion_date, _ = lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert f"ingestion_date={ingestion_date}" in kwargs["key"]

    def test_returns_valid_ingestion_date_format(self):
        ingestion_date, _ = lm.upload_to_s3([{"cd_geocode": 1}])
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", ingestion_date)

    def test_returns_filename_with_json_extension(self):
        _, filename = lm.upload_to_s3([{"cd_geocode": 1}])
        assert filename.startswith("data_") and filename.endswith(".json")

    def test_body_is_valid_json_with_correct_data(self):
        data = [{"cd_geocode": 3550308, "vl_populacao": 11904961}]
        lm.upload_to_s3(data)
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        parsed = json.loads(kwargs["body"])
        assert parsed[0]["vl_populacao"] == 11904961


########## lambda_handler ##########

class TestLambdaHandler:

    def test_returns_200_on_success(self):
        records = [{"cd_geocode": 1, "vl_populacao": 100}]

        with patch.object(lm, "fetch_all_populacao", return_value=records), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["statusCode"] == 200

    def test_response_includes_total_records(self):
        records = [{"cd_geocode": i} for i in range(645)]

        with patch.object(lm, "fetch_all_populacao", return_value=records), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["total_records"] == 645

    def test_raises_and_sends_email_on_failure(self):
        lm.manager.ses.send_email_on_failure.reset_mock()

        with patch.object(lm, "fetch_all_populacao", side_effect=RuntimeError("API down")):
            with pytest.raises(RuntimeError):
                lm.lambda_handler({}, None)

        lm.manager.ses.send_email_on_failure.assert_called_once()
