####################################################################
# Tests for aws/lambda_scripts/BronzeApiCaptureIbgeMunicipios.py
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
import importlib
from unittest.mock import MagicMock, patch, call
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
import BronzeApiCaptureIbgeMunicipios as lm


########## Sample IBGE API response ##########

SAMPLE_IBGE_RAW = [
    {
        "id": 3550308,
        "nome": "São Paulo",
        "microrregiao": {
            "id": 35061,
            "nome": "São Paulo",
            "mesorregiao": {
                "id": 3515,
                "nome": "Metropolitana de São Paulo",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        },
        "regiao-imediata": {
            "id": 350001,
            "nome": "São Paulo",
            "regiao-intermediaria": {
                "id": 3501,
                "nome": "São Paulo",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        }
    },
    {
        "id": 3509502,
        "nome": "Campinas",
        "microrregiao": {
            "id": 35032,
            "nome": "Campinas",
            "mesorregiao": {
                "id": 3507,
                "nome": "Campinas",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        },
        "regiao-imediata": {
            "id": 350005,
            "nome": "Campinas",
            "regiao-intermediaria": {
                "id": 3503,
                "nome": "Campinas",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        }
    },
    {
        "id": 3548500,
        "nome": "Santos",
        "microrregiao": {
            "id": 35063,
            "nome": "Santos",
            "mesorregiao": {
                "id": 3516,
                "nome": "Litoral Sul Paulista",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        },
        "regiao-imediata": {
            "id": 350009,
            "nome": "Santos",
            "regiao-intermediaria": {
                "id": 3505,
                "nome": "Sorocaba",
                "UF": {"id": 35, "sigla": "SP", "nome": "São Paulo"}
            }
        }
    }
]


########## get_json ##########

class TestGetJson:

    def test_returns_parsed_json_on_200(self):
        mock_resp        = MagicMock()
        mock_resp.status = 200
        mock_resp.data   = json.dumps([{"id": 1}]).encode()

        with patch.object(lm.http, "request", return_value=mock_resp):
            result = lm.get_json("https://example.com")

        assert result == [{"id": 1}]

    def test_raises_after_all_retries_on_5xx(self):
        mock_resp        = MagicMock()
        mock_resp.status = 500

        with patch.object(lm.http, "request", return_value=mock_resp), \
             patch("BronzeApiCaptureIbgeMunicipios.time.sleep"):
            with pytest.raises(RuntimeError, match="attempts failed"):
                lm.get_json("https://example.com")

    def test_raises_after_all_retries_on_connection_error(self):
        with patch.object(lm.http, "request", side_effect=urllib3.exceptions.HTTPError("timeout")), \
             patch("BronzeApiCaptureIbgeMunicipios.time.sleep"):
            with pytest.raises(RuntimeError, match="attempts failed"):
                lm.get_json("https://example.com")

    def test_retries_correct_number_of_times(self):
        mock_resp        = MagicMock()
        mock_resp.status = 503

        with patch.object(lm.http, "request", return_value=mock_resp) as mock_req, \
             patch("BronzeApiCaptureIbgeMunicipios.time.sleep"):
            with pytest.raises(RuntimeError):
                lm.get_json("https://example.com")

        assert mock_req.call_count == lm.MAX_RETRIES


########## flatten_municipio ##########

class TestFlattenMunicipio:

    def test_flattens_nested_structure_correctly(self):
        result = lm.flatten_municipio(SAMPLE_IBGE_RAW[0])

        assert result["cd_geocode"]      == 3550308
        assert result["nm_municipio"]    == "São Paulo"
        assert result["nm_microrregiao"] == "São Paulo"
        assert result["nm_mesorregiao"]  == "Metropolitana de São Paulo"

    def test_returns_all_four_fields(self):
        result = lm.flatten_municipio(SAMPLE_IBGE_RAW[1])
        expected_keys = {"cd_geocode", "nm_municipio", "nm_microrregiao", "nm_mesorregiao"}
        assert set(result.keys()) == expected_keys

    def test_handles_missing_nested_keys_gracefully(self):
        raw_incomplete = {"id": 9999, "nome": "Unknown"}
        result = lm.flatten_municipio(raw_incomplete)

        assert result["cd_geocode"]      == 9999
        assert result["nm_municipio"]    == "Unknown"
        assert result["nm_microrregiao"] is None
        assert result["nm_mesorregiao"]  is None


########## fetch_all_municipios ##########

class TestFetchAllMunicipios:

    def test_returns_flattened_records(self):
        with patch.object(lm, "get_json", return_value=SAMPLE_IBGE_RAW):
            result = lm.fetch_all_municipios()

        assert len(result) == 3
        assert result[0]["cd_geocode"] == 3550308
        assert result[1]["nm_municipio"] == "Campinas"

    def test_raises_when_api_returns_empty(self):
        with patch.object(lm, "get_json", return_value=[]):
            with pytest.raises(ValueError, match="empty response"):
                lm.fetch_all_municipios()

    def test_raises_when_api_returns_none(self):
        with patch.object(lm, "get_json", return_value=None):
            with pytest.raises(ValueError, match="empty response"):
                lm.fetch_all_municipios()


########## upload_to_s3 ##########

class TestUploadToS3:

    def setup_method(self):
        lm.manager.s3.put_s3_file.reset_mock()

    def test_calls_s3_put_once(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        lm.manager.s3.put_s3_file.assert_called_once()

    def test_uses_correct_bucket(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert kwargs["bucket"] == "test-bucket"

    def test_key_contains_ibge_path(self):
        lm.upload_to_s3([{"cd_geocode": 1}])
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert "ibge/tb_municipios/" in kwargs["key"]

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

    def test_body_is_valid_json_string(self):
        data = [{"cd_geocode": 3550308, "nm_municipio": "São Paulo"}]
        lm.upload_to_s3(data)
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        parsed = json.loads(kwargs["body"])
        assert parsed[0]["nm_municipio"] == "São Paulo"


########## lambda_handler ##########

class TestLambdaHandler:

    def test_returns_200_on_success(self):
        municipios = [{"cd_geocode": 1}]

        with patch.object(lm, "fetch_all_municipios", return_value=municipios), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["statusCode"] == 200

    def test_response_includes_total_records(self):
        municipios = [{"cd_geocode": i} for i in range(645)]

        with patch.object(lm, "fetch_all_municipios", return_value=municipios), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["total_records"] == 645

    def test_response_includes_ingestion_date_and_filename(self):
        with patch.object(lm, "fetch_all_municipios", return_value=[{"cd_geocode": 1}]), \
             patch.object(lm, "upload_to_s3", return_value=("2026-03-25", "data_120000.json")):
            response = lm.lambda_handler({}, None)

        assert response["ingestion_date"] == "2026-03-25"
        assert response["filename"] == "data_120000.json"

    def test_raises_and_sends_email_on_failure(self):
        lm.manager.ses.send_email_on_failure.reset_mock()

        with patch.object(lm, "fetch_all_municipios", side_effect=RuntimeError("API down")):
            with pytest.raises(RuntimeError):
                lm.lambda_handler({}, None)

        lm.manager.ses.send_email_on_failure.assert_called_once()
