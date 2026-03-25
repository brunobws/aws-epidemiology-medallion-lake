####################################################################
# Tests for aws/lambda_scripts/BronzeS3CaptureSinan.py
#
# Strategy: the Lambda executes AWS calls at module-level (Logs,
# Dynamo, AwsManager). We inject mocks into sys.modules BEFORE
# importing the Lambda so no real AWS calls are made.
# All tests run fully offline — no credentials required.
####################################################################

import sys
import os
import io
import json
import re
import zipfile
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
import BronzeS3CaptureSinan as lm


########## Helper: create a test ZIP with CSV ##########

def create_test_zip(csv_content: str, csv_name: str = "DENGBR25.csv") -> bytes:
    """Creates an in-memory ZIP file containing a single CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_content.encode('latin-1'))
    return buf.getvalue()


SAMPLE_CSV = "ID;DT_NOTIFIC;SG_UF;NM_MUNICIPIO\n001;01/01/2025;35;Sao Paulo\n002;02/01/2025;35;Campinas\n"
SAMPLE_ZIP = create_test_zip(SAMPLE_CSV)


########## build_source_url ##########

class TestBuildSourceUrl:

    def test_builds_dengue_url_correctly(self):
        url = lm.build_source_url("Dengue", "DENGBR", 2025)
        assert url == "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/csv/DENGBR25.csv.zip"

    def test_builds_chikungunya_url_correctly(self):
        url = lm.build_source_url("Chikungunya", "CHIKBR", 2024)
        assert url == "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Chikungunya/csv/CHIKBR24.csv.zip"

    def test_builds_zika_url_correctly(self):
        url = lm.build_source_url("Zika", "ZIKABR", 2023)
        assert url == "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Zika/csv/ZIKABR23.csv.zip"

    def test_extracts_last_two_digits_of_year(self):
        url = lm.build_source_url("Dengue", "DENGBR", 2000)
        assert "DENGBR00.csv.zip" in url


########## download_bytes ##########

class TestDownloadBytes:

    def test_returns_bytes_on_200(self):
        mock_resp        = MagicMock()
        mock_resp.status = 200
        mock_resp.data   = b"fake zip content"

        with patch.object(lm.http, "request", return_value=mock_resp):
            result = lm.download_bytes("https://example.com/file.zip")

        assert result == b"fake zip content"

    def test_returns_none_on_404(self):
        mock_resp        = MagicMock()
        mock_resp.status = 404

        with patch.object(lm.http, "request", return_value=mock_resp):
            result = lm.download_bytes("https://example.com/missing.zip")

        assert result is None

    def test_raises_after_all_retries_on_5xx(self):
        mock_resp        = MagicMock()
        mock_resp.status = 500

        with patch.object(lm.http, "request", return_value=mock_resp), \
             patch("BronzeS3CaptureSinan.time.sleep"):
            with pytest.raises(RuntimeError, match="attempts failed"):
                lm.download_bytes("https://example.com")

    def test_retries_correct_number_of_times(self):
        mock_resp        = MagicMock()
        mock_resp.status = 503

        with patch.object(lm.http, "request", return_value=mock_resp) as mock_req, \
             patch("BronzeS3CaptureSinan.time.sleep"):
            with pytest.raises(RuntimeError):
                lm.download_bytes("https://example.com")

        assert mock_req.call_count == lm.MAX_RETRIES


########## extract_csv_from_zip ##########

class TestExtractCsvFromZip:

    def test_extracts_csv_content(self):
        result = lm.extract_csv_from_zip(SAMPLE_ZIP)
        assert "ID;DT_NOTIFIC;SG_UF;NM_MUNICIPIO" in result
        assert "Sao Paulo" in result

    def test_raises_when_no_csv_in_zip(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr("readme.txt", "no csv here")

        with pytest.raises(ValueError, match="No CSV file found"):
            lm.extract_csv_from_zip(buf.getvalue())

    def test_handles_latin1_encoding(self):
        csv_with_accents = "ID;NOME\n001;São Paulo\n002;Araraquará\n"
        zip_bytes = create_test_zip(csv_with_accents)
        result = lm.extract_csv_from_zip(zip_bytes)
        assert "São Paulo" in result
        assert "Araraquará" in result


########## upload_csv_to_s3 ##########

class TestUploadCsvToS3:

    def setup_method(self):
        lm.manager.s3.put_s3_file.reset_mock()

    def test_calls_s3_put_once(self):
        lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        lm.manager.s3.put_s3_file.assert_called_once()

    def test_uses_correct_bucket(self):
        lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert kwargs["bucket"] == "test-bucket"

    def test_key_contains_sinan_path(self):
        lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert "sinan/tb_notificacoes/" in kwargs["key"]

    def test_key_contains_ingestion_date_partition(self):
        ingestion_date, _ = lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        kwargs = lm.manager.s3.put_s3_file.call_args.kwargs
        assert f"ingestion_date={ingestion_date}" in kwargs["key"]

    def test_filename_includes_disease_and_year(self):
        _, filename = lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        assert filename == "dengue_2025.csv"

    def test_returns_valid_ingestion_date_format(self):
        ingestion_date, _ = lm.upload_csv_to_s3(SAMPLE_CSV, "dengue", 2025)
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", ingestion_date)


########## lambda_handler ##########

class TestLambdaHandler:

    def test_returns_200_on_success(self):
        with patch.object(lm, "download_bytes", return_value=SAMPLE_ZIP), \
             patch.object(lm, "upload_csv_to_s3", return_value=("2026-03-25", "dengue_2025.csv")):
            response = lm.lambda_handler({"years": [2025]}, None)

        assert response["statusCode"] == 200

    def test_response_includes_files_list(self):
        with patch.object(lm, "download_bytes", return_value=SAMPLE_ZIP), \
             patch.object(lm, "upload_csv_to_s3", return_value=("2026-03-25", "dengue_2025.csv")):
            response = lm.lambda_handler({"years": [2025]}, None)

        assert len(response["files"]) == 3  # 3 diseases x 1 year

    def test_skips_missing_files_gracefully(self):
        def mock_download(url):
            if "Zika" in url:
                return None  # 404
            return SAMPLE_ZIP

        with patch.object(lm, "download_bytes", side_effect=mock_download), \
             patch.object(lm, "upload_csv_to_s3", return_value=("2026-03-25", "dengue_2025.csv")):
            response = lm.lambda_handler({"years": [2025]}, None)

        assert response["statusCode"] == 200
        assert len(response["files"]) == 2  # Only dengue and chikungunya

    def test_raises_when_no_files_downloaded(self):
        with patch.object(lm, "download_bytes", return_value=None):
            with pytest.raises(ValueError, match="No SINAN files"):
                lm.lambda_handler({"years": [2025]}, None)

    def test_raises_and_sends_email_on_failure(self):
        lm.manager.ses.send_email_on_failure.reset_mock()

        with patch.object(lm, "download_bytes", side_effect=RuntimeError("Network error")):
            with pytest.raises(RuntimeError):
                lm.lambda_handler({"years": [2025]}, None)

        lm.manager.ses.send_email_on_failure.assert_called_once()
