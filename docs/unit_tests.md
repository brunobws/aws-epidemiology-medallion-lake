# Unit Tests & QA

The pipeline includes a robust suite of **88 Pytest Unit Tests**. The objective of these tests is to guarantee that the logic functions as expected before the code ever touches the AWS environment.

All tests run **fully offline** — no AWS credentials, no network access, no infrastructure required.

---

## How to Run
**Install pytest:**
```bash
pip install pytest
```

**Run all tests:**
```bash
python -m pytest tests/ -v
```

---

## Test Files
| File | Module Tested |
|---|---|
| [tests/test_lambda_ibge_municipios.py](../tests/test_lambda_ibge_municipios.py) | `aws/scripts/lambda_scripts/BronzeApiCaptureIbgeMunicipios.py` |
| [tests/test_lambda_ibge_populacao.py](../tests/test_lambda_ibge_populacao.py) | `aws/scripts/lambda_scripts/BronzeApiCaptureIbgePopulacao.py` |
| [tests/test_lambda_infodengue.py](../tests/test_lambda_infodengue.py) | `aws/scripts/lambda_scripts/BronzeApiCaptureInfoDengue.py` |
| [tests/test_lambda_sinan.py](../tests/test_lambda_sinan.py) | `aws/scripts/lambda_scripts/BronzeS3CaptureSinan.py` |

**Total: 88 tests**

### 1. `test_lambda_ibge_municipios.py`
Tests the extraction of IBGE spatial data.
- **TestGetJson:** Mocks HTTP 200, 500, and Connection Errors to ensure the retry mechanism and JSON parsing work.
- **TestUploadToS3:** Ensures S3 upload is called with the correct bucket (`S3_BUCKET` env), proper partitioning (`ingestion_date=YYYY-MM-DD`), and valid JSON bodies.
- **TestLambdaHandler:** Validates that the handler returns a 200 HTTP status and correctly emails engineers upon exceptions.

### 2. `test_lambda_ibge_populacao.py`
Tests the population ingestion.
- Similar structure to Municipios, but asserts specific population JSON keys and transformations.

### 3. `test_lambda_infodengue.py`
Tests the highly complex InfoDengue API logic.
- **TestFlattenAlertRecord:** Asserts that the nested JSON from InfoDengue is correctly flattened, geographical codes are extracted, and epidemiological weeks are parsed accurately.
- **TestApiPagination:** Validates that the Lambda correctly paginates across 645 cities in São Paulo without timing out.

### 4. `test_lambda_sinan.py`
Tests the extraction of CSVs from government open data ZIP files.
- **TestUploadCsvToS3:** Verifies that large files are streamed to S3.
- **TestLambdaHandler:** Validates that the handler accurately loops over years and diseases (Dengue, Zika, Chikungunya) and generates the correct manifest response.

---

## What Else Can Be Tested? (Next Steps)
While our Lambda (Bronze) ingestion layer is heavily tested, we can expand our suite to other layers:

1. **Shared Modules (`aws/modules/`):**
   - We should add tests for `support.py` (e.g. testing the `eval_values` function that caused the DynamoDB bug).
   - Tests for `logs.py` to ensure it formats the S3 Parquet schema correctly.
2. **PySpark Glue Jobs (`aws/scripts/glue_scripts/`):**
   - Using `pytest-spark` and `chispa`, we can spin up a local Spark Session during tests. We can pass a mock DataFrame into `bronze_to_silver.py` and assert that columns were correctly renamed, cast to integers/dates, and partitioned.

---

## The Role of `conftest.py`
The `conftest.py` file is a special pytest configuration file. 
Since our Python code is buried in `aws/scripts/lambda_scripts/` and `aws/modules/`, running `pytest` from the root directory normally causes `ModuleNotFoundError` (because Python doesn't know where to find the files). 
Our `conftest.py` dynamically injects these internal paths into the `sys.path` before any test runs, allowing the tests to easily `import BronzeApiCaptureInfoDengue` as if it were a local library.

---

## Infrastructure Testing (Terraform in CI/CD)
**Is GitHub Actions blocking a deployment considered a test?**
Yes! Beyond Python unit tests, our CI/CD pipeline acts as an **Infrastructure Test**. 

Since we use Terraform (`.github/workflows/aws_infrastructure.yml`), the `terraform validate` and `terraform plan` commands ensure our AWS architecture has no syntax errors or cyclic dependencies before applying. 
*Tip: To further enhance our "esteira", we could add `tflint` (to enforce cloud best practices) and `tfsec` (to scan for security vulnerabilities like open S3 buckets) directly into the GitHub Actions pipeline!*
