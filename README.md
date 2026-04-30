  # EpiMind Data Platform on AWS

![AWS](https://img.shields.io/badge/AWS-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![AWS Step Functions](https://img.shields.io/badge/Step_Functions-FF4F8B?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![AI](https://img.shields.io/badge/Artificial%20Intelligence-000000?style=for-the-badge&logo=openai&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6B6B?style=for-the-badge&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=for-the-badge&logo=terraform&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-4479A1?style=for-the-badge&logo=github-actions&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

A production-grade data platform that monitors arbovirus epidemiological data (like Dengue) in the state of São Paulo, Brazil. It ingests data through a Medallion Architecture, integrating Artificial Intelligence (AI) for advanced data querying, and serves analytics via a web Streamlit dashboard hosted on a custom domain https://epimind.com.br/. 

Running on AWS with modularized logging, automated data quality testing, and email notifications for pipeline failures, with an event-configuration driven architecture. The platform currently extracts from 4 public APIs, treating and ingesting more than 3 Million records. Fully deployed using Terraform (IaC) and automated via GitHub Actions CI/CD.

## Table of Contents

- [Live Dashboard](#live-dashboard)
- [Architecture Overview](#architecture-overview)
- [How It Works](#how-it-works)
- [Cloud Setup](#cloud-setup)
- [Local Setup](#local-setup)
- [Technology Stack](#technology-stack)
- [Key Features](#key-features)
- [AI Analyst Integration](#ai-analyst-integration)
- [Documentation](#documentation)
- [Code Organization](#code-organization)
- [Infrastructure & CI/CD](#infrastructure--cicd)
- [Testing](#testing)
- [Future Enhancements](#future-enhancements)

<a id="live-dashboard"></a>

## Live Dashboard 

**Access the dashboard:** [https://epimind.com.br/](https://epimind.com.br/)

![Dashboard Screenshot](docs/img/02_dashboard/01_overview.png)

Interactive analytics with three main sections:

- **Surveillance (Vigilância)** – View general indicators and a detailed epidemiological ranking of cities.
- **AI Analyst (IA Analista)** – Ask questions using natural language to an AI assistant powered by your Data Lake.
- **Observability** – Real-time logs showing successful pipeline runs, errors, performance metrics, and data quality results (BDQ Tests).

Watch demo videos to see the dashboard in action:

- [Surveillance Dashboard Demo](docs/videos/Dashboard_vigilância.mp4)
- [AI Analyst Demo](docs/videos/Dasboard_IA.mp4)
- [Observability Logs Demo](docs/videos/Dashboard_observabilidade.mp4)

> [!NOTE]
> For detailed dashboard and AI documentation, see [Dashboard Guide](docs/dashboard.md).

<a id="architecture-overview"></a>

## Architecture Overview

![EpiMind Architecture](docs/img/01_architecture/01_main_architecture.jpg)

The pipeline runs entirely on a serverless AWS stack, orchestrated by AWS Step Functions and provisioned using Terraform. Data flows through three layers:

- **Bronze** – Raw data from APIs, preserved in S3 for full reprocessability.
- **Silver** – PySpark-transformed [Parquet](https://parquet.apache.org/) files, partitioned for efficient querying.
- **Gold** – Pre-aggregated and modeled tables queryable via Athena.

> [!NOTE]
> [Full architecture documentation →](docs/architecture.md)
> [Interactive Miro Dashboard →](https://miro.com/app/board/uXjVHagcgOk=/?share_link_id=204214847785)

<a id="how-it-works"></a>
 
## How It Works

The automated pipeline is orchestrated by **AWS Step Functions**:

1. **Ingestion** – AWS Lambda extracts data via API and stores it in S3 Bronze.
2. **Transformations (Glue)** – AWS Glue jobs clean, partition, and transform data from Bronze to Silver, and then aggregate it into the Gold layer using Athena SQL.
3. **AI Integration** – The dashboard sends user queries to an AI service that dynamically translates natural language into insights over the curated data.
4. **Configuration Driven** – Glue jobs and Lambda functions are entirely driven by parameters stored in DynamoDB, meaning no code changes are required to add new tables.
5. **Observability** – Structured execution logs are written to an Athena table, providing full execution context and performance monitoring shown in the Streamlit app.

<a id="cloud-setup"></a>

## Cloud Setup ☁️

All components run on AWS infrastructure. 

> [!NOTE]
> A dedicated read-only user was created for you to safely explore the data lake. Access it with the credentials below.

```
AWS Console: https://580148408154.signin.aws.amazon.com/console
User: datalake-reader
```

Password: [Request access via WhatsApp](https://wa.me/5515997595138?text=Hi%2C+I%27d+like+to+request+the+read-only+AWS+console+password+for+the+EpiMind+project.)

With read-only access, you can:
- Browse S3 layers and Athena queries.
- View Step Functions executions.
- Access DynamoDB configuration tables.
- View Terraform state if applicable.

<a id="local-setup"></a>

## Local Setup 💻

If you want to run the project locally, there are two simple ways to initialize the dashboard and environment:

- **Using Windows Batch:** Simply double-click the `.bat` file in the project root. It will automatically set up the environment and launch the Streamlit dashboard.
- **Using PowerShell:** Run the `.ps1` script to activate the virtual environment and initialize all local dependencies.

<a id="technology-stack"></a>

## Technology Stack

| Component | Technology |
|-----------|------------|
| **Cloud** | [AWS Lambda](https://docs.aws.amazon.com/lambda/), [Glue](https://docs.aws.amazon.com/glue/), [Athena](https://docs.aws.amazon.com/athena/), [S3](https://docs.aws.amazon.com/s3/), [DynamoDB](https://docs.aws.amazon.com/dynamodb/), [Step Functions](https://aws.amazon.com/step-functions/), [EventBridge](https://aws.amazon.com/eventbridge/) |
| **IaC & CI/CD**| [Terraform](https://www.terraform.io/), [GitHub Actions](https://github.com/features/actions) |
| **Orchestration** | AWS Step Functions |
| **Processing** | Python, PySpark, SQL |
| **Dashboard** | Streamlit, hosted on EC2 with a custom domain (registro.br) |
| **Data Format** | [Parquet](https://parquet.apache.org/) |

<a id="key-features"></a>

## Key Features

**Terraform IaC & CI/CD** – The entire AWS infrastructure is mapped as code and automatically deployed via GitHub Actions pipelines.

**AWS Step Functions** – Serverless orchestration for all ETL steps, providing visual state machines and eliminating the overhead of managing Apache Airflow.

**AI Integration** – Seamless connection with LLMs within the dashboard to translate human questions into data insights.

**Modularized Logging** – All components (Lambda, Glue jobs) use a centralized Logs class that writes structured execution records with step-level timing to an Athena table. Each log includes job name, status, warnings, errors, and custom metadata.

**Business Data Quality (BDQ)** – Automated validation tests built on Great Expectations check data completeness, accuracy, and consistency at each layer. Quality metrics are stored in Athena for historical analysis and trend detection, acting as an active firewall against corrupted government APIs.

**Email Alerting** – Configurable email notifications using AWS SES for pipeline failures and data quality reports. Alert recipients and thresholds are managed entirely in DynamoDB.

**Generic Processing Engines** – Both Glue jobs are designed as configuration-driven engines. Pass different DynamoDB parameters and they process entirely different datasets without touching the code.

**DynamoDB Configuration (Zero-Hardcoding)** – Pipeline parameters, notification settings, and data quality thresholds are stored dynamically in DynamoDB. The platform operates on a strict "Zero-Hardcoding Architecture".

**Optimized Storage** – Silver and Gold layers use partitioning by date for query performance. Gold layer uses Parquet for cost-efficient query performance via Athena.

**Custom Domain Setup** – The dashboard is exposed professionally via `epimind.com.br`, managed through `registro.br` and AWS.

<a id="ai-analyst-integration"></a>

## 🤖 AI Analyst Integration

EpiMind features an embedded AI assistant powered by **Anthropic Claude Haiku** via AWS Bedrock. Users can ask epidemiological questions in natural language, and the system dynamically translates them into safe, optimized Athena SQL queries, fetches the results, and returns a human-readable analysis. The AI is strictly bound by custom prompts to prevent hallucinations and SQL injections.

> [!NOTE]
> [Read the full AI Guide (Flowchart, Bedrock Code & System Prompts) →](docs/ai_guide.md)

<a id="documentation"></a>

## Documentation

- [Architecture](docs/architecture.md) – Design patterns, data flow, component details
- [Dashboard Guide](docs/dashboard.md) – Using Streamlit analytics and Network flows
- [AI Guide](docs/ai_guide.md) – Explaining how the AI Analyst works
- [Infrastructure as Code & CI/CD](docs/infrastructure.md) – Terraform configuration, and GitHub Actions
- [Modules](docs/modules.md) – Shared Python modules: Logs, Quality, AwsManager
- [DynamoDB Parameters](docs/dynamo_params.md) – Pipeline configuration tables
- [Unit Tests](docs/unit_tests.md) – Full test reference with per-class test tables

<a id="code-organization"></a>

## Code Organization 📂

- `aws/terraform/` – Infrastructure as Code definitions.
- `aws/step_functions/` – Step Functions definitions.
- `aws/scripts/` – AWS Lambda and Glue Python scripts.
- `aws/sql/` – Athena queries for Gold layer aggregation.
- `aws/dynamo_params/` – Configurations loaded into DynamoDB.
- `streamlit_app/` – Streamlit analytics interface and AI logic.
- `.github/workflows/` – CI/CD automation pipelines.

<a id="infrastructure--cicd"></a>

## Infrastructure & CI/CD

The whole project runs as Code. See the [Infrastructure Guide](docs/infrastructure.md) for how Terraform manages the deployment, and how GitHub Actions automates changes to Lambdas, Glue, and the Step Functions workflow.

<a id="testing"></a>

## Testing

**Unit Tests** – A robust suite of **117 pytest tests** covering the shared modules, Lambda ingestion logic, and PySpark transformations (`pyspark_utils`). All tests are runnable fully offline without AWS dependencies (by mocking AWS services via `moto` and simulating local Spark DataFrames). See the `tests/` directory and `requirements-dev.txt` for details.

<a id="future-enhancements"></a>

## Future Enhancements

- **Granular Spatial Analytics:** Refactoring the Gold layer ingestion pipeline to support neighborhood-level (Bairro/CEP) epidemiological granularity, allowing pinpoint detection of outbreaks within cities.
- **Enhanced CI/CD Security:** Integrating `tfsec` and `tflint` into the GitHub Actions pipeline to catch infrastructure vulnerabilities automatically.


---

## Questions or Feedback

Thanks for reading! If you have any questions about the pipeline or would like to discuss the architecture, feel free to reach out.

**Contact:**
- Email: brun0ws@outlook.com
- LinkedIn: [Bruno Silva](https://www.linkedin.com/in/brunowds/)
- WhatsApp: [Message me](https://wa.me/5515997595138)
- Phone: +55 15 99759-5138
