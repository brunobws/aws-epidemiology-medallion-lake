# AI Guide (IA Analista)

A unique feature of the EpiMind project is the integration of an AI Assistant that understands the underlying AWS architecture, the epidemiological context, and the data schemas. 

Users can ask complex questions using natural language to extract deep insights without writing a single line of SQL.

---

## 🔄 How It Works (Architecture Flow)

The integration uses a multi-step orchestration entirely managed in Python via Streamlit and `boto3`.

```mermaid
sequenceDiagram
    participant User
    participant Streamlit
    participant Bedrock as AWS Bedrock<br/>(Claude Haiku)
    participant Athena as AWS Athena<br/>(Data Lake)

    User->>Streamlit: Asks question in natural language
    Streamlit->>Bedrock: Sends Question + Database Schema Context
    Bedrock-->>Streamlit: Returns optimized SQL Query
    Streamlit->>Athena: Executes SQL Query on Gold/Silver layers
    Athena-->>Streamlit: Returns raw data results (DataFrame)
    Streamlit->>Bedrock: Sends original Question + Raw Data Results
    Bedrock-->>Streamlit: Returns final human-readable analysis
    Streamlit-->>User: Displays Text Analysis & DataFrame UI
```

1. **User Prompt:** The user types a question in the Streamlit UI.
2. **Schema Injection:** The backend injects the Athena schema (tables, column definitions, available cities, and thresholds) into the LLM context.
3. **Query Generation:** The LLM translates the question into an optimized Athena SQL query.
4. **Execution:** The system executes the query securely on the Data Lake via `PyAthena`.
5. **Answer Generation:** The LLM receives the numerical/categorical results from Athena and summarizes them in a human-readable format.

> *"What is the current dengue situation in Sorocaba?"*

![AI Prompt](img/dashboard_IA_pergunta_sorocaba.png)
![AI Answer](img/dashboard_IA_pergunta_dengue.png)
![AI Details](img/dashboard_IA_pergunta_dengue_2.png)

---

## 🧠 Model Selection & Costs

The AI Analyst is powered by **Anthropic Claude Haiku**, provisioned via **AWS Bedrock** (`global.anthropic.claude-haiku`).

**Why Claude Haiku?**
- **Speed:** It is optimized for near-instant responses, which is critical for maintaining a fluid, interactive dashboard experience.
- **Cost-Efficiency:** As an enterprise dashboard, scanning large Athena tables and hitting LLM APIs can add up quickly. Claude Haiku offers excellent reasoning capabilities for SQL generation at a fraction of the cost of heavier models (costing roughly **$0.25 per million input tokens**).

---

## 🚦 Usage Limits

To prevent abuse and tightly control AWS costs (both from Bedrock API calls and Athena Data Scanned), the platform implements a strict throttle:
- **5 Questions per User:** Once a user reaches 5 interactions, the chat input is gracefully disabled. This limit guarantees that the cloud expenditure for the demonstration environment remains highly predictable and sustainable.

---

## 🛡️ Smart Filtering (Out of Scope Guardrails)

Because interacting with the database costs compute resources, the AI is configured with strict guardrails within its system prompt.

If a user asks a question entirely unrelated to epidemiology or outside the available arbovirus data scope, the AI intelligently flags it as out-of-scope and refuses to execute an Athena query.

This protects the system from "hallucinated" queries and unnecessary AWS Athena scan costs.

![Out of Scope Handling](img/dashboard_IA_fora_do_escopo.png)

[Watch AI Analyst Demo](videos/Dasboard_IA.mp4)
