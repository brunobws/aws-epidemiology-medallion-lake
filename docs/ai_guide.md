# AI Guide (IA Analista)

A unique feature of the EpiMind project is the integration of an AI Assistant that understands the underlying AWS architecture, the epidemiological context, and the data schemas. 

Users can ask complex questions using natural language to extract deep insights without writing a single line of SQL.

---

## How It Works

1. **User Prompt:** The user types a question in the Streamlit UI.
2. **Schema Injection:** The backend injects the Athena schema (tables, column definitions, available cities, and thresholds) into the LLM context.
3. **Query Generation:** The LLM translates the question into an optimized Athena SQL query.
4. **Execution & Answer:** The system executes the query on the Data Lake and returns the data, which the AI then summarizes in a human-readable format.

> *"What is the current dengue situation in Sorocaba?"*

![AI Prompt](img/dashboard_IA_pergunta_sorocaba.png)
![AI Answer](img/dashboard_IA_pergunta_dengue.png)
![AI Details](img/dashboard_IA_pergunta_dengue_2.png)

---

## Smart Filtering (Out of Scope Guardrails)

Because interacting with the database costs compute resources (Athena charges per TB scanned), the AI is configured with strict guardrails.

If a user asks a question entirely unrelated to epidemiology or outside the available arbovirus data scope, the AI intelligently flags it as out-of-scope and refuses to execute a query.

This protects the system from "hallucinated" queries and unnecessary AWS costs.

![Out of Scope Handling](img/dashboard_IA_fora_do_escopo.png)

[Watch AI Analyst Demo](videos/Dasboard_IA.mp4)
