# Dashboard Guide

The Streamlit dashboard serves as the front-end for the EpiMind platform. It connects directly to Athena to fetch curated data and provides observability logs.

**Live dashboard:** [https://epimind.com.br/](https://epimind.com.br/)

---

## 1. Surveillance (Vigilância)

This tab visualizes the Gold layer tables, allowing users to drill down into the data across different cities and timeframes.

**General Overview:**
Provides KPIs and graphs about disease incidence.
![Overview](img/dashboard_vigilancia_visao_geral.png)

**Ranking & Critical Analysis:**
Calculates risk scores based on disease thresholds and population data.
![Ranking](img/dashboard_vigilancia_ranking.png)

[Watch Surveillance Demo](videos/Dashboard_vigilância.mp4)

---

## 2. Observability (Observabilidade)

A centralized view of pipeline health. It reads directly from the `execution_logs` table in Athena to show:
- Whether Step Functions, Lambdas, and Glue jobs succeeded or failed.
- The amount of data processed per run.
- Time spent on each step.

![Observability Logs](img/dashboard_observabilidade.png)

[Watch Observability Demo](videos/Dashboard_observabilidade.mp4)

---

## 3. Network Flows (Registro.br & Nginx)

The dashboard is professionally hosted on an EC2 instance, making it fully available on the web without specifying ports.

**How the routing works:**
1. **DNS Registration:** The domain `epimind.com.br` is registered and managed in **Registro.br**.
2. **DNS Resolution:** The domain points to the AWS Elastic IP attached to the EC2 instance.
3. **Nginx Reverse Proxy:** Incoming HTTP/HTTPS traffic hits the EC2 instance, where **Nginx** is listening on port 80. Nginx acts as a reverse proxy, forwarding requests directly to the Streamlit container running internally on port 8501.

This architecture provides a clean URL and a secure entry point for the application.

![Registro.br Custom Domain](img/conta_registro_br.png)
