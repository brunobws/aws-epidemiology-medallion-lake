####################################################################
# Author: Bruno William da Silva
# Date: 17/04/2026
#
# Description:
#   AWS Bedrock service for EpiMind IA Analista feature.
#   Implements a two-step Text-to-SQL pipeline:
#     Step 1 — LLM generates a safe SELECT-only SQL query.
#     Step 2 — LLM receives Athena results and returns a full
#               epidemiological analysis in Brazilian Portuguese.
#
#   Key Features:
#   - Claude claude-haiku via AWS Bedrock (InvokeModel API)
#   - Strict SQL safety validation (SELECT-only, no DDL/DML)
#   - Scope guard — politely rejects off-topic questions
#   - Full table schema embedded in system prompt
#   - Risk Score concept (Rt + relative incidence) included
####################################################################

########### imports ################
import json
import re
import boto3
from typing import Optional
from utils.logger import get_logger
###################################

logger = get_logger(__name__)


####################################################################
# CONSTANTS
####################################################################
BEDROCK_REGION       = "sa-east-1"
# Claude Haiku 4.5 in sa-east-1 only supports INFERENCE_PROFILE throughput.
# The system-defined cross-region profile ARN is used instead of the bare model ID.
BEDROCK_MODEL_ID     = "arn:aws:bedrock:sa-east-1:580148408154:inference-profile/global.anthropic.claude-haiku-4-5-20251001-v1:0"
BEDROCK_MAX_TOKENS   = 4096
BEDROCK_TEMPERATURE  = 0.0   # deterministic SQL generation

# Keywords that indicate an unsafe SQL statement (non-SELECT)
_FORBIDDEN_SQL_KEYWORDS = {
    "insert", "update", "delete", "drop", "create", "alter",
    "truncate", "replace", "merge", "exec", "execute", "grant",
    "revoke", "call", "begin", "commit", "rollback",
}


####################################################################
# SYSTEM PROMPT — schema + rules for the LLM
####################################################################
_SYSTEM_PROMPT = """\
You are EpiMind, an expert epidemiological data analyst embedded in a
Streamlit dashboard for arboviral disease surveillance in São Paulo, Brazil.
You ONLY answer questions about dengue, chikungunya, and zika in the state
of São Paulo (SP). If the user asks about anything outside this scope,
politely refuse in Brazilian Portuguese and explain your limitations.

=======================================================================
DATABASE: gold   (AWS Athena / Apache Hive metastore)
ALL TABLES HAVE DATA ONLY FOR 2026. Always default to 2026 when the user
does not specify a year.
=======================================================================

--- TABLE 1: tb_ft_alerta_semanal ---
Granularity: municipality + disease + epidemiological week
Partitions : nr_ano_epi (int), ds_doenca (string), dt_semana_epidemiologica (date)

Columns:
  cd_geocode        int     -- IBGE geocode (7 digits)
  nm_municipio      string  -- municipality name
  nm_microrregiao   string  -- IBGE micro-region
  nm_mesorregiao    string  -- IBGE meso-region
  vl_populacao      int     -- population estimate
  nr_semana_epi     int     -- epidemiological week number (1-53)
  nr_nivel_alerta   int     -- alert level: 1=green, 2=yellow, 3=orange, 4=red
  ds_nivel_alerta   string  -- alert label: verde, amarelo, laranja, vermelho
  vl_casos          int     -- confirmed cases in the week
  vl_casos_estimados double -- estimated cases (epidemiological model)
  vl_incidencia     double  -- incidence per 100k inhabitants in the week
  vl_rt             double  -- reproduction number (Rt); Rt > 1 = epidemic growth
  fl_epidemia       int     -- epidemic flag: 1 when Rt > 1 AND transmission confirmed
  fl_transmissao    int     -- confirmed active transmission (0/1)
  fl_receptividade  int     -- climate receptivity (0/1)
  vl_temp_min       double  -- min temperature (°C)
  vl_temp_max       double  -- max temperature (°C)
  vl_umid_min       double  -- min relative humidity (%)
  vl_umid_max       double  -- max relative humidity (%)

--- TABLE 2: tb_ft_perfil_demografico ---
Granularity: municipality + disease + age group + sex + notification month
Partitions : nr_ano_notificacao (int), nr_mes_notificacao (int)

Columns:
  cd_geocode_ibge      int    -- IBGE code (6 digits, SINAN standard)
  nm_municipio         string -- municipality name
  nm_microrregiao      string -- IBGE micro-region
  nm_mesorregiao       string -- IBGE meso-region
  id_agravo            string -- CID-10: A90=dengue, A92=chikungunya, A92.8=zika
  ds_doenca            string -- disease name: dengue, chikungunya, zika
  ds_faixa_etaria      string -- age group: 0-4, 5-14, 15-29, 30-59, 60+
  cs_sexo              string -- sex: M=male, F=female, I=unknown
  nr_notificacoes      int    -- total notifications (includes under investigation)
  nr_casos_confirmados int    -- confirmed cases (classi_fin = 10)
  nr_obitos            int    -- deaths (evolucao=3) or under investigation (evolucao=9)
  nr_curas             int    -- recovered cases (evolucao=1)

--- TABLE 3: tb_ft_ranking_anual ---
Granularity: municipality + disease + year
Partitions : nr_ano_epi (int), ds_doenca (string)

Columns:
  cd_geocode                  int    -- IBGE geocode (7 digits)
  nm_municipio                string -- municipality name
  nm_microrregiao             string -- IBGE micro-region
  nm_mesorregiao              string -- IBGE meso-region
  vl_populacao                int    -- population estimate
  vl_total_casos              int    -- total confirmed cases in the year
  vl_incidencia_acumulada     double -- cumulative annual incidence per 100k
  nr_max_alerta               int    -- max alert level reached (1-4)
  nr_semanas_alerta_vermelho  int    -- weeks at red alert (level 4)
  nr_semanas_alerta_alto      int    -- weeks at high/critical alert (level >= 3)
  nr_semanas_transmissao_ativa int   -- weeks with confirmed active transmission
  nr_semanas_rt_acima_1       int    -- weeks with Rt > 1 (epidemic growth)
  vl_rt_medio                 double -- average Rt across the year
  nr_rank_estado              int    -- state ranking by cumulative incidence (1 = worst)
  nr_rank_mesorregiao         int    -- meso-region ranking by cumulative incidence

=======================================================================
RISK SCORE CONCEPT (mention naturally when relevant in analysis):
  Risk Score = Rt velocity + relative incidence burden
  A municipality is HIGH RISK when BOTH:
    - Rt > 1.2 (fast epidemic growth)
    - High relative incidence (small city with high rate/100k is MORE critical
      than a metropolis with a lower rate)
  Example phrasing: "São João do Pau d'Alho has a high risk score because
  it combines elevated Rt with very high incidence for a small municipality."
=======================================================================

CRITICAL SQL RULES:
1. Generate ONLY SELECT statements — no INSERT, UPDATE, DELETE, DROP, etc.
2. Always filter partitions explicitly:
   - tb_ft_alerta_semanal / tb_ft_ranking_anual: WHERE nr_ano_epi = 2026
   - tb_ft_perfil_demografico: WHERE nr_ano_notificacao = 2026
3. Use LOWER() for string comparisons on ds_doenca and nm_municipio.
4. Limit results to at most 200 rows to control cost: add LIMIT 200 unless
   the query is already bounded by GROUP BY aggregates producing few rows.
5. Do NOT use subqueries inside FROM with aliases when a simpler flat query
   suffices — keep SQL readable and Athena-compatible (Presto dialect).
6. Always use the exact table names above — no schema prefix needed.
"""


####################################################################
# BEDROCK SERVICE
####################################################################
class BedrockService:
    """
    Thin wrapper around AWS Bedrock (InvokeModel) for EpiMind.

    Two public methods:
      generate_sql(question)        -> str (SQL or refusal message)
      generate_analysis(q, sql, df) -> str (epidemiological narrative)
    """

    def __init__(self, region: str = BEDROCK_REGION, model_id: str = BEDROCK_MODEL_ID):
        """
        Initialize the Bedrock client.

        Args:
            region:   AWS region where Bedrock is available.
            model_id: Anthropic Claude model ID on Bedrock.
        """
        self.model_id = model_id
        self.client   = boto3.client("bedrock-runtime", region_name=region)
        logger.info(f"BedrockService initialized: model={model_id}, region={region}")

    # ── private helpers ──────────────────────────────────────────────

    def _invoke(self, user_message: str, temperature: float = BEDROCK_TEMPERATURE) -> str:
        """
        Send a message to Bedrock and return the assistant text response.

        Args:
            user_message: The human turn content.
            temperature:  Sampling temperature (0 = deterministic).

        Returns:
            Stripped text from the model's response.

        Raises:
            RuntimeError: If the Bedrock call fails.
        """
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": BEDROCK_MAX_TOKENS,
            "temperature": temperature,
            "system": _SYSTEM_PROMPT,
            "messages": [
                {"role": "user", "content": user_message}
            ],
        }

        try:
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )
            result = json.loads(response["body"].read())
            text   = result["content"][0]["text"].strip()
            logger.info(f"Bedrock response received ({len(text)} chars)")
            return text

        except Exception as exc:
            logger.error(f"Bedrock invocation failed: {exc}")
            raise RuntimeError(f"Bedrock call failed: {exc}") from exc

    # ── public methods ───────────────────────────────────────────────

    def generate_sql(self, question: str) -> Optional[str]:
        """
        Ask the LLM to generate a safe SELECT SQL for the given question.

        Returns None if the question is out of scope (LLM refuses).
        Returns the SQL string if generation is successful.

        Args:
            question: Natural language question from the user.

        Raises:
            RuntimeError: If the Bedrock call itself fails.
        """
        prompt = (
            f"The user asked: \"{question}\"\n\n"
            "If the question is outside your scope (not about dengue, "
            "chikungunya, or zika in São Paulo), respond ONLY with the "
            "exact token: OUT_OF_SCOPE\n\n"
            "Otherwise, respond ONLY with a valid Presto/Athena SQL SELECT "
            "statement — no markdown, no explanation, no code fences. "
            "Just the raw SQL ending with a semicolon."
        )

        raw = self._invoke(prompt, temperature=0.0)
        logger.info(f"SQL generation raw output (first 200 chars): {raw[:200]}")

        # Out-of-scope guard
        if "OUT_OF_SCOPE" in raw:
            return None

        # Extract SQL — strip markdown fences if the model disobeyed
        sql = self._extract_sql(raw)

        # Safety check — block any non-SELECT statement
        if not self._is_safe_sql(sql):
            logger.warning(f"Unsafe SQL detected and blocked: {sql[:120]}")
            raise ValueError("O modelo gerou uma query não permitida (não-SELECT). Tente reformular a pergunta.")

        return sql

    def generate_analysis(
        self,
        question: str,
        sql: str,
        query_result: str,
        row_count: int,
    ) -> str:
        """
        Ask the LLM to produce a full epidemiological analysis in pt-BR.

        Args:
            question:     Original user question.
            sql:          The SQL that was executed.
            query_result: Markdown table of Athena results (truncated if large).
            row_count:    Total number of rows returned.

        Returns:
            Rich narrative analysis in Brazilian Portuguese.
        """
        prompt = (
            f"The user asked (in Portuguese): \"{question}\"\n\n"
            f"The following SQL was executed against the Gold layer:\n```sql\n{sql}\n```\n\n"
            f"Query returned {row_count} row(s). Here are the results:\n\n"
            f"{query_result}\n\n"
            "Write a complete analysis in Brazilian Portuguese for a public health "
            "professional. Include:\n"
            "- A clear, direct answer to the user's question\n"
            "- Relevant epidemiological insights (Rt, alert level, incidence trends)\n"
            "- Risk Score assessment when applicable (Rt > 1.2 + high relative incidence)\n"
            "- Actionable recommendations or alerts for at-risk municipalities\n"
            "- If no data was returned, explain possible reasons and suggest rephrasing\n\n"
            "Use **bold** for key numbers and municipality names. Use bullet points "
            "and markdown tables when they improve readability. Be concise but thorough."
        )

        return self._invoke(prompt, temperature=0.3)

    # ── static helpers ───────────────────────────────────────────────

    @staticmethod
    def _extract_sql(text: str) -> str:
        """
        Strip markdown code fences and return raw SQL.

        Args:
            text: Raw LLM response.

        Returns:
            Clean SQL string.
        """
        # Remove ```sql ... ``` or ``` ... ``` fences
        cleaned = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
        # Remove trailing backticks
        cleaned = cleaned.replace("```", "").strip()
        return cleaned

    @staticmethod
    def _is_safe_sql(sql: str) -> bool:
        """
        Return True only if the SQL is a plain SELECT statement.

        Args:
            sql: SQL string to validate.

        Returns:
            True if safe (SELECT-only), False otherwise.
        """
        # Tokenize first word, ignoring comments and whitespace
        stripped = re.sub(r"--[^\n]*", "", sql)          # remove line comments
        stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)  # block comments
        tokens   = stripped.lower().split()

        if not tokens:
            return False

        # First meaningful token must be SELECT or WITH (CTEs are fine)
        first = tokens[0]
        if first not in ("select", "with"):
            return False

        # Additional guard: none of the tokens should be forbidden DML/DDL
        for token in tokens:
            # Strip trailing parentheses/semicolons from token
            clean_token = token.rstrip("(;")
            if clean_token in _FORBIDDEN_SQL_KEYWORDS:
                return False

        return True
