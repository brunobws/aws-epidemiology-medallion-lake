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
import yaml
from pathlib import Path
from typing import Optional
from utils.logger import get_logger
###################################

logger = get_logger(__name__)


####################################################################
# LOAD PROMPTS & CONFIGURATION FROM YAML
####################################################################
PROMPTS_DIR = Path(__file__).parent / "prompts"

try:
    with open(PROMPTS_DIR / "data_dictionary.yaml", "r", encoding="utf-8") as f:
        _dict_data = yaml.safe_load(f)
        _TABLE_SCHEMAS = "\n\n".join(_dict_data.get("tables", {}).values())

    with open(PROMPTS_DIR / "analista_prompt.yaml", "r", encoding="utf-8") as f:
        _prompt_config = yaml.safe_load(f)
except Exception as e:
    logger.error(f"Failed to load YAML configuration: {e}")
    raise RuntimeError(f"YAML config load failed: {e}")

# CONSTANTS
BEDROCK_REGION       = "sa-east-1"
BEDROCK_MODEL_ID     = _prompt_config.get("model", {}).get("id")
BEDROCK_MAX_TOKENS   = _prompt_config.get("model", {}).get("max_tokens", 4096)
BEDROCK_TEMPERATURE  = _prompt_config.get("model", {}).get("temperature", 0.0)

# Keywords that indicate an unsafe SQL statement (non-SELECT)
_FORBIDDEN_SQL_KEYWORDS = {
    "insert", "update", "delete", "drop", "create", "alter",
    "truncate", "replace", "merge", "exec", "execute", "grant",
    "revoke", "call", "begin", "commit", "rollback",
}

####################################################################
# SYSTEM PROMPT — schema + rules for the LLM
####################################################################
_SYSTEM_PROMPT = _prompt_config.get("system_prompt", "").format(table_schemas=_TABLE_SCHEMAS)


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
            "If the question asks for general knowledge, prevention, symptoms, "
            "or best practices about dengue, chikungunya, or zika and does NOT "
            "require querying the database, respond ONLY with the exact token: GENERAL_KNOWLEDGE\n\n"
            "Otherwise, respond ONLY with a valid Presto/Athena SQL SELECT "
            "statement — no markdown, no explanation, no code fences. "
            "Just the raw SQL ending with a semicolon."
        )

        raw = self._invoke(prompt, temperature=0.0)
        logger.info(f"SQL generation raw output (first 200 chars): {raw[:200]}")

        # Out-of-scope guard
        if "OUT_OF_SCOPE" in raw:
            return None

        # General knowledge guard
        if "GENERAL_KNOWLEDGE" in raw:
            return "GENERAL_KNOWLEDGE"

        # Extract SQL — strip markdown fences if the model disobeyed
        sql = self._extract_sql(raw)

        # Safety check — block any non-SELECT statement
        if not self._is_safe_sql(sql):
            logger.warning(f"Unsafe SQL detected and blocked: {sql[:120]}")
            raise ValueError("O modelo gerou uma query não permitida (não-SELECT). Tente reformular a pergunta.")

        return sql

    def fix_sql(self, question: str, wrong_sql: str, error_msg: str) -> str:
        """
        Ask the LLM to fix a SQL query that failed in Athena.

        Args:
            question: Original user question.
            wrong_sql: The SQL that failed.
            error_msg: The error message returned by Athena.

        Returns:
            The corrected SQL string.
        """
        prompt = (
            f"The user asked: \"{question}\"\n\n"
            f"You generated this SQL query:\n```sql\n{wrong_sql}\n```\n\n"
            f"But it failed to execute with this error:\n{error_msg}\n\n"
            "Look carefully at the table schemas provided. For example, if a COLUMN_NOT_FOUND error occurred, "
            "it means you used a column that does not exist in that table. "
            "Fix the SQL to resolve the error. Respond ONLY with a valid Presto/Athena SQL SELECT statement "
            "— no markdown, no explanation. Just the raw SQL ending with a semicolon."
        )

        raw = self._invoke(prompt, temperature=0.0)
        logger.info(f"SQL fix raw output (first 200 chars): {raw[:200]}")

        sql = self._extract_sql(raw)

        if not self._is_safe_sql(sql):
            logger.warning(f"Unsafe SQL detected during fix: {sql[:120]}")
            raise ValueError("O modelo tentou corrigir a query mas gerou uma instrução não permitida (não-SELECT).")

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
        if sql == "GENERAL_KNOWLEDGE":
            prompt = (
                f"The user asked (in Portuguese): \"{question}\"\n\n"
                "This is a general knowledge question about dengue, chikungunya, or zika. "
                "You are an expert epidemiological analyst. Write a complete, clear, and direct "
                "answer in Brazilian Portuguese for the user. Include best practices, symptoms, "
                "or prevention methods as appropriate.\n\n"
                "Use **bold** for key terms and bullet points to improve readability."
            )
        else:
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
