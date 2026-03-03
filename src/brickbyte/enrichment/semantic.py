"""
AI-powered semantic enrichment for brickbyte.
Uses Databricks Foundation Models to generate metadata.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("brickbyte")


@dataclass
class ColumnEnrichment:
    """Enrichment results for a single column."""

    column_name: str
    description: Optional[str] = None
    is_pii: bool = False
    pii_type: Optional[str] = None
    data_classification: Optional[str] = None

    def __str__(self) -> str:
        parts = [f"{self.column_name}:"]
        if self.description:
            parts.append(f'  "{self.description}"')
        if self.is_pii:
            parts.append(f"  PII detected: {self.pii_type}")
        if self.data_classification:
            parts.append(f"  Classification: {self.data_classification}")
        return "\n".join(parts)


@dataclass
class TableEnrichment:
    """Enrichment results for a table."""

    table_name: str
    columns: List[ColumnEnrichment] = field(default_factory=list)
    table_description: Optional[str] = None

    def __str__(self) -> str:
        lines = [f"Table: {self.table_name}"]
        if self.table_description:
            lines.append(f"Description: {self.table_description}")
        lines.append("")
        for col in self.columns:
            lines.append(str(col))
        return "\n".join(lines)


ENRICHMENT_PROMPT = """Analyze this database table and provide metadata enrichment.

Table: {table_name}
Columns and sample data:
{column_samples}

For each column, provide:
1. A brief description (1-2 sentences)
2. Whether it contains PII (personally identifiable information)
3. If PII, what type (email, phone, ssn, name, address, etc.)
4. Data classification (public, internal, confidential, restricted)

Also provide a brief description of the table's purpose.

Respond in JSON format:
{{
  "table_description": "Brief description of the table",
  "columns": [
    {{
      "name": "column_name",
      "description": "Description of the column",
      "is_pii": true/false,
      "pii_type": "type or null",
      "classification": "public/internal/confidential/restricted"
    }}
  ]
}}
"""


class SemanticEnricher:
    """
    AI-powered semantic enrichment using Databricks Foundation Models.
    Requires an active Spark session.
    """

    def __init__(
        self,
        model_name: str = "databricks-meta-llama-3-3-70b-instruct",
        sample_rows: int = 50,
    ):
        self.model_name = model_name
        self.sample_rows = sample_rows
        self._spark = None
        self._client = None

    @property
    def spark(self):
        """Get active Spark session, raising if unavailable."""
        if self._spark is None:
            from pyspark.sql import SparkSession

            session = SparkSession.getActiveSession()
            if session is None:
                raise RuntimeError(
                    "SemanticEnricher requires an active SparkSession. "
                    "Use SQLSemanticEnricher for SQL-based enrichment, "
                    "or ensure you are running in a Databricks notebook."
                )
            self._spark = session
        return self._spark

    @property
    def client(self):
        """Get or create Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

    def _get_column_samples(self, table_name: str) -> Dict[str, List[str]]:
        """Get sample values for each column."""
        schema = self.spark.table(table_name).schema
        col_names = [f.name for f in schema.fields]

        if "data" in col_names:
            data_col = "data"
        elif "_airbyte_data" in col_names:
            data_col = "_airbyte_data"
        else:
            # Flattened mode - sample all columns directly
            df = self.spark.sql(
                f"SELECT * FROM {table_name} LIMIT {self.sample_rows}"
            ).toPandas()
            samples = {}
            for col in df.columns:
                if not col.startswith("_"):
                    vals = df[col].dropna().astype(str).head(5).tolist()
                    samples[col] = [v[:100] for v in vals]
            return samples

        df = self.spark.sql(
            f"SELECT {data_col} FROM {table_name} LIMIT {self.sample_rows}"
        ).toPandas()

        samples = {}
        for _, row in df.iterrows():
            try:
                record = json.loads(row[data_col])
                for col, value in record.items():
                    if col not in samples:
                        samples[col] = []
                    if value is not None and len(samples[col]) < 5:
                        samples[col].append(str(value)[:100])
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.debug(f"Error parsing JSON row: {e}")
                continue

        return samples

    def _format_samples_for_prompt(self, samples: Dict[str, List[str]]) -> str:
        """Format column samples for the prompt."""
        lines = []
        for col, values in samples.items():
            values_str = ", ".join(f'"{v}"' for v in values[:3])
            lines.append(f"- {col}: {values_str}")
        return "\n".join(lines)

    def _call_foundation_model(self, prompt: str) -> str:
        """Call the Foundation Model API."""
        try:
            from databricks.sdk.service.serving import (
                ChatMessage,
                ChatMessageRole,
            )

            response = self.client.serving_endpoints.query(
                name=self.model_name,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning(f"Foundation Model call failed: {e}")
            return "{}"

    def _parse_enrichment_response(
        self,
        response: str,
        table_name: str,
    ) -> TableEnrichment:
        """Parse the Foundation Model response into structured enrichment."""
        enrichment = TableEnrichment(table_name=table_name)

        try:
            json_match = re.search(r"\{[\s\S]*\}", response)
            if json_match:
                data = json.loads(json_match.group())
            else:
                data = {}
        except json.JSONDecodeError:
            data = {}

        enrichment.table_description = data.get("table_description")

        for col_data in data.get("columns", []):
            col = ColumnEnrichment(
                column_name=col_data.get("name", ""),
                description=col_data.get("description"),
                is_pii=col_data.get("is_pii", False),
                pii_type=col_data.get("pii_type"),
                data_classification=col_data.get("classification"),
            )
            if col.column_name:
                enrichment.columns.append(col)

        return enrichment

    def enrich(self, table_name: str) -> TableEnrichment:
        """Generate semantic enrichment for a table."""
        logger.info(f"  Analyzing table: {table_name}")

        samples = self._get_column_samples(table_name)

        if not samples:
            logger.info("    No data columns found to analyze")
            return TableEnrichment(table_name=table_name)

        samples_str = self._format_samples_for_prompt(samples)
        prompt = ENRICHMENT_PROMPT.format(
            table_name=table_name,
            column_samples=samples_str,
        )

        logger.info("    Calling Foundation Model...")
        response = self._call_foundation_model(prompt)

        enrichment = self._parse_enrichment_response(response, table_name)

        logger.info(
            f"    Generated descriptions for {len(enrichment.columns)} columns"
        )

        return enrichment

    def apply_to_catalog(self, enrichment: TableEnrichment):
        """Apply enrichment metadata to Unity Catalog."""
        logger.info(f"  Applying metadata to {enrichment.table_name}")

        # Set table comment
        if enrichment.table_description:
            try:
                escaped_desc = enrichment.table_description.replace("'", "''")
                self.spark.sql(
                    f"COMMENT ON TABLE {enrichment.table_name} IS '{escaped_desc}'"
                )
                logger.info("    Set table description")
            except Exception as e:
                logger.warning(f"    Could not set table comment: {e}")

        # Set PII tags
        pii_fields = []
        for col in enrichment.columns:
            if col.is_pii:
                pii_fields.append(f"{col.column_name}:{col.pii_type or 'pii'}")

        if pii_fields:
            try:
                pii_value = ",".join(pii_fields)
                self.spark.sql(
                    f"ALTER TABLE {enrichment.table_name} "
                    f"SET TAGS ('pii_fields' = '{pii_value}')"
                )
                logger.info(f"    Tagged PII fields: {pii_fields}")
            except Exception as e:
                logger.warning(f"    Could not set PII tags: {e}")

        # In flatten mode, use COMMENT ON COLUMN for each column
        # In raw mode, use TBLPROPERTIES
        schema = self.spark.table(enrichment.table_name).schema
        col_names = {f.name for f in schema.fields}

        flatten_mode = "data" not in col_names and "_airbyte_data" not in col_names

        if flatten_mode and enrichment.columns:
            for col in enrichment.columns:
                if col.description and col.column_name in col_names:
                    try:
                        escaped = col.description.replace("'", "''")
                        self.spark.sql(
                            f"COMMENT ON COLUMN {enrichment.table_name}."
                            f"`{col.column_name}` IS '{escaped}'"
                        )
                    except Exception as e:
                        logger.warning(
                            f"    Could not set comment on {col.column_name}: {e}"
                        )
            logger.info("    Set column-level comments")
        elif enrichment.columns:
            try:
                desc_summary = "; ".join(
                    f"{c.column_name}: {c.description}"
                    for c in enrichment.columns[:10]
                    if c.description
                )
                if desc_summary:
                    escaped = desc_summary.replace("'", "''")[:1000]
                    self.spark.sql(
                        f"ALTER TABLE {enrichment.table_name} "
                        f"SET TBLPROPERTIES ('brickbyte.field_descriptions' = '{escaped}')"
                    )
                    logger.info("    Stored field descriptions in table properties")
            except Exception as e:
                logger.warning(f"    Could not set field descriptions: {e}")

        logger.info("    Applied metadata to catalog")


class SQLSemanticEnricher:
    """
    SQL-based semantic enrichment using SQL connector.
    Does not require Spark.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str,
        schema: str,
        model_name: str = "databricks-meta-llama-3-3-70b-instruct",
        sample_rows: int = 50,
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self._access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self.model_name = model_name
        self.sample_rows = sample_rows
        self._connection = None
        self._client = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            from databricks import sql

            self._connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self._access_token,
                catalog=self.catalog,
                schema=self.schema,
            )
        return self._connection

    def _execute(self, query: str):
        """Execute a SQL query and return results."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()

    @property
    def client(self):
        """Get Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

    def _get_column_samples(self, table_name: str) -> Dict[str, List[str]]:
        """Get sample values via SQL."""
        # Get columns
        columns = self._execute(f"DESCRIBE TABLE {table_name}")
        col_names = [row[0] for row in columns]

        if "data" in col_names:
            # Raw mode
            rows = self._execute(
                f"SELECT data FROM {table_name} LIMIT {self.sample_rows}"
            )
            samples = {}
            for row in rows:
                try:
                    record = json.loads(row[0])
                    for col, value in record.items():
                        if col not in samples:
                            samples[col] = []
                        if value is not None and len(samples[col]) < 5:
                            samples[col].append(str(value)[:100])
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    logger.debug(f"Error parsing JSON row: {e}")
                    continue
            return samples
        else:
            # Flatten mode
            sample_cols = [c for c in col_names if not c.startswith("_")][:20]
            if not sample_cols:
                return {}

            cols_str = ", ".join(f"`{c}`" for c in sample_cols)
            rows = self._execute(
                f"SELECT {cols_str} FROM {table_name} LIMIT {self.sample_rows}"
            )
            samples = {}
            for col_idx, col in enumerate(sample_cols):
                vals = []
                for row in rows:
                    if row[col_idx] is not None and len(vals) < 5:
                        vals.append(str(row[col_idx])[:100])
                if vals:
                    samples[col] = vals
            return samples

    def _call_foundation_model(self, prompt: str) -> str:
        """Call the Foundation Model API."""
        try:
            from databricks.sdk.service.serving import (
                ChatMessage,
                ChatMessageRole,
            )

            response = self.client.serving_endpoints.query(
                name=self.model_name,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning(f"Foundation Model call failed: {e}")
            return "{}"

    def enrich(self, table_name: str) -> TableEnrichment:
        """Generate semantic enrichment for a table."""
        logger.info(f"  Analyzing table: {table_name}")

        samples = self._get_column_samples(table_name)
        if not samples:
            return TableEnrichment(table_name=table_name)

        lines = []
        for col, values in samples.items():
            values_str = ", ".join(f'"{v}"' for v in values[:3])
            lines.append(f"- {col}: {values_str}")
        samples_str = "\n".join(lines)

        prompt = ENRICHMENT_PROMPT.format(
            table_name=table_name,
            column_samples=samples_str,
        )

        response = self._call_foundation_model(prompt)

        enrichment = TableEnrichment(table_name=table_name)
        try:
            json_match = re.search(r"\{[\s\S]*\}", response)
            if json_match:
                data = json.loads(json_match.group())
            else:
                data = {}
        except json.JSONDecodeError:
            data = {}

        enrichment.table_description = data.get("table_description")
        for col_data in data.get("columns", []):
            col = ColumnEnrichment(
                column_name=col_data.get("name", ""),
                description=col_data.get("description"),
                is_pii=col_data.get("is_pii", False),
                pii_type=col_data.get("pii_type"),
                data_classification=col_data.get("classification"),
            )
            if col.column_name:
                enrichment.columns.append(col)

        return enrichment

    def apply_to_catalog(self, enrichment: TableEnrichment):
        """Apply enrichment metadata via SQL."""
        logger.info(f"  Applying metadata to {enrichment.table_name}")

        if enrichment.table_description:
            try:
                escaped = enrichment.table_description.replace("'", "''")
                self._execute(
                    f"COMMENT ON TABLE {enrichment.table_name} IS '{escaped}'"
                )
            except Exception as e:
                logger.warning(f"    Could not set table comment: {e}")

        pii_fields = []
        for col in enrichment.columns:
            if col.is_pii:
                pii_fields.append(f"{col.column_name}:{col.pii_type or 'pii'}")

        if pii_fields:
            try:
                pii_value = ",".join(pii_fields)
                self._execute(
                    f"ALTER TABLE {enrichment.table_name} "
                    f"SET TAGS ('pii_fields' = '{pii_value}')"
                )
            except Exception as e:
                logger.warning(f"    Could not set PII tags: {e}")

        # Use COMMENT ON COLUMN in flatten mode
        columns = self._execute(f"DESCRIBE TABLE {enrichment.table_name}")
        col_names = {row[0] for row in columns}
        flatten_mode = "data" not in col_names

        if flatten_mode:
            for col in enrichment.columns:
                if col.description and col.column_name in col_names:
                    try:
                        escaped = col.description.replace("'", "''")
                        self._execute(
                            f"COMMENT ON COLUMN {enrichment.table_name}."
                            f"`{col.column_name}` IS '{escaped}'"
                        )
                    except Exception as e:
                        logger.warning(
                            f"    Could not set comment on {col.column_name}: {e}"
                        )

    def close(self):
        """Close the SQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


def enrich_table(
    catalog: str,
    schema: str,
    table: str,
    apply_to_catalog: bool = True,
    model_name: str = "databricks-meta-llama-3-3-70b-instruct",
) -> TableEnrichment:
    """Convenience function to enrich a single table using Spark."""
    table_name = f"{catalog}.{schema}.{table}"

    enricher = SemanticEnricher(model_name=model_name)
    enrichment = enricher.enrich(table_name)

    if apply_to_catalog:
        enricher.apply_to_catalog(enrichment)

    return enrichment
