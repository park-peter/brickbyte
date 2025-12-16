"""
AI-powered semantic enrichment for BrickByte.
Uses Databricks Foundation Models to generate metadata.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ColumnEnrichment:
    """Enrichment results for a single column."""
    
    column_name: str
    description: Optional[str] = None
    is_pii: bool = False
    pii_type: Optional[str] = None  # e.g., "email", "phone", "ssn", "name"
    data_classification: Optional[str] = None  # e.g., "public", "internal", "confidential"
    
    def __str__(self) -> str:
        parts = [f"{self.column_name}:"]
        if self.description:
            parts.append(f'  "{self.description}"')
        if self.is_pii:
            parts.append(f"  ⚠️ PII detected: {self.pii_type}")
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


# Prompt template for Foundation Model
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
    
    Generates:
    - Column descriptions from data samples
    - PII detection
    - Data classification suggestions
    """
    
    def __init__(
        self,
        model_name: str = "databricks-meta-llama-3-1-70b-instruct",
        sample_rows: int = 50,
    ):
        """
        Initialize the enricher.
        
        Args:
            model_name: Foundation Model endpoint name
            sample_rows: Number of sample rows to analyze
        """
        self.model_name = model_name
        self.sample_rows = sample_rows
        self._spark = None
        self._client = None
    
    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark
    
    @property
    def client(self):
        """Get or create Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient()
        return self._client
    
    def _get_column_samples(self, table_name: str) -> Dict[str, List[str]]:
        """Get sample values for each column by parsing _airbyte_data JSON."""
        df = self.spark.sql(
            f"SELECT _airbyte_data FROM {table_name} LIMIT {self.sample_rows}"
        ).toPandas()
        
        samples = {}
        
        # Parse JSON from _airbyte_data to get actual columns
        for _, row in df.iterrows():
            try:
                data = json.loads(row["_airbyte_data"])
                for col, value in data.items():
                    if col not in samples:
                        samples[col] = []
                    if value is not None and len(samples[col]) < 5:
                        samples[col].append(str(value)[:100])
            except (json.JSONDecodeError, KeyError, TypeError):
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
            response = self.client.serving_endpoints.query(
                name=self.model_name,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning(f"Warning: Foundation Model call failed: {e}")
            return "{}"
    
    def _parse_enrichment_response(
        self,
        response: str,
        table_name: str,
    ) -> TableEnrichment:
        """Parse the Foundation Model response into structured enrichment."""
        enrichment = TableEnrichment(table_name=table_name)
        
        # Try to extract JSON from response
        try:
            # Find JSON in response (may have surrounding text)
            json_match = re.search(r'\{[\s\S]*\}', response)
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
        """
        Generate semantic enrichment for a table.
        
        Args:
            table_name: Fully qualified table name (catalog.schema.table)
        
        Returns:
            TableEnrichment with AI-generated metadata
        """
        logger.info(f"  Analyzing table: {table_name}")
        
        # Get column samples
        samples = self._get_column_samples(table_name)
        
        if not samples:
            logger.info("    No data columns found to analyze")
            return TableEnrichment(table_name=table_name)
        
        # Build prompt
        samples_str = self._format_samples_for_prompt(samples)
        prompt = ENRICHMENT_PROMPT.format(
            table_name=table_name,
            column_samples=samples_str,
        )
        
        # Call Foundation Model
        logger.info("    Calling Foundation Model...")
        response = self._call_foundation_model(prompt)
        
        # Parse response
        enrichment = self._parse_enrichment_response(response, table_name)
        
        logger.info(f"    ✓ Generated descriptions for {len(enrichment.columns)} columns")
        
        return enrichment
    
    def apply_to_catalog(self, enrichment: TableEnrichment):
        """
        Apply enrichment metadata to Unity Catalog.
        
        Since data is stored as JSON in _airbyte_data, we store field-level
        metadata as table tags and set the table description.
        """
        logger.info(f"  Applying metadata to {enrichment.table_name}")
        
        # Set table comment
        if enrichment.table_description:
            try:
                escaped_desc = enrichment.table_description.replace("'", "''")
                self.spark.sql(
                    f"COMMENT ON TABLE {enrichment.table_name} IS '{escaped_desc}'"
                )
                logger.info("    ✓ Set table description")
            except Exception as e:
                logger.warning(f"    Warning: Could not set table comment: {e}")
        
        # Store field metadata as table tags (fields are inside JSON)
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
                logger.info(f"    ✓ Tagged PII fields: {pii_fields}")
            except Exception as e:
                logger.warning(f"    Warning: Could not set PII tags: {e}")
        
        # Store column descriptions as table property for reference
        if enrichment.columns:
            try:
                # Build a summary of field descriptions
                desc_summary = "; ".join(
                    f"{c.column_name}: {c.description}"
                    for c in enrichment.columns[:10]  # Limit to avoid huge properties
                    if c.description
                )
                if desc_summary:
                    escaped = desc_summary.replace("'", "''")[:1000]
                    self.spark.sql(
                        f"ALTER TABLE {enrichment.table_name} "
                        f"SET TBLPROPERTIES ('brickbyte.field_descriptions' = '{escaped}')"
                    )
                    logger.info("    ✓ Stored field descriptions in table properties")
            except Exception as e:
                logger.warning(f"    Warning: Could not set field descriptions: {e}")
        
        logger.info("    ✓ Applied metadata to catalog")


def enrich_table(
    catalog: str,
    schema: str,
    table: str,
    apply_to_catalog: bool = True,
    model_name: str = "databricks-meta-llama-3-1-70b-instruct",
) -> TableEnrichment:
    """
    Convenience function to enrich a single table.
    
    Args:
        catalog: Unity Catalog name
        schema: Schema name
        table: Table name
        apply_to_catalog: Whether to apply metadata to Unity Catalog
        model_name: Foundation Model to use
    
    Returns:
        TableEnrichment with AI-generated metadata
    """
    table_name = f"{catalog}.{schema}.{table}"
    
    enricher = SemanticEnricher(model_name=model_name)
    enrichment = enricher.enrich(table_name)
    
    if apply_to_catalog:
        enricher.apply_to_catalog(enrichment)
    
    return enrichment

