"""
BrickByte Enrichment Module.

Provides AI-powered metadata enrichment for tables:
- Column descriptions via Foundation Models
- PII detection
- Data classification
"""
from brickbyte.enrichment.semantic import SemanticEnricher, enrich_table

__all__ = ["SemanticEnricher", "enrich_table"]

