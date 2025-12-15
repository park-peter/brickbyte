"""
Preview engine for BrickByte.
Provides diff calculation and schema comparison before syncing.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SchemaChange:
    """Represents a schema change between source and target."""
    
    column: str
    change_type: str  # "added", "removed", "type_changed"
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    
    def __str__(self) -> str:
        if self.change_type == "added":
            return f"  + {self.column} ({self.source_type}) - NEW"
        elif self.change_type == "removed":
            return f"  - {self.column} ({self.target_type}) - REMOVED"
        else:
            return f"  ~ {self.column}: {self.target_type} -> {self.source_type}"


@dataclass
class StreamPreview:
    """Preview information for a single stream."""
    
    stream_name: str
    source_count: int
    target_count: int
    new_records: int = 0
    modified_records: int = 0
    deleted_records: int = 0
    schema_changes: List[SchemaChange] = field(default_factory=list)
    sample_records: List[dict] = field(default_factory=list)
    
    def __str__(self) -> str:
        parts = []
        
        # Record counts
        if self.new_records > 0:
            parts.append(f"+{self.new_records} new")
        if self.modified_records > 0:
            parts.append(f"~{self.modified_records} modified")
        if self.deleted_records > 0:
            parts.append(f"-{self.deleted_records} deleted")
        
        if not parts:
            parts.append(f"{self.source_count} records")
        
        line = f"{self.stream_name}: {' | '.join(parts)}"
        
        # Schema changes
        if self.schema_changes:
            line += "\n  Schema changes:"
            for change in self.schema_changes:
                line += f"\n  {change}"
        
        return line


@dataclass
class PreviewResult:
    """Complete preview result for all streams."""
    
    streams: List[StreamPreview] = field(default_factory=list)
    total_source_records: int = 0
    total_new_records: int = 0
    total_modified_records: int = 0
    total_deleted_records: int = 0
    has_schema_changes: bool = False
    
    def __str__(self) -> str:
        lines = ["=" * 60, "Sync Preview", "=" * 60, ""]
        
        for stream in self.streams:
            lines.append(str(stream))
        
        lines.append("")
        lines.append("-" * 60)
        lines.append(
            f"Total: {self.total_source_records} records "
            f"(+{self.total_new_records} new, "
            f"~{self.total_modified_records} modified, "
            f"-{self.total_deleted_records} deleted)"
        )
        
        if self.has_schema_changes:
            lines.append("⚠️  Schema changes detected")
        
        lines.append("=" * 60)
        
        return "\n".join(lines)


class PreviewEngine:
    """
    Generates previews of sync operations.
    
    Compares source data (sampled) with existing target tables to show:
    - Target record counts
    - Schema changes (inferred from samples)
    - Sample records
    """
    
    def __init__(self, catalog: str, schema: str):
        """
        Initialize the preview engine.
        
        Args:
            catalog: Unity Catalog name
            schema: Target schema name
        """
        self.catalog = catalog
        self.schema = schema
        self._spark = None
    
    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession
                self._spark = SparkSession.builder.getOrCreate()
            except ImportError:
                return None
        return self._spark
    
    def get_table_name(self, stream_name: str) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{stream_name}"
    
    def table_exists(self, stream_name: str) -> bool:
        """Check if target table exists."""
        if not self.spark:
            return False
        
        table_name = self.get_table_name(stream_name)
        return self.spark.catalog.tableExists(table_name)
    
    def get_target_count(self, stream_name: str) -> int:
        """Get record count from target table."""
        if not self.table_exists(stream_name):
            return 0
        
        table_name = self.get_table_name(stream_name)
        return self.spark.table(table_name).count()
    
    def get_target_schema(self, stream_name: str) -> Dict[str, str]:
        """Get schema of target table."""
        if not self.table_exists(stream_name):
            return {}
        
        table_name = self.get_table_name(stream_name)
        df = self.spark.table(table_name)
        return {f.name: str(f.dataType) for f in df.schema.fields}
    
    def get_source_schema(self, sample_records: List[dict]) -> Dict[str, str]:
        """Infer schema from sample records."""
        if not sample_records:
            return {}
        
        # Simple inference from first record
        # In a real scenario, we might want to check Airbyte catalog
        record = sample_records[0]
        return {k: type(v).__name__ for k, v in record.items()}
    
    def compare_schemas(
        self,
        source_schema: Dict[str, str],
        target_schema: Dict[str, str],
    ) -> List[SchemaChange]:
        """Compare source and target schemas."""
        changes = []
        
        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())
        
        # New columns
        for col in source_cols - target_cols:
            changes.append(SchemaChange(
                column=col,
                change_type="added",
                source_type=source_schema[col],
            ))
        
        # Removed columns
        for col in target_cols - source_cols:
            changes.append(SchemaChange(
                column=col,
                change_type="removed",
                target_type=target_schema[col],
            ))
        
        # Type changes - simplified
        # Note: inferred source types (python types) vs target types (spark types)
        # mismatch is expected, so we largely skip strict type comparison here
        # unless we map them properly. For now, we omit type_changed to avoid noise.
        
        return changes
    
    def preview_stream(
        self,
        ab_source: Any,
        stream_name: str,
        primary_key: Optional[List[str]] = None,
        sample_size: int = 5,
    ) -> StreamPreview:
        """Generate preview for a single stream."""
        target_count = self.get_target_count(stream_name)
        
        # Get samples from stream
        sample_records = []
        try:
             # We only peek at the first N records
            records_gen = ab_source.get_records(stream_name)
            for i, record in enumerate(records_gen):
                if i >= sample_size:
                    break
                sample_records.append(record)
        except Exception:
            pass
            
        # Compare schemas
        source_schema = self.get_source_schema(sample_records)
        target_schema = self.get_target_schema(stream_name)
        schema_changes = self.compare_schemas(source_schema, target_schema)
        
        return StreamPreview(
            stream_name=stream_name,
            source_count=-1, # Unknown in streaming
            target_count=target_count,
            new_records=-1, # Unknown
            modified_records=-1, # Unknown
            deleted_records=-1, # Unknown
            schema_changes=schema_changes,
            sample_records=sample_records,
        )
    
    def preview(
        self,
        ab_source: Any,
        streams: List[str],
        primary_key: Optional[Dict[str, List[str]]] = None,
        sample_size: int = 5,
    ) -> PreviewResult:
        """
        Generate preview for all streams.
        
        Args:
            ab_source: Initialized Airbyte source
            streams: List of stream names
            primary_key: Optional dict mapping stream names to primary key columns
            sample_size: Number of sample records per stream
        
        Returns:
            PreviewResult with all stream previews
        """
        result = PreviewResult()
        
        for stream_name in streams:
            keys = primary_key.get(stream_name) if primary_key else None
            stream_preview = self.preview_stream(
                ab_source, stream_name, keys, sample_size
            )
            result.streams.append(stream_preview)
            
            # Totals are less relevant with unknown counts, but we act best effort
            if stream_preview.schema_changes:
                result.has_schema_changes = True
        
        return result

