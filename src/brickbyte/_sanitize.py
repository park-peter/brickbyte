"""
Stream name sanitization and SQL identifier validation for brickbyte.
"""
import re


def sanitize_stream_name(name: str) -> str:
    """
    Sanitize a stream name for use as a table name.

    - Lowercase
    - Replace hyphens, dots, and whitespace with underscores
    - Strip characters that are invalid even in backtick-quoted identifiers
    - Prefix with underscore if starts with digit
    """
    result = name.lower()
    result = re.sub(r"[-.\s]+", "_", result)
    # Remove null bytes, backticks, semicolons
    result = re.sub(r"[\x00`;]+", "", result)
    # Strip leading/trailing underscores from the substitution
    result = result.strip("_") or "_"
    # Prefix with underscore if starts with digit
    if result[0].isdigit():
        result = f"_{result}"
    return result


def validate_identifier(name: str) -> str:
    """
    Validate that a name is safe for use inside backtick-quoted identifiers.

    Only rejects characters that are unsafe even inside backtick-quoted
    identifiers: null bytes, backticks, semicolons. Does NOT reject hyphens,
    dots, or unicode — Databricks allows these inside backtick-quoted identifiers.

    Returns the validated name.
    Raises ValueError if the name contains dangerous characters.
    """
    if not name:
        raise ValueError("Identifier cannot be empty")

    dangerous = re.search(r"[\x00`;]", name)
    if dangerous:
        raise ValueError(
            f"Identifier '{name}' contains unsafe character: "
            f"{repr(dangerous.group())}"
        )

    return name


def quoted_table_name(catalog: str, schema: str, table: str) -> str:
    """
    Build a fully-qualified, backtick-quoted table name.

    Validates all parts and returns `catalog`.`schema`.`table`.
    """
    validate_identifier(catalog)
    validate_identifier(schema)
    validate_identifier(table)
    return f"`{catalog}`.`{schema}`.`{table}`"
