"""Create and evolve Unity Catalog Delta tables from ETL table contracts."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

AUDIT_COLUMNS = [
    {"name": "run_id", "type": "STRING", "nullable": False},
    {"name": "environment", "type": "STRING", "nullable": False},
    {"name": "latest_update_datetime", "type": "TIMESTAMP", "nullable": False},
    {"name": "latest_insert_datetime", "type": "TIMESTAMP", "nullable": False},
]
RAW_AUDIT_COLUMNS = [
    {"name": "arrival_date", "type": "DATE", "nullable": False},
    {"name": "source_file_name", "type": "STRING", "nullable": False},
    {"name": "source_file_path", "type": "STRING", "nullable": False},
    {"name": "source_file_checksum", "type": "STRING", "nullable": False},
    {"name": "source_row_number", "type": "BIGINT", "nullable": False},
] + AUDIT_COLUMNS


def _quote(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _qualified(catalog: str, schema: str, table: str) -> str:
    return ".".join(_quote(part) for part in (catalog, schema, table))


def _spark_type(data_type: str) -> str:
    normalized = data_type.upper()
    if normalized.startswith(("VARCHAR", "CHAR", "TEXT", "STRING")):
        return "STRING"
    if normalized.startswith(("INT", "BIGINT", "SMALLINT", "TINYINT")):
        return "BIGINT"
    if normalized.startswith(("REAL", "FLOAT", "DOUBLE")):
        return "DOUBLE"
    if normalized.startswith("DECIMAL"):
        return normalized if "(" in normalized else "DECIMAL(38,18)"
    if normalized.startswith(("BOOL", "BOOLEAN")):
        return "BOOLEAN"
    if normalized.startswith("DATE"):
        return "DATE"
    if normalized.startswith(("TIMESTAMP", "DATETIME")):
        return "TIMESTAMP"
    return "STRING"


def _load_contracts(metadata_root: Path) -> list[dict[str, Any]]:
    contracts = []
    for path in sorted((metadata_root / "tables").glob("**/*.y*ml")):
        with path.open(encoding="utf-8") as stream:
            contract = yaml.safe_load(stream) or {}
        if contract.get("table_name"):
            contracts.append(contract)
    return contracts


def _configured_columns(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": column["name"],
            "type": _spark_type(column.get("type", "STRING")),
            "nullable": column.get("nullable", True),
        }
        for column in contract.get("columns", [])
    ]


def _columns_for_contract(contract: dict[str, Any], contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns = _configured_columns(contract)
    layer = contract.get("database_layer", "").lower()
    if not columns and layer == "raw":
        for persistent in contracts:
            if persistent.get("database_layer", "").lower() != "persistent":
                continue
            if persistent.get("source_table") == contract["table_name"]:
                columns = _configured_columns(persistent)
                break
    if not columns and layer == "consumption":
        source_table = contract.get("source_table")
        source = next((item for item in contracts if item.get("table_name") == source_table), None)
        if source:
            columns = _configured_columns(source)
    if not columns:
        raise ValueError(
            f"{layer}.{contract['table_name']} needs columns or a Persistent source table with columns"
        )
    audit_columns = RAW_AUDIT_COLUMNS if layer == "raw" else AUDIT_COLUMNS
    existing = {column["name"] for column in columns}
    return columns + [column for column in audit_columns if column["name"] not in existing]


def _column_sql(column: dict[str, Any]) -> str:
    nullable = "" if column["nullable"] else " NOT NULL"
    return f"{_quote(column['name'])} {column['type']}{nullable}"


def _sync_table(spark: Any, catalog: str, contract: dict[str, Any], contracts: list[dict[str, Any]]) -> None:
    schema = contract["database_layer"].lower()
    table = contract["table_name"]
    qualified_name = _qualified(catalog, schema, table)
    columns = _columns_for_contract(contract, contracts)
    if not spark.catalog.tableExists(f"{catalog}.{schema}.{table}"):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified_name} ({', '.join(_column_sql(column) for column in columns)}) USING DELTA")
        return
    existing = {field.name.lower() for field in spark.table(f"{catalog}.{schema}.{table}").schema.fields}
    additions = [column for column in columns if column["name"].lower() not in existing]
    if additions:
        spark.sql(f"ALTER TABLE {qualified_name} ADD COLUMNS ({', '.join(_column_sql(column) for column in additions)})")


def sync_catalog(metadata_root: str | Path, catalog: str) -> None:
    """Synchronize every table contract to a Unity Catalog Delta table."""
    from pyspark.sql import SparkSession

    contracts = _load_contracts(Path(metadata_root))
    if not contracts:
        raise ValueError(f"No table contracts found under {metadata_root}")
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {_quote(catalog)}")
    for schema in ("raw", "persistent", "consumption"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote(catalog)}.{_quote(schema)}")
    for contract in contracts:
        _sync_table(spark, catalog, contract, contracts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize Unity Catalog tables from ETL YAML metadata")
    parser.add_argument("--metadata-root", required=True)
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    sync_catalog(args.metadata_root, args.catalog)


if __name__ == "__main__":
    main()