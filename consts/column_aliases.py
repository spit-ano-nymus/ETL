"""
column_aliases.py
-----------------
Per-table column rename defaults, keyed by source table/file name.

Add a "_default" block for aliases that should apply to every source; add
a block per specific source (Trino table name, or the file name without
extension) for aliases that only apply to that one source. A per-table
entry overrides a "_default" entry for the same column name.

Example:
    COLUMN_ALIASES = {
        "_default": {
            "cust_no": "customer_id",   # applies everywhere unless overridden below
        },
        "trans": {                       # Trino table "trans"
            "ops_id": "operationid",
            "txn_dt": "transaction_date",
        },
        "sales_weekly": {                # file "sales_weekly.csv" (extension stripped)
            "amt": "amount",
        },
    }

Leave a table's block empty (or omit it) if you prefer to do that table's
renaming through the UI each time.
"""

COLUMN_ALIASES: dict[str, dict[str, str]] = {
    "_default": {
        # "original_name": "new_name",
    },
    # "trans": {
    #     "ops_id": "operationid",
    # },
}


def aliases_for(table_key: str) -> dict[str, str]:
    """
    Merge "_default" aliases with the table-specific block for `table_key`.
    Table-specific entries win over "_default" for the same column.
    """
    merged = dict(COLUMN_ALIASES.get("_default", {}))
    merged.update(COLUMN_ALIASES.get(table_key, {}))
    return merged
