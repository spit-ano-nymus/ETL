"""
column_aliases.py
-----------------
Programmer-defined column rename defaults.

Add entries here to pre-populate the "Rename columns" UI in Step 1.
Keys   = original column name as it appears in the source file.
Values = the new name to use in the destination table.

Example:
    COLUMN_ALIASES = {
        "bra_code":   "Branch ID",
        "cust_no":    "Customer Number",
        "txn_dt":     "Transaction Date",
    }

Leave COLUMN_ALIASES empty if you prefer to do all renaming through the UI.
"""

COLUMN_ALIASES: dict[str, str] = {
    # "original_name": "new_name",
}
