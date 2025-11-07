from __future__ import annotations
from typing import List, Optional

def extract_table_like(last_page) -> Optional[List[List[str]]]:
    """
    Try increasingly permissive extraction strategies and return a table
    with >= 2 rows. Prefer the largest table when multiple candidates exist.
    """
    strategies = [
        dict(
            vertical_strategy="lines",
            horizontal_strategy="lines",
            intersection_tolerance=5,
            snap_tolerance=3,
            join_tolerance=3,
            edge_min_length=3,
            min_words_vertical=1,
            min_words_horizontal=1,
        ),
        {},  # pdfplumber defaults
    ]

    # Try .extract_table(...) then .extract_tables(...) for each strategy
    for st in strategies:
        tbl = last_page.extract_table(table_settings=st) if st else last_page.extract_table()
        if tbl and len(tbl) >= 2:
            return tbl

        tables = last_page.extract_tables(table_settings=st) if st else last_page.extract_tables() or []
        tables = [t for t in (tables or []) if t and len(t) >= 2]
        if tables:
            tables.sort(key=lambda t: len(t), reverse=True)
            return tables[0]

    return None
