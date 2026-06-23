#!/usr/bin/env python3
"""
verify_warehouse.py – Quick health-check for the DuckDB RetailDW.

Usage:
    python verify_warehouse.py
    python verify_warehouse.py --db /path/to/RetailDW.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


# ── helpers ───────────────────────────────────────────────────────────────────

def header(title: str) -> None:
    width = 60
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'═' * width}")


def run(con: duckdb.DuckDBPyConnection, sql: str) -> list:
    return con.execute(sql).fetchall()


def table_exists(con, name: str) -> bool:
    r = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()
    return bool(r and r[0])


# ── sections ──────────────────────────────────────────────────────────────────

def section_row_counts(con) -> None:
    header("TABLE ROW COUNTS")
    tables = [
        "dim_customer", "dim_product", "dim_store",
        "dim_date", "dim_channel", "fact_sales", "dead_letter_events",
    ]
    fmt = "{:<25} {:>12}"
    print(fmt.format("Table", "Row count"))
    print("-" * 38)
    total_facts = 0
    for t in tables:
        if table_exists(con, t):
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(fmt.format(t, f"{n:,}"))
            if t == "fact_sales":
                total_facts = n
        else:
            print(fmt.format(t, "TABLE MISSING ⚠"))
    if total_facts == 0:
        print("\n  ⚠  fact_sales is empty – make sure the pipeline has run.")


def section_total_revenue(con) -> None:
    header("QUERY 1 – TOTAL REVENUE (purchases net of returns)")
    if not table_exists(con, "fact_sales"):
        print("  fact_sales not found."); return
    row = con.execute("""
        SELECT round(sum(revenue), 2) AS total_revenue
        FROM fact_sales
        WHERE event_type IN ('purchase', 'return')
    """).fetchone()
    print(f"  Total revenue : ${row[0]:,.2f}" if row and row[0] is not None else "  No data yet.")


def section_return_rate(con) -> None:
    header("QUERY 2 – RETURN RATE")
    if not table_exists(con, "fact_sales"):
        print("  fact_sales not found."); return
    row = con.execute("""
        SELECT
            count(CASE WHEN event_type='return'   THEN 1 END) AS returns,
            count(CASE WHEN event_type='purchase' THEN 1 END) AS purchases,
            round(
                100.0 * count(CASE WHEN event_type='return' THEN 1 END)
                / nullif(count(CASE WHEN event_type='purchase' THEN 1 END), 0),
                2
            ) AS return_rate_pct
        FROM fact_sales
    """).fetchone()
    if row:
        print(f"  Purchases : {row[1]:,}")
        print(f"  Returns   : {row[0]:,}")
        print(f"  Rate      : {row[2]}%" if row[2] is not None else "  Rate      : N/A")


def section_top_categories(con) -> None:
    header("QUERY 3 – REVENUE BY PRODUCT CATEGORY")
    if not (table_exists(con, "fact_sales") and table_exists(con, "dim_product")):
        print("  Required tables missing."); return
    rows = con.execute("""
        SELECT p.product_category,
               round(sum(f.revenue), 2) AS revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.event_type IN ('purchase','return')
        GROUP BY p.product_category
        ORDER BY revenue DESC
    """).fetchall()
    if not rows:
        print("  No data yet."); return
    fmt = "  {:<20} {:>14}"
    print(fmt.format("Category", "Revenue ($)"))
    print("  " + "-" * 35)
    for cat, rev in rows:
        print(fmt.format(cat or "Unknown", f"{rev:,.2f}"))


def section_dead_letters(con) -> None:
    header("DEAD-LETTER REJECTION SUMMARY")
    if not table_exists(con, "dead_letter_events"):
        print("  dead_letter_events not found."); return
    rows = con.execute("""
        SELECT reject_reason, count(*) AS cnt
        FROM dead_letter_events
        GROUP BY reject_reason
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    if not rows:
        print("  No dead-letter records – great!")
        return
    fmt = "  {:<50} {:>8}"
    print(fmt.format("Reject reason", "Count"))
    print("  " + "-" * 59)
    for reason, cnt in rows:
        print(fmt.format((reason or "")[:50], f"{cnt:,}"))


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Verify RetailDW DuckDB warehouse.")
    parser.add_argument(
        "--db",
        default=str(Path(__file__).resolve().parent / "warehouse" / "RetailDW.duckdb"),
        help="Path to the .duckdb file",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"\n  ✗  Database not found: {db_path}")
        print("     Run:  duckdb warehouse/RetailDW.duckdb < warehouse/create_tables.sql")
        sys.exit(1)

    print(f"\n  Database : {db_path}")
    print(f"  Size     : {db_path.stat().st_size / 1024:.1f} KB")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        section_row_counts(con)
        section_total_revenue(con)
        section_return_rate(con)
        section_top_categories(con)
        section_dead_letters(con)
    finally:
        con.close()

    print("\n  ✓  Verification complete.\n")


if __name__ == "__main__":
    main()
