"""Import EOIR CSV data into DuckDB.

Reads tab-delimited CSVs from the EOIR FOIA data release and loads them
into a local DuckDB database. Lookup tables are loaded in full; main tables
are loaded with DuckDB's efficient CSV scanner.

Usage:
    uv run python app/import_data.py [--data-dir PATH] [--db-path PATH]
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

# Default paths
DEFAULT_DATA_DIR = Path.home() / "Downloads" / "EOIR Case Data 2026-0301"
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "eoir.duckdb"


def import_all(data_dir: Path, db_path: Path) -> None:
    """Import all EOIR CSV files into DuckDB."""
    main_dir = data_dir / "EOIR Case Data"
    lookup_dir = data_dir / "Lookup"

    if not main_dir.exists():
        print(f"ERROR: Main data directory not found: {main_dir}")
        return
    if not lookup_dir.exists():
        print(f"ERROR: Lookup directory not found: {lookup_dir}")
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database: {db_path}")
    print(f"Data dir: {data_dir}")
    print()

    con = duckdb.connect(str(db_path))

    # Import lookup tables first (small, fast)
    print("=" * 60)
    print("IMPORTING LOOKUP TABLES")
    print("=" * 60)
    lookup_files = sorted(lookup_dir.glob("*.csv"))
    for csv_path in lookup_files:
        table_name = csv_path.stem.lower()
        import_csv(con, csv_path, table_name, is_lookup=True)

    # Import main tables
    print()
    print("=" * 60)
    print("IMPORTING MAIN TABLES")
    print("=" * 60)
    main_files = sorted(main_dir.glob("*.csv"))
    for csv_path in main_files:
        table_name = csv_path.stem.lower()
        import_csv(con, csv_path, table_name, is_lookup=False)

    # Create useful indexes
    print()
    print("=" * 60)
    print("CREATING INDEXES")
    print("=" * 60)
    create_indexes(con)

    # Summary
    print()
    print("=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    print(f"Total tables: {len(tables)}")
    for (t,) in tables:
        count = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t}: {count:,} rows")

    con.close()
    print(f"\nDatabase size: {db_path.stat().st_size / (1024**3):.2f} GB")


def import_csv(con: duckdb.DuckDBPyConnection, csv_path: Path, table_name: str, is_lookup: bool) -> None:
    """Import a single CSV into DuckDB."""
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    label = f"  {table_name} ({size_mb:.0f} MB)" if size_mb > 1 else f"  {table_name}"
    print(f"{label} ...", end=" ", flush=True)

    start = time.time()

    try:
        # Drop existing table
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        # DuckDB's read_csv auto-detects delimiters and types
        con.execute(f"""
            CREATE TABLE "{table_name}" AS
            SELECT * FROM read_csv(
                '{csv_path}',
                delim = '\t',
                header = true,
                auto_detect = true,
                sample_size = 10000,
                ignore_errors = true,
                null_padding = true
            )
        """)

        count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        elapsed = time.time() - start
        print(f"{count:,} rows ({elapsed:.1f}s)")

    except Exception as e:
        elapsed = time.time() - start
        print(f"FAILED ({elapsed:.1f}s): {e}")


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    """Create indexes on frequently-queried columns."""
    indexes = [
        ("a_tblcase", "IDNCASE"),
        ("b_tblproceeding", "IDNCASE"),
        ("b_tblproceeding", "IDNPROCEEDING"),
        ("tbl_schedule", "IDNCASE"),
        ("tbl_schedule", "IDNPROCEEDING"),
        ("tbl_court_motions", "IDNCASE"),
        ("tbl_court_appln", "IDNCASE"),
        ("b_tblproceedcharges", "IDNCASE"),
        ("tbl_repsassigned", "IDNCASE"),
    ]

    for table, column in indexes:
        try:
            idx_name = f"idx_{table}_{column}".lower()
            con.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{column}")')
            print(f"  Index: {idx_name}")
        except Exception as e:
            print(f"  Index {table}.{column}: skipped ({e})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import EOIR CSV data into DuckDB")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    args = parser.parse_args()

    import_all(args.data_dir, args.db_path)
