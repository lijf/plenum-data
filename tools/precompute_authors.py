"""
Precompute the author index for Plenum's `aut` mode.

Reads the OpenAlex public S3 snapshot's parquet tree (no auth, no rate
limits, CC0), keeps authors with ``works_count >= MIN_WORKS``, and writes
the compact form ``Cache.loadAuthors()`` decodes: a bare JSON array of
{"displayName", "hIndex", "id", "worksCount"}.

Outputs in --output-dir:
    authors.json        the array (~250 MB at >=50)
    authors.json.xz     the same bytes as .xz (~37 MB), the release asset
    authors.meta.json   {"count", "minWorks", "sha256", "snapshotDate"}

Run once per public snapshot (about quarterly); README.md has the whole
chore, including the release upload and the bundled-tier refresh
(make_bundled.py). Thresholds: Decision-Journal 2026-05-08 and
2026-10-18 (D1): >=50 is the full index, >=500 the bundled tier.

Usage:
    .venv/bin/python precompute_authors.py
    .venv/bin/python precompute_authors.py --min-works 25 --output-dir /tmp/a25
"""

import argparse
import json
from pathlib import Path

import duckdb

from artifact_io import checked_date, compress_file, snapshot_date, verify, write_meta

# OpenAlex public S3 bucket, parquet tree. No auth, no rate limit. CC0.
# Layout observed 2026-10-18: data/parquet/authors/manifest.json plus
# updated_date=YYYY-MM-DD/part_NNNN.parquet.
S3_AUTHORS_GLOB = "s3://openalex/data/parquet/authors/updated_date=*/*.parquet"

# Default works-count threshold. See Decision-Journal 2026-05-08.
DEFAULT_MIN_WORKS = 50


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "output",
    )
    parser.add_argument(
        "--min-works",
        type=int,
        default=DEFAULT_MIN_WORKS,
        help=f"Authors with works_count below this are dropped. Default: {DEFAULT_MIN_WORKS}.",
    )
    parser.add_argument(
        "--snapshot-date",
        help="YYYY-MM-DD for authors.meta.json. Default: the parquet manifest's date.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "authors.json"
    date = checked_date(args.snapshot_date) if args.snapshot_date else snapshot_date("authors")
    print(f"Snapshot {date}")

    con = duckdb.connect()
    # httpfs reads parquet column chunks from S3 with range requests. The
    # region must be set even for public buckets — DuckDB defaults wrong
    # otherwise and fails with a confusing error.
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_region='us-east-1';")
    # Default http_timeout (30s) and http_retries (3) are too tight for
    # OpenAlex's larger part files on slower / variable connections —
    # one timed-out 100MB part fails the whole query. Generous values
    # here cost nothing on fast links and rescue the slow / flaky case.
    con.execute("SET http_timeout=300000;")  # 5 min per request
    con.execute("SET http_retries=20;")
    con.execute("SET http_keep_alive=true;")
    # Show DuckDB's TUI progress bar in the terminal — the streaming +
    # window-function aggregation is the long-running step and there's
    # nothing on disk to watch until the JSON write phase. progress_bar_time
    # is the threshold in ms before the bar appears (0 = always).
    con.execute("SET enable_progress_bar=true;")
    con.execute("SET progress_bar_time=0;")

    print(f"Streaming authors from {S3_AUTHORS_GLOB}")
    print(f"  filter: works_count >= {args.min_works}")
    print("  (several minutes, network-bound)")

    # The works_count filter runs before the window, so the window sees
    # about 3M rows. A record updated more than once could appear in
    # several updated_date partitions; QUALIFY ROW_NUMBER() ... = 1 keeps
    # the latest. Use QUALIFY, not a CTE with rn: DuckDB 1.5.2 fails over
    # parquet on the CTE shape with an internal bind error on
    # summary_stats. hive_partitioning=false keeps the file's own
    # updated_date column instead of a DATE parsed from the path.
    con.execute(f"""
        CREATE TEMP TABLE authors AS
        SELECT
            REGEXP_REPLACE(id, '^https?://openalex\\.org/', '') AS id,
            display_name,
            works_count,
            summary_stats.h_index AS h_index
        FROM read_parquet('{S3_AUTHORS_GLOB}', hive_partitioning=false)
        WHERE works_count >= {args.min_works}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
        ORDER BY id;
    """)

    n_authors = con.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
    print(f"  filtered to {n_authors:,} authors")

    print(f"Writing JSON array → {output_path}")
    write_json_array(con, output_path)
    size_mb = output_path.stat().st_size / 1e6
    print(f"  {output_path} ({size_mb:.1f} MB)")

    xz_path = args.output_dir / "authors.json.xz"
    meta_path = args.output_dir / "authors.meta.json"
    sha = compress_file(output_path, xz_path)
    write_meta(
        meta_path,
        {"count": n_authors, "minWorks": args.min_works, "sha256": sha, "snapshotDate": date},
    )
    verify(xz_path, meta_path)
    print(f"  {xz_path} ({xz_path.stat().st_size / 1e6:.1f} MB), {meta_path.name}")


def write_json_array(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Stream authors → single JSON array file. Matches Plenum's Cache
    encoder output (camelCase keys via direct emission, sorted keys for
    diff stability, no whitespace). Streamed in 10K-row batches so
    memory stays bounded for ~3M authors — a fetchall + json.dump pass
    would briefly materialize ~2 GB of Python dicts.
    """
    con.execute("SELECT id, display_name, works_count, h_index FROM authors ORDER BY id")

    with open(path, "w", encoding="utf-8") as f:
        f.write("[")
        first = True
        while True:
            rows = con.fetchmany(10_000)
            if not rows:
                break
            for row in rows:
                if not first:
                    f.write(",")
                first = False
                obj = {
                    "id": row[0],
                    "displayName": row[1],
                    "worksCount": row[2],
                    "hIndex": row[3],
                }
                json.dump(
                    obj,
                    f,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
        f.write("]")


if __name__ == "__main__":
    main()
