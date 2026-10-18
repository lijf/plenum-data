# plenum-data

CC0 derivatives of the public OpenAlex snapshot, published as GitHub releases for [Plenum](https://github.com/lijf/plenum), a macOS research-literature explorer. Nothing here is hand-written data: every release is produced by `tools/precompute_authors.py` from the parquet tree at `s3://openalex/data/parquet/authors/`, which OpenAlex publishes under CC0.

## Releases

One release per OpenAlex snapshot, tagged `snapshot-YYYY-MM-DD` with the snapshot's manifest date. Each carries two assets with fixed names:

| Asset | Contents |
|---|---|
| `authors.json.xz` | A bare JSON array of every author with at least 50 works: `{"displayName", "hIndex", "id", "worksCount"}` per record, sorted by `id`, xz-compressed (Python `lzma` defaults: FORMAT_XZ, CRC64, preset 6) |
| `authors.meta.json` | `{"count": Int, "minWorks": 50, "sha256": "<64 hex>", "snapshotDate": "YYYY-MM-DD"}`; `sha256` is over the uncompressed JSON |

Plenum fetches `https://github.com/lijf/plenum-data/releases/latest/download/authors.json.xz` and the sidecar beside it, decompresses with Foundation's LZMA decoder, verifies the hash, and installs the file as its full author index. The smaller tier it ships with (authors with at least 500 works) and the topic co-occurrence graph are bundled inside the app, not released here.

Sizes, snapshot of 2026-05-09: 2,819,960 authors, 227 MB uncompressed, 34 MB as xz. Today's snapshot has about 3.06 million authors with at least 50 works.

## How releases are made

`.github/workflows/release.yml` runs on the 20th of every month and on manual dispatch. It reads the manifest date, exits when the latest release already carries that snapshot, otherwise runs the precompute on a GitHub-hosted runner (DuckDB reads only the projected columns from S3 by range requests; no AWS account, no OpenAlex API credit) and publishes the two assets as the new latest release. OpenAlex publishes public snapshots quarterly, around the second Wednesday of January, April, July and October, so most monthly runs exit early at no cost.

To run it by hand: `gh workflow run release.yml --repo lijf/plenum-data`, then `gh run watch --repo lijf/plenum-data`.

To run the precompute locally: `pip install "duckdb==1.5.2"` (or `uv sync` in `tools/`), then `python3 tools/precompute_authors.py --output-dir out`. It writes `authors.json`, `authors.json.xz` and `authors.meta.json` and verifies the xz against the sidecar before exiting.

## License

Data: CC0 1.0. Scripts under `tools/`: MIT. See `LICENSE`.
