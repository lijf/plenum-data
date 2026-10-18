"""
Shared output helpers for the topic-graph precomputes and make_bundled.py.

Plenum decodes these artifacts with Foundation's
``NSData.decompressed(using: .lzma)``, which reads the .xz container that
Python's ``lzma`` module writes by default (FORMAT_XZ, CRC64 check,
preset 6). Foundation cannot read gzip framing, so nothing here writes
.gz.

Every artifact is a pair: ``<name>.json.xz`` plus a ``<name>.meta.json``
sidecar holding ``snapshotDate``, ``count`` and ``sha256`` (hex, over the
uncompressed JSON bytes), and ``minWorks`` for the author index.
Stdlib only, so make_bundled.py runs without DuckDB.
"""

import hashlib
import json
import lzma
import os
import re
import urllib.request
from pathlib import Path

# Each parquet entity directory carries a manifest whose "date" field is
# the snapshot date, e.g. {"date": "2026-10-14", "entity": "authors", ...}.
MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/parquet/{entity}/manifest.json"

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

CHUNK_BYTES = 1 << 20


def snapshot_date(entity: str) -> str:
    """Snapshot date of the public parquet tree for ``entity``
    ("authors" or "works"). One small anonymous HTTPS GET."""
    url = MANIFEST_URL.format(entity=entity)
    with urllib.request.urlopen(url, timeout=60) as resp:
        return checked_date(json.load(resp)["date"])


def checked_date(date: str) -> str:
    if not DATE_PATTERN.match(date):
        raise SystemExit(f"snapshot date must be YYYY-MM-DD, got {date!r}")
    return date


def compress_file(src: Path, dest: Path) -> str:
    """Stream ``src`` into ``dest`` as .xz and return the SHA-256 hex
    digest of the uncompressed bytes. ``dest`` is replaced atomically."""
    digest = hashlib.sha256()
    tmp = dest.with_name(dest.name + ".tmp")
    with open(src, "rb") as fin, lzma.open(tmp, "wb") as fout:
        while chunk := fin.read(CHUNK_BYTES):
            digest.update(chunk)
            fout.write(chunk)
    os.replace(tmp, dest)
    return digest.hexdigest()


def compress_bytes(data: bytes, dest: Path) -> str:
    """``compress_file`` for an in-memory payload."""
    tmp = dest.with_name(dest.name + ".tmp")
    with lzma.open(tmp, "wb") as fout:
        fout.write(data)
    os.replace(tmp, dest)
    return hashlib.sha256(data).hexdigest()


def write_meta(path: Path, meta: dict) -> None:
    """Sidecar JSON: sorted keys, two-space indent, ASCII, trailing newline."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(meta, sort_keys=True, indent=2) + "\n", encoding="ascii")
    os.replace(tmp, path)


def read_meta(path: Path) -> dict:
    return json.loads(path.read_text(encoding="ascii"))


def verify(xz_path: Path, meta_path: Path):
    """Decompress ``xz_path``, check its SHA-256 and top-level element
    count against ``meta_path``, and return the decoded JSON value.
    Exits with a message on any mismatch."""
    meta = read_meta(meta_path)
    with lzma.open(xz_path, "rb") as f:
        data = f.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != meta["sha256"]:
        raise SystemExit(f"{xz_path.name}: sha256 {digest} does not match {meta_path.name}")
    value = json.loads(data)
    if len(value) != meta["count"]:
        raise SystemExit(f"{xz_path.name}: {len(value)} entries, {meta_path.name} says {meta['count']}")
    checked_date(meta["snapshotDate"])
    return value
