"""Turso cloud backup/restore for PokeRo.

Design: the bot keeps using local SQLite (all existing sqlite3 call sites
untouched). This module periodically ships a gzip'd copy of the game DBs to
Turso over HTTPS and pulls them back on first boot of a fresh machine — so
you can start the bot from Termux, NexCloud, or anywhere without data loss.

Env (set in .env or the NexCloud/Pterodactyl panel):
  TURSO_DATABASE_URL   e.g. libsql://pokero-....turso.io  (or https://...)
  TURSO_AUTH_TOKEN     database auth token from `turso db tokens create`
  TURSO_BACKUP_INTERVAL  seconds between background pushes (default 300)
"""

from __future__ import annotations

import base64
import gzip
import os
import sqlite3
import tempfile
import threading
import time
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # dotenv optional — panel env vars still work
    pass

import httpx

# ── Config ──────────────────────────────────────────────────────────────────

TURSO_DATABASE_URL = (os.environ.get("TURSO_DATABASE_URL") or "").strip()
TURSO_AUTH_TOKEN = (os.environ.get("TURSO_AUTH_TOKEN") or "").strip()
TURSO_BACKUP_INTERVAL = int(os.environ.get("TURSO_BACKUP_INTERVAL") or "300")

# DB files that hold real game state worth syncing.
# pokeapi_cache.db is intentionally excluded (rebuildable cache).
SYNCED_DBS = ("ZNX.db", "t1727372828728stereolodu8.db")

_CHUNK_SIZE = 48_000  # chars per row — well under any sane column limits

_meta_table_sql = """
CREATE TABLE IF NOT EXISTS ro_cloud_backups (
    db_name      TEXT NOT NULL,
    backup_id    TEXT NOT NULL,
    chunk_index  INTEGER NOT NULL,
    chunk_count  INTEGER NOT NULL,
    data         TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (db_name, backup_id, chunk_index)
)
"""

_push_lock = threading.Lock()
_background_started = False


# ── HTTP plumbing ──────────────────────────────────────────────────────────


def turso_enabled() -> bool:
    return bool(TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)


def _https_url() -> str:
    url = TURSO_DATABASE_URL
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://") :]
    elif url.startswith("turso://"):
        url = "https://" + url[len("turso://") :]
    if not url.startswith("https://"):
        url = "https://" + url
    return url.rstrip("/")


def _tag(value):
    """Encode a Python value for Turso's Hrana JSON protocol."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(value)).decode()}
    return {"type": "text", "value": str(value)}


def _decode_value(v):
    """Inverse of _tag for rows we read back."""
    if not isinstance(v, dict):
        return v
    t = v.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(v.get("value") or 0)
    if t == "float":
        return v.get("value")
    if t == "blob":
        return base64.b64decode(v.get("base64") or "")
    return v.get("value")


def _execute(sql: str, args=None, *, want_rows: bool = True, timeout: float = 30.0):
    """Run one statement over Turso HTTP. Returns result dict or raises."""
    stmt = {"sql": sql, "want_rows": want_rows}
    if args:
        stmt["args"] = [_tag(a) for a in args]
    payload = {"stmt": stmt}
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = httpx.post(
        f"{_https_url()}/v1/execute", headers=headers, json=payload, timeout=timeout
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Turso HTTP {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"Turso error: {body['error']}")
    return body.get("result") or {}


def _rows(result) -> list[list]:
    cols = [c.get("name") for c in (result.get("cols") or [])]
    out = []
    for raw in result.get("rows") or []:
        out.append([_decode_value(v) for v in raw])
    return out


def ensure_meta_table() -> None:
    _execute(_meta_table_sql, want_rows=False)


# ── Local dump / restore ───────────────────────────────────────────────────


def _dump_db(path: str) -> bytes:
    """gzip a consistent snapshot of a SQLite file (WAL checkpointed first)."""
    src = sqlite3.connect(path, check_same_thread=False)
    try:
        try:
            src.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except sqlite3.Error:
            pass
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            dst = sqlite3.connect(tmp_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
            with open(tmp_path, "rb") as fh:
                return gzip.compress(fh.read(), compresslevel=6)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    finally:
        src.close()


def _restore_db(path: str, gz_bytes: bytes) -> None:
    raw = gzip.decompress(gz_bytes)
    # Write to a temp file first so a mid-write crash can't corrupt the live DB.
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=directory)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(raw)
        # Sanity: must open as a valid SQLite file.
        probe = sqlite3.connect(tmp_path)
        try:
            probe.execute("SELECT count(*) FROM sqlite_master")
        finally:
            probe.close()
        for suffix in ("", "-wal", "-shm"):
            stale = path + suffix
            if os.path.exists(stale):
                try:
                    os.unlink(stale)
                except OSError:
                    pass
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ── Push / pull ────────────────────────────────────────────────────────────


def push_db(path: str, *, timeout: float = 60.0) -> str | None:
    """Upload a gzip'd copy of `path` to Turso. Returns backup_id or None."""
    if not turso_enabled() or not os.path.exists(path):
        return None
    if not _push_lock.acquire(blocking=False):
        return None  # a push is already running
    try:
        blob = _dump_db(path)
        b64 = base64.b64encode(blob).decode()
        chunks = [b64[i : i + _CHUNK_SIZE] for i in range(0, len(b64), _CHUNK_SIZE)] or [
            ""
        ]
        backup_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        db_name = os.path.basename(path)
        created = datetime.now(timezone.utc).isoformat(timespec="seconds")

        ensure_meta_table()
        # Drop older backups for this DB so the table never grows unbounded.
        _execute(
            "DELETE FROM ro_cloud_backups WHERE db_name = ?",
            [db_name],
            want_rows=False,
        )
        for idx, chunk in enumerate(chunks):
            _execute(
                "INSERT INTO ro_cloud_backups "
                "(db_name, backup_id, chunk_index, chunk_count, data, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [db_name, backup_id, idx, len(chunks), chunk, created],
                want_rows=False,
                timeout=timeout,
            )
        return backup_id
    finally:
        _push_lock.release()


def latest_backup_id(db_name: str) -> str | None:
    ensure_meta_table()
    res = _execute(
        "SELECT backup_id FROM ro_cloud_backups WHERE db_name = ? "
        "ORDER BY created_at DESC, backup_id DESC LIMIT 1",
        [db_name],
    )
    rows = _rows(res)
    return rows[0][0] if rows else None


def pull_db(path: str, *, timeout: float = 60.0) -> bool:
    """Download the newest backup for `path` from Turso. True on success."""
    if not turso_enabled():
        return False
    db_name = os.path.basename(path)
    backup_id = latest_backup_id(db_name)
    if not backup_id:
        return False
    res = _execute(
        "SELECT chunk_index, chunk_count, data FROM ro_cloud_backups "
        "WHERE db_name = ? AND backup_id = ? ORDER BY chunk_index ASC",
        [db_name, backup_id],
        timeout=timeout,
    )
    rows = _rows(res)
    if not rows:
        return False
    chunk_count = int(rows[0][1])
    by_index = {int(r[0]): r[2] or "" for r in rows}
    if len(by_index) != chunk_count:
        raise RuntimeError(
            f"Turso backup incomplete for {db_name}: {len(by_index)}/{chunk_count} chunks"
        )
    b64 = "".join(by_index[i] for i in range(chunk_count))
    gz = base64.b64decode(b64)
    _restore_db(path, gz)
    return True


def _local_has_data(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        con = sqlite3.connect(path)
        try:
            row = con.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'"
            ).fetchone()
            if not row or row[0] == 0:
                return False
            n = con.execute("SELECT count(*) FROM users").fetchone()[0]
            return (n or 0) > 0
        finally:
            con.close()
    except sqlite3.Error:
        return False


def sync_on_startup() -> None:
    """Call once at bot start (before init_db). Pulls cloud data if local is empty."""
    if not turso_enabled():
        return
    for db_name in SYNCED_DBS:
        path = db_name
        try:
            if not _local_has_data(path):
                if pull_db(path):
                    print(f"[TURSO] Restored {path} from cloud backup")
                else:
                    # Fresh machine, nothing in cloud yet — seed the cloud
                    # with whatever local file exists (may be brand new).
                    if os.path.exists(path):
                        bid = push_db(path)
                        if bid:
                            print(f"[TURSO] Seeded cloud backup for {path} id={bid}")
            else:
                # Local already has data — ship it up so the cloud is current.
                bid = push_db(path)
                if bid:
                    print(f"[TURSO] Pushed local {path} to cloud id={bid}")
        except Exception as e:
            print(f"[TURSO] startup sync failed for {path}: {e}")


def push_all() -> None:
    if not turso_enabled():
        return
    for db_name in SYNCED_DBS:
        try:
            bid = push_db(db_name)
            if bid:
                print(f"[TURSO] Backup pushed {db_name} id={bid}")
        except Exception as e:
            print(f"[TURSO] push failed for {db_name}: {e}")


def push_all_safe() -> None:
    """Never raise — for shutdown hooks where an error must not block exit."""
    try:
        push_all()
    except Exception:
        pass


def start_background_sync() -> None:
    """Daemon thread: push a fresh backup every TURSO_BACKUP_INTERVAL seconds."""
    global _background_started
    if not turso_enabled() or _background_started:
        return
    _background_started = True

    def _loop() -> None:
        # First tick after the interval, so startup sync already covered boot.
        while True:
            time.sleep(max(30, TURSO_BACKUP_INTERVAL))
            try:
                push_all()
            except Exception:
                pass

    threading.Thread(target=_loop, name="turso-backup", daemon=True).start()
    print(f"[TURSO] Background backup every {TURSO_BACKUP_INTERVAL}s → {_https_url()}")


# ── Tiny CLI:  python turso_sync.py push|pull|status ───────────────────────

if __name__ == "__main__":
    import sys

    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if not turso_enabled():
        print("TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set")
        raise SystemExit(1)
    if cmd == "push":
        push_all()
    elif cmd == "pull":
        for name in SYNCED_DBS:
            ok = pull_db(name)
            print(f"pull {name}: {'OK' if ok else 'no backup in cloud'}")
    elif cmd == "status":
        print("URL:", _https_url())
        for name in SYNCED_DBS:
            try:
                bid = latest_backup_id(name)
                print(f"  {name}: latest={bid or '(none)'} local={'yes' if os.path.exists(name) else 'no'}")
            except Exception as e:
                print(f"  {name}: ERROR {e}")
    else:
        print("usage: turso_sync.py push|pull|status")
