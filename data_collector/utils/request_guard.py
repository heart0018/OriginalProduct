#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google API 呼び出し用のガード/キャッシュ層
- SQLite 永続キャッシュ（デフォルトTTLは関数引数）
- QPS制御（MAX_QPS env、デフォルト5）
- 並列制限（MAX_CONCURRENCY env、デフォルト3）
- Place Details 二重取得防止（place_idを一定期間メモ）
"""

import os
import time
import json
import sqlite3
import threading
import hashlib
from contextlib import contextmanager
from urllib.parse import urlencode
import requests

_BASE_DIR = os.path.dirname(os.path.dirname(__file__))
_CACHE_DIR = os.path.join(_BASE_DIR, ".cache")
os.makedirs(_CACHE_DIR, exist_ok=True)
_CACHE_DB = os.path.join(_CACHE_DIR, "google_cache.sqlite")

_lock = threading.Lock()
_sem = threading.Semaphore(int(os.getenv("MAX_CONCURRENCY", "3")))
_MAX_QPS = float(os.getenv("MAX_QPS", "5"))
_last_req_ts = [0.0]


def _now() -> int:
    return int(time.time())


def _key(url: str, params: dict) -> str:
    q = urlencode(sorted((params or {}).items()))
    s = f"{url}?{q}"
    return hashlib.sha256(s.encode()).hexdigest()


def _open_db():
    conn = sqlite3.connect(_CACHE_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_cache (
            k TEXT PRIMARY KEY,
            v BLOB NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS marker (
            k TEXT PRIMARY KEY,
            updated_at INTEGER NOT NULL
        )
        """
    )
    return conn


@contextmanager
def _rate_limit():
    with _sem:
        with _lock:
            elapsed = time.time() - _last_req_ts[0]
            min_interval = 1.0 / max(_MAX_QPS, 0.1)
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            _last_req_ts[0] = time.time()
        yield


def get_json(url: str, params: dict, ttl_sec: int = 60 * 60 * 24):
    """GETしてJSON返す。TTL内はキャッシュを返す。"""
    conn = _open_db()
    k = _key(url, params)
    row = conn.execute("SELECT v, updated_at FROM kv_cache WHERE k=?", (k,)).fetchone()
    if row and (_now() - row[1] <= ttl_sec):
        try:
            return json.loads(row[0])
        finally:
            conn.close()

    with _rate_limit():
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    conn.execute(
        "REPLACE INTO kv_cache(k, v, updated_at) VALUES(?,?,?)",
        (k, json.dumps(data, ensure_ascii=False), _now()),
    )
    conn.commit()
    conn.close()
    return data


def already_fetched_place(place_id: str, ttl_sec: int = 60 * 60 * 24 * 14) -> bool:
    """直近ttl_sec以内に同じplace_idのDetailsを取得済みか。"""
    if not place_id:
        return False
    conn = _open_db()
    k = f"place_details:{place_id}"
    row = conn.execute("SELECT updated_at FROM marker WHERE k=?", (k,)).fetchone()
    conn.close()
    return bool(row and (_now() - row[0] <= ttl_sec))


def mark_fetched_place(place_id: str):
    if not place_id:
        return
    conn = _open_db()
    k = f"place_details:{place_id}"
    conn.execute(
        "REPLACE INTO marker(k, updated_at) VALUES(?, ?)", (k, _now())
    )
    conn.commit()
    conn.close()


def get_photo_direct_url(photo_reference: str, maxwidth: int = 800, ttl_sec: int = 60*60*24*30) -> str | None:
    """Places Photoの302先URLを長期キャッシュ。
    直接URLを返すことでクライアントの都度API消費を防ぐ。
    """
    if not photo_reference:
        return None

    url = "https://maps.googleapis.com/maps/api/place/photo"
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY")
    params = {
        "maxwidth": maxwidth,
        "photo_reference": photo_reference,
        "key": api_key,
    }

    conn = _open_db()
    k = _key(url, params)
    row = conn.execute("SELECT v, updated_at FROM kv_cache WHERE k=?", (k,)).fetchone()
    if row and (_now() - row[1] <= ttl_sec):
        try:
            payload = json.loads(row[0])
            return payload.get("location")
        finally:
            conn.close()

    # 同日の再試行抑止
    day_key = f"photo:{photo_reference}:{time.strftime('%Y%m%d')}"
    seen = conn.execute("SELECT 1 FROM marker WHERE k=?", (day_key,)).fetchone()
    if seen and row:
        payload = json.loads(row[0])
        conn.close()
        return payload.get("location")

    with _rate_limit():
        resp = requests.get(url, params=params, allow_redirects=False, timeout=15)
    if resp.status_code == 302:
        loc = resp.headers.get("Location")
        payload = {"location": loc}
        conn.execute(
            "REPLACE INTO kv_cache(k, v, updated_at) VALUES(?,?,?)",
            (k, json.dumps(payload, ensure_ascii=False), _now()),
        )
        conn.execute("REPLACE INTO marker(k, updated_at) VALUES(?,?)", (day_key, _now()))
        conn.commit()
        conn.close()
        return loc
    else:
        conn.execute("REPLACE INTO marker(k, updated_at) VALUES(?,?)", (day_key, _now()))
        conn.commit()
        conn.close()
        return None
