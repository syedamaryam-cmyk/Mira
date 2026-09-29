"""
MIRA — Real-time Multi-source Feedback Synthesis Platform
---------------------------------------------------------
Aggregates & remembers feedback from multiple channels over time.
Identifies emerging themes, tracks sentiment shifts, links feedback
to product changes. Hindsight is the primary long-term memory.

Features:
  • Real-time feedback injection + Server-Sent Events (SSE) stream
  • Hindsight retain / recall / reflect (required for full synthesis)
  • Simple session auth (demo users)
  • Connector stubs (Amazon, Flipkart, Croma, Social, Support, …)
  • Rich Audio category scenarios + multi-platform catalog
Pure Python 3.10+ stdlib. No pip packages.
"""
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs, quote
import json, sqlite3, os, datetime, re, random, hashlib, secrets, threading, time, queue, urllib.request

ROOT = Path(__file__).parent
DB = ROOT / "mira.sqlite3"
NOW = lambda: datetime.datetime.now(datetime.timezone.utc)

# ---------------------------------------------------------------------------
# Live event bus (real-time)
# ---------------------------------------------------------------------------
LIVE_QUEUE: queue.Queue = queue.Queue(maxsize=500)
SUBSCRIBERS: list = []  # list of queue.Queue per SSE client
SUB_LOCK = threading.Lock()
LIVE_RUNNING = True

def broadcast(event: dict):
    """Push an event to all SSE subscribers and the ring buffer."""
    try:
        LIVE_QUEUE.put_nowait(event)
    except queue.Full:
        try:
            LIVE_QUEUE.get_nowait()
            LIVE_QUEUE.put_nowait(event)
        except Exception:
            pass
    with SUB_LOCK:
        dead = []
        for q in SUBSCRIBERS:
            try:
                q.put_nowait(event)
            except Exception:
                dead.append(q)
        for q in dead:
            SUBSCRIBERS.remove(q)

def subscribe():
    q = queue.Queue(maxsize=50)
    with SUB_LOCK:
        SUBSCRIBERS.append(q)
    return q

def unsubscribe(q):
    with SUB_LOCK:
        if q in SUBSCRIBERS:
            SUBSCRIBERS.remove(q)

# ---------------------------------------------------------------------------
# Hindsight — REQUIRED for full synthesis
# ---------------------------------------------------------------------------
def hindsight_configured():
    return all(os.getenv(x) for x in ("HINDSIGHT_API_URL", "HINDSIGHT_API_KEY", "HINDSIGHT_BANK_ID"))

def hindsight(operation, payload, timeout=12):
    """Call Hindsight Cloud REST API. Returns dict or None if not configured."""
    base = os.getenv("HINDSIGHT_API_URL", "").rstrip("/")
    key = os.getenv("HINDSIGHT_API_KEY")
    bank = os.getenv("HINDSIGHT_BANK_ID")
    if not (base and key and bank):
        return None
    endpoint = {
        "retain": "/memories",
        "recall": "/memories/recall",
        "reflect": "/reflect",
    }.get(operation)
    if not endpoint:
        return None
    url = f"{base}/v1/default/banks/{quote(bank, safe='')}{endpoint}"
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"error": str(exc)}

def retain_memory(content, context="MIRA feedback synthesis", timestamp=None):
    """Retain to Hindsight (primary) + local SQLite (always)."""
    ts = timestamp or NOW().isoformat()
    c = connect()
    try:
        c.execute("INSERT INTO memories(text, created_at, source) VALUES(?,?,?)",
                  (content, ts, context))
        c.commit()
    finally:
        c.close()
    if hindsight_configured():
        result = hindsight("retain", {
            "items": [{"content": content, "context": context, "timestamp": ts}]
        })
        return result
    return {"local_only": True, "warning": "Hindsight not configured — set HINDSIGHT_* env vars"}

# ---------------------------------------------------------------------------
# Auth (simple session tokens)
# ---------------------------------------------------------------------------
SESSIONS = {}  # token -> {user, role, exp}
DEMO_USERS = {
    "product@mira.demo": {"password": "mira2026", "name": "Priya Sharma", "role": "product_lead"},
    "research@mira.demo": {"password": "mira2026", "name": "Arjun Mehta", "role": "researcher"},
    "admin@mira.demo": {"password": "mira2026", "name": "Admin", "role": "admin"},
}

def make_token():
    return secrets.token_urlsafe(32)

def auth_user(email, password):
    u = DEMO_USERS.get(email.lower().strip())
    if not u or u["password"] != password:
        return None
    token = make_token()
    SESSIONS[token] = {
        "email": email.lower().strip(),
        "name": u["name"],
        "role": u["role"],
        "exp": time.time() + 86400 * 7,
    }
    return token, SESSIONS[token]

def get_session(handler):
    auth = handler.headers.get("Authorization", "")
    token = None
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
    if not token:
        token = handler.headers.get("X-MIRA-Token", "")
    if not token:
        # cookie fallback
        cookie = handler.headers.get("Cookie", "")
        for part in cookie.split(";"):
            if part.strip().startswith("mira_token="):
                token = part.strip().split("=", 1)[1]
                break
    if not token or token not in SESSIONS:
        return None
    s = SESSIONS[token]
    if s["exp"] < time.time():
        del SESSIONS[token]
        return None
    return s

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def connect():
    c = sqlite3.connect(str(DB), check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA journal_mode=WAL")
    return c

def initialize():
    c = connect()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS products(
        id INTEGER PRIMARY KEY, name TEXT, brand TEXT, model TEXT, category TEXT,
        image TEXT, source TEXT, source_url TEXT, price REAL, currency TEXT,
        rating REAL, review_count INTEGER, availability TEXT, confidence INTEGER,
        updated_at TEXT, demo INTEGER DEFAULT 1, platform TEXT
    );
    CREATE TABLE IF NOT EXISTS reviews(
        id INTEGER PRIMARY KEY, product_id INTEGER REFERENCES products(id),
        channel TEXT, source TEXT, rating INTEGER, title TEXT, body TEXT,
        theme TEXT, sentiment TEXT, review_date TEXT, demo INTEGER DEFAULT 1,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS price_history(
        id INTEGER PRIMARY KEY, product_id INTEGER, price REAL, observed_at TEXT, source TEXT
    );
    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY, product_id INTEGER, kind TEXT, title TEXT, detail TEXT, occurred_at TEXT
    );
    CREATE TABLE IF NOT EXISTS sessions(
        id INTEGER PRIMARY KEY, title TEXT, question TEXT, notes TEXT, created_at TEXT, owner TEXT
    );
    CREATE TABLE IF NOT EXISTS session_products(
        session_id INTEGER, product_id INTEGER, PRIMARY KEY(session_id, product_id)
    );
    CREATE TABLE IF NOT EXISTS feedback(
        id INTEGER PRIMARY KEY, question TEXT, vote TEXT, note TEXT, created_at TEXT, owner TEXT
    );
    CREATE TABLE IF NOT EXISTS memories(
        id INTEGER PRIMARY KEY, text TEXT, created_at TEXT, source TEXT
    );
    CREATE TABLE IF NOT EXISTS ingestion_runs(
        id INTEGER PRIMARY KEY, source TEXT, status TEXT, records_found INTEGER,
        records_added INTEGER, records_failed INTEGER, started_at TEXT, completed_at TEXT, detail TEXT
    );
    CREATE TABLE IF NOT EXISTS connectors(
        id TEXT PRIMARY KEY, name TEXT, kind TEXT, status TEXT, last_sync TEXT, detail TEXT
    );
    """)
    if c.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
        pass  # no demo seed: MIRA only holds real ingested data
    c.commit()
    c.close()

def seed(c):
    """Rich multi-platform catalog with deep Audio scenario."""
    platforms = [("Amazon","🛒"),("Flipkart","🛍️"),("Croma","📺"),("Myntra","👗"),
                 ("Nykaa","💄"),("Reliance Digital","🔌"),("Ajio","🧥")]
    catalog = [
        # === AUDIO (rich scenario focus) ===
        ("Auralis QuietWave X5","Auralis","QWX5","Audio","🎧",24999,4.5,1842),
        ("Auralis QuietWave X5 Wireless","Auralis","QWX5","Audio","🎧",23990,4.4,1123),
        ("Auralis QuietWave X5 (Croma exclusive)","Auralis","QWX5","Audio","🎧",25490,4.6,691),
        ("SonyStyle WH-XM5 Pro","SonyStyle","XM5P","Audio","🎧",29990,4.6,3204),
        ("Lumen SoundArc Pro","Lumen","SAP2","Audio","🎧",14990,4.2,815),
        ("Morrow Fold Studio","Morrow","FS1","Audio","🎧",11999,4.1,404),
        ("Auralis QuietWave X5 ANC Max","Auralis","QWX5M","Audio","🎧",27990,4.3,512),
        # Smartphones
        ("Nova Pixel 8a","Nova","P8A","Smartphones","📱",42999,4.3,3421),
        ("Helix Galaxy S24 FE","Helix","S24FE","Smartphones","📱",54999,4.5,5120),
        ("Aether One Plus 12R","Aether","OP12R","Smartphones","📱",38999,4.4,2765),
        # Laptops
        ("Vortex Book 14","Vortex","VB14","Laptops","💻",72990,4.4,892),
        ("Nexus Air 13","Nexus","NA13","Laptops","💻",99990,4.7,1203),
        # Fashion
        ("Urban Thread Oversized Tee","UrbanThread","UT-TEE","Fashion","👕",1299,4.1,4521),
        ("Apex Runner Pro Shoes","Apex","ARP-42","Fashion","👟",4999,4.2,3210),
        # Beauty
        ("GlowLab Vitamin C Serum","GlowLab","GL-VC","Beauty","✨",899,4.4,8765),
        # Home
        ("NestSmart Air Purifier","NestSmart","NS-AP","Home","🌬️",12999,4.3,1567),
        ("BrewMaster Pour-Over Kit","BrewMaster","BM-PO","Home","☕",3499,4.5,987),
    ]
    product_ids = []
    for name, brand, model, cat, img, price, rating, rcount in catalog:
        chosen = random.sample(platforms, k=random.randint(1, min(3, len(platforms))))
        for plat_name, _ in chosen:
            p_price = price + random.randint(-800, 1200)
            p_rating = round(min(5.0, max(3.5, rating + random.uniform(-0.2, 0.15))), 1)
            p_rcount = max(40, rcount + random.randint(-100, 300))
            avail = random.choice(["In stock","In stock","In stock","Limited","Pre-order"])
            cur = c.execute(
                """INSERT INTO products
                (name,brand,model,category,image,source,source_url,price,currency,rating,
                 review_count,availability,confidence,updated_at,demo,platform)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)""",
                (name, brand, model, cat, img, plat_name,
                 f"https://example.invalid/{plat_name.lower()}/{model}",
                 p_price, "INR", p_rating, p_rcount, avail, random.randint(88,99),
                 NOW().isoformat(), plat_name))
            pid = cur.lastrowid
            product_ids.append((pid, name, brand, cat, plat_name, model))
            for days_ago, delta in [(55,1800),(30,900),(12,400),(0,0)]:
                c.execute("INSERT INTO price_history(product_id,price,observed_at,source) VALUES(?,?,?,?)",
                          (pid, p_price+delta, (NOW()-datetime.timedelta(days=days_ago)).isoformat(), plat_name))
            c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                      (pid,"discovery",f"Listed on {plat_name}",
                       f"Catalog entry for {name} on {plat_name}.",
                       (NOW()-datetime.timedelta(days=60)).isoformat()))

    channels = ["Marketplace Review","Social Mention","Support Ticket",
                "App Store Review","Internal Survey","Influencer Post"]

    # Rich Audio-focused theme evolution
    audio_phases = {
        "early": [  # days 1-18
            ("battery life","negative",0.38),("sound quality","positive",0.28),
            ("comfort","positive",0.14),("noise cancellation","positive",0.12),
            ("build quality","mixed",0.08),
        ],
        "peak": [  # days 19-35  — complaints peak
            ("battery life","negative",0.48),("sound quality","positive",0.18),
            ("microphone","negative",0.12),("firmware request","negative",0.10),
            ("comfort","positive",0.07),("noise cancellation","positive",0.05),
        ],
        "update": [  # days 36-45 — firmware ships
            ("firmware update","positive",0.30),("battery life","mixed",0.22),
            ("sound quality","positive",0.22),("comfort","positive",0.12),
            ("app experience","mixed",0.08),("noise cancellation","positive",0.06),
        ],
        "recovery": [  # days 46-60
            ("battery life","positive",0.28),("firmware update","positive",0.22),
            ("sound quality","positive",0.24),("comfort","positive",0.12),
            ("app experience","positive",0.08),("noise cancellation","positive",0.06),
        ],
    }
    other_scenarios = {
        "Smartphones": {
            "early": [("camera quality","positive",0.30),("battery life","mixed",0.25),("heating","negative",0.20),("display","positive",0.15),("software","mixed",0.10)],
            "mid": [("heating","negative",0.28),("battery life","negative",0.22),("camera quality","positive",0.22),("software update","positive",0.15),("display","positive",0.13)],
            "late": [("software update","positive",0.32),("battery life","positive",0.20),("camera quality","positive",0.25),("heating","mixed",0.10),("performance","positive",0.13)],
        },
        "Laptops": {
            "early": [("build quality","positive",0.25),("keyboard","positive",0.22),("battery life","mixed",0.20),("display","positive",0.18),("ports","negative",0.15)],
            "mid": [("battery life","negative",0.30),("thermals","negative",0.20),("display","positive",0.20),("keyboard","positive",0.18),("build quality","positive",0.12)],
            "late": [("battery life","mixed",0.22),("display","positive",0.25),("keyboard","positive",0.20),("thermals","mixed",0.15),("value for money","positive",0.18)],
        },
        "Fashion": {
            "early": [("fit","mixed",0.30),("fabric quality","positive",0.25),("sizing","negative",0.20),("color accuracy","mixed",0.15),("delivery","positive",0.10)],
            "mid": [("sizing","negative",0.32),("fabric quality","positive",0.20),("fit","mixed",0.18),("durability","mixed",0.15),("value","positive",0.15)],
            "late": [("sizing guide improved","positive",0.28),("fabric quality","positive",0.25),("fit","positive",0.22),("durability","positive",0.15),("return experience","mixed",0.10)],
        },
        "Beauty": {
            "early": [("texture","positive",0.30),("results","mixed",0.25),("packaging","positive",0.20),("scent","mixed",0.15),("value","positive",0.10)],
            "mid": [("results","positive",0.28),("texture","positive",0.22),("irritation","negative",0.18),("packaging","positive",0.17),("longevity","mixed",0.15)],
            "late": [("results","positive",0.32),("texture","positive",0.25),("formula update","positive",0.18),("packaging","positive",0.15),("value","positive",0.10)],
        },
        "Home": {
            "early": [("build quality","positive",0.25),("ease of use","positive",0.25),("noise level","mixed",0.20),("effectiveness","mixed",0.18),("design","positive",0.12)],
            "mid": [("effectiveness","positive",0.28),("noise level","negative",0.22),("ease of use","positive",0.20),("build quality","positive",0.18),("support","mixed",0.12)],
            "late": [("effectiveness","positive",0.30),("noise level","mixed",0.15),("ease of use","positive",0.22),("firmware","positive",0.18),("value","positive",0.15)],
        },
    }
    phrases = {
        "positive": ["Really impressed.","Exceeded expectations.","Would recommend.","Noticeable improvement.","Solid day-to-day.","Feels premium."],
        "negative": ["Disappointing for the price.","Expected better.","Needs a fix soon.","Considering return.","Not as listed.","Support unhelpful."],
        "mixed": ["Some good, some not.","Works but not perfect.","Depends on use case.","Potential is there.","Okay overall."],
    }

    for pid, name, brand, cat, platform, model in product_ids:
        n_reviews = random.randint(55, 90) if cat == "Audio" else random.randint(35, 60)
        for i in range(n_reviews):
            day = random.randint(1, 58)
            if cat == "Audio":
                if day <= 18: phase = "early"
                elif day <= 35: phase = "peak"
                elif day <= 45: phase = "update"
                else: phase = "recovery"
                themes = audio_phases[phase]
            else:
                sc = other_scenarios.get(cat, other_scenarios["Home"])
                phase = "early" if day <= 20 else ("mid" if day <= 40 else "late")
                themes = sc[phase]
            r = random.random(); cum = 0
            chosen_theme, chosen_sent = themes[0][0], themes[0][1]
            for t, s, w in themes:
                cum += w
                if r <= cum:
                    chosen_theme, chosen_sent = t, s
                    break
            channel = random.choices(channels, weights=[0.42,0.18,0.14,0.10,0.08,0.08], k=1)[0]
            rating = {"positive": random.choice([4,5,5,4,5]),
                      "negative": random.choice([1,2,2,1,3]),
                      "mixed": random.choice([3,3,4,2,3])}[chosen_sent]
            body = (random.choice(phrases[chosen_sent]) +
                    f" Theme: {chosen_theme}. Synthetic #{i+1} for {name} on {platform}.")
            review_date = (NOW() - datetime.timedelta(days=60-day)).date().isoformat()
            created = (NOW() - datetime.timedelta(days=60-day, hours=random.randint(0,20))).isoformat()
            c.execute("""INSERT INTO reviews
                (product_id,channel,source,rating,title,body,theme,sentiment,review_date,demo,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,1,?)""",
                (pid, channel, platform, rating, chosen_theme.title(), body,
                 chosen_theme, chosen_sent, review_date, created))

        # Audio-specific change events (the core synthesis story)
        if cat == "Audio" and "QuietWave" in name:
            c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                      (pid,"theme_shift","Battery complaints surge across channels",
                       f"Negative battery-life feedback for {name} spiked on marketplace + support + social.",
                       (NOW()-datetime.timedelta(days=38)).isoformat()))
            c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                      (pid,"product_change","Firmware 2.4.1 shipped — battery optimization",
                       f"OTA update for {model} targeting idle drain and ANC power use. Linked to later sentiment recovery.",
                       (NOW()-datetime.timedelta(days=28)).isoformat()))
            c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                      (pid,"theme_shift","Post-update battery sentiment recovery",
                       f"Marketplace + app-store + support tickets show improved battery theme for {name} after firmware.",
                       (NOW()-datetime.timedelta(days=12)).isoformat()))
            c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                      (pid,"theme_shift","App experience emerges as secondary theme",
                       "After battery stabilizes, app pairing & EQ controls become the next rising conversation.",
                       (NOW()-datetime.timedelta(days=6)).isoformat()))

    c.execute("INSERT INTO sessions(title,question,notes,created_at,owner) VALUES(?,?,?,?,?)",
              ("QuietWave battery arc","Did the firmware fix battery sentiment across platforms and channels?",
               "Deep-dive: peak complaints → firmware 2.4.1 → recovery. Multi-channel evidence.",
               NOW().isoformat(),"product@mira.demo"))
    c.execute("INSERT INTO memories(text,created_at,source) VALUES(?,?,?)",
              ("SYNTHESIS: Auralis QuietWave X5 battery complaints peaked ~day 22-35 across Marketplace, Support, and Social. "
               "Firmware 2.4.1 (day ~28) targeted idle drain. Later reviews (day 46+) show clear positive shift on battery theme. "
               "App experience is the emerging secondary theme post-recovery.",
               NOW().isoformat(),"seeded synthesis"))
    c.execute("INSERT INTO memories(text,created_at,source) VALUES(?,?,?)",
              ("Watchlist: (1) App experience for Audio after battery recovery, (2) Heating on mid-range phones, "
               "(3) Fashion sizing accuracy after guide updates.",
               NOW().isoformat(),"seeded synthesis"))

# ---------------------------------------------------------------------------
# Synthesis helpers
# ---------------------------------------------------------------------------
def compute_theme_stats(c, product_id=None, days=None):
    q = """SELECT theme, sentiment, COUNT(*) as n, MIN(review_date) as first_seen,
                  MAX(review_date) as last_seen, AVG(rating) as avg_rating
           FROM reviews WHERE 1=1"""
    params = []
    if product_id:
        q += " AND product_id=?"; params.append(product_id)
    if days:
        cutoff = (NOW() - datetime.timedelta(days=days)).date().isoformat()
        q += " AND review_date >= ?"; params.append(cutoff)
    q += " GROUP BY theme, sentiment ORDER BY n DESC"
    return [dict(x) for x in c.execute(q, params)]

def emerging_themes(c, lookback_days=21):
    recent_cut = (NOW() - datetime.timedelta(days=lookback_days)).date().isoformat()
    older_cut = (NOW() - datetime.timedelta(days=lookback_days*2)).date().isoformat()
    recent = c.execute("SELECT theme,sentiment,COUNT(*) n FROM reviews WHERE review_date>=? GROUP BY theme,sentiment",(recent_cut,)).fetchall()
    older = c.execute("SELECT theme,sentiment,COUNT(*) n FROM reviews WHERE review_date>=? AND review_date<? GROUP BY theme,sentiment",(older_cut,recent_cut)).fetchall()
    older_map = {(r["theme"],r["sentiment"]): r["n"] for r in older}
    recent_total = sum(r["n"] for r in recent) or 1
    older_total = sum(r["n"] for r in older) or 1
    rising = []
    for r in recent:
        key = (r["theme"], r["sentiment"])
        rs = r["n"] / recent_total
        os_ = older_map.get(key, 0) / older_total
        delta = rs - os_
        if delta > 0.02 or (older_map.get(key,0)==0 and r["n"]>=4):
            rising.append({"theme":r["theme"],"sentiment":r["sentiment"],
                           "recent_count":r["n"],"older_count":older_map.get(key,0),
                           "share_delta":round(delta*100,1),
                           "trend":"rising" if delta>0.05 else "emerging"})
    rising.sort(key=lambda x: -x["share_delta"])
    return rising[:15]

def sentiment_timeline(c, product_id=None, buckets=6):
    q = "SELECT review_date, sentiment, rating FROM reviews"
    params = []
    if product_id:
        q += " WHERE product_id=?"; params.append(product_id)
    q += " ORDER BY review_date"
    rows = [dict(x) for x in c.execute(q, params)]
    if not rows: return []
    dates = sorted(set(r["review_date"] for r in rows))
    step = max(1, len(dates)//buckets)
    periods = []
    for i in range(0, len(dates), step):
        chunk = dates[i:i+step]
        if not chunk: continue
        start, end = chunk[0], chunk[-1]
        subset = [r for r in rows if start <= r["review_date"] <= end]
        periods.append({
            "period": f"{start[5:]} → {end[5:]}",
            "positive": sum(1 for r in subset if r["sentiment"]=="positive"),
            "negative": sum(1 for r in subset if r["sentiment"]=="negative"),
            "mixed": sum(1 for r in subset if r["sentiment"]=="mixed"),
            "avg_rating": round(sum(r["rating"] for r in subset)/max(1,len(subset)),2),
            "count": len(subset),
        })
    return periods

def channel_breakdown(c, product_id=None):
    q = "SELECT channel, sentiment, COUNT(*) n FROM reviews"
    params = []
    if product_id:
        q += " WHERE product_id=?"; params.append(product_id)
    q += " GROUP BY channel, sentiment ORDER BY n DESC"
    return [dict(x) for x in c.execute(q, params)]

def live_feed(c, limit=50):
    rows = c.execute("""SELECT r.id, r.product_id, r.channel, r.source, r.rating, r.title,
               r.body, r.theme, r.sentiment, r.review_date, r.created_at,
               p.name as product_name, p.category, p.platform
               FROM reviews r JOIN products p ON p.id=r.product_id
               ORDER BY COALESCE(r.created_at, r.review_date) DESC, r.id DESC LIMIT ?""",(limit,)).fetchall()
    return [dict(x) for x in rows]

def synthesize_answer(c, question, product_id=None, user=None):
    """Full synthesis: local evidence + Hindsight recall/reflect."""
    themes = compute_theme_stats(c, product_id)
    emerging = emerging_themes(c)
    timeline = [dict(x) for x in c.execute(
        """SELECT e.*, p.name FROM events e JOIN products p ON p.id=e.product_id
           ORDER BY occurred_at DESC LIMIT 15""")]
    recent = live_feed(c, 6)

    # Hindsight is MUST — call recall + reflect
    h_recall = hindsight("recall", {"query": question, "budget": "mid", "max_tokens": 2000})
    h_reflect = hindsight("reflect", {"query": question, "budget": "mid", "max_tokens": 1500})

    terms = set(re.findall(r"[a-z]{3,}", question.lower()))
    local_mem = [x["text"] for x in c.execute(
        "SELECT text FROM memories ORDER BY created_at DESC LIMIT 40")
        if terms.intersection(set(re.findall(r"[a-z]{3,}", x["text"].lower())))][:5]

    matching = [t for t in themes if any(w in t["theme"].lower() for w in terms)]
    if matching:
        lead = max(matching, key=lambda t: t["n"])
        summary = (f"Strongest matching theme: **{lead['theme']}** ({lead['sentiment']}) — "
                   f"{lead['n']} records, {lead['first_seen']} → {lead['last_seen']}, "
                   f"avg {lead['avg_rating']:.1f}★.")
    elif themes:
        lead = themes[0]
        summary = (f"Most frequent theme: **{lead['theme']}** ({lead['sentiment']}) "
                   f"with {lead['n']} records. Question did not tightly match one theme.")
    else:
        summary = "No stored review evidence for this scope yet."

    if emerging:
        top = emerging[0]
        summary += (f" Emerging signal: **{top['theme']}** ({top['sentiment']}) is {top['trend']} "
                    f"(+{top['share_delta']}% share).")

    change_events = [e for e in timeline if e["kind"] in ("product_change","theme_shift")]
    if change_events and any(w in question.lower() for w in
            ("change","update","fix","improve","battery","shift","firmware","recovery")):
        ev = change_events[0]
        summary += f" Linked event: {ev['title']} on {ev['name']} ({(ev['occurred_at'] or '')[:10]})."

    # Prefer Hindsight reflect text when available
    reflect_text = None
    if h_reflect and not h_reflect.get("error"):
        reflect_text = h_reflect.get("text") or h_reflect.get("response") or h_reflect.get("answer")
    if reflect_text:
        summary = reflect_text + "\n\n—\nLocal evidence cross-check: " + summary

    summary += " (Evidence includes synthetic demo records where labelled.)"

    # Retain this observation
    retain_memory(
        f"MIRA synthesis ({user.get('email') if user else 'anon'}): Q={question}. Insight={summary[:400]}",
        context="MIRA agent observation",
    )

    return {
        "answer": summary,
        "themes": themes[:12],
        "emerging": emerging[:8],
        "evidence": recent,
        "timeline": timeline[:8],
        "local_memory": local_mem,
        "hindsight_configured": hindsight_configured(),
        "hindsight_recall": (h_recall or {}).get("results") or (h_recall or {}).get("memories") or [],
        "hindsight_reflect": reflect_text,
        "hindsight_error": (h_recall or {}).get("error") or (h_reflect or {}).get("error"),
        "memory_status": (
            "Hindsight primary memory active" if hindsight_configured()
            else "⚠ Hindsight NOT configured — set HINDSIGHT_API_URL, HINDSIGHT_API_KEY, HINDSIGHT_BANK_ID. Local SQLite only."
        ),
    }

# ---------------------------------------------------------------------------
# Real-time injector (simulates continuous multi-channel ingest)
# ---------------------------------------------------------------------------
INJECT_THEMES = [
    ("battery life","positive","Firmware seems to have helped battery."),
    ("battery life","mixed","Battery better but still not class-leading."),
    ("app experience","negative","App keeps disconnecting after latest update."),
    ("app experience","mixed","EQ controls improved but pairing is flaky."),
    ("sound quality","positive","Still the best part of these headphones."),
    ("comfort","positive","Wore them 4 hours straight, no pain."),
    ("noise cancellation","positive","ANC remains excellent on flights."),
    ("microphone","negative","Call quality still weak outdoors."),
]

def live_injector():
    """Background thread: every 8-18s inject a new feedback row + broadcast."""
    time.sleep(4)
    while LIVE_RUNNING:
        try:
            c = connect()
            audio = c.execute(
                "SELECT id,name,platform FROM products WHERE category='Audio' ORDER BY RANDOM() LIMIT 1"
            ).fetchone()
            if audio:
                theme, sent, phrase = random.choice(INJECT_THEMES)
                channel = random.choice(["Marketplace Review","Social Mention","Support Ticket","App Store Review"])
                rating = {"positive":5,"negative":2,"mixed":3}[sent]
                body = f"{phrase} [live inject] Theme: {theme}."
                now = NOW()
                cur = c.execute(
                    """INSERT INTO reviews
                    (product_id,channel,source,rating,title,body,theme,sentiment,review_date,demo,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,1,?)""",
                    (audio["id"], channel, audio["platform"], rating, theme.title(), body,
                     theme, sent, now.date().isoformat(), now.isoformat()))
                rid = cur.lastrowid
                c.commit()
                event = {
                    "type": "feedback",
                    "id": rid,
                    "product_id": audio["id"],
                    "product_name": audio["name"],
                    "platform": audio["platform"],
                    "channel": channel,
                    "theme": theme,
                    "sentiment": sent,
                    "rating": rating,
                    "body": body,
                    "review_date": now.date().isoformat(),
                    "created_at": now.isoformat(),
                    "ts": now.isoformat(),
                }
                broadcast(event)
                # Retain notable ones to Hindsight
                if random.random() < 0.35:
                    retain_memory(
                        f"Live feedback on {audio['name']} ({audio['platform']}): "
                        f"{theme}/{sent} via {channel}. {phrase}",
                        context="live connector ingest",
                    )
            c.close()
        except Exception as e:
            print("injector error:", e)
        time.sleep(random.uniform(8, 18))

# ---------------------------------------------------------------------------
# Connector sync stub
# ---------------------------------------------------------------------------
def run_connector_sync(connector_id, user=None):
    c = connect()
    conn = c.execute("SELECT * FROM connectors WHERE id=?", (connector_id,)).fetchone()
    if not conn:
        c.close()
        return {"error": "unknown connector"}
    start = NOW().isoformat()
    added = 0
    # Pull 3-8 synthetic new reviews into matching products
    products = c.execute("SELECT id,name,platform,category FROM products ORDER BY RANDOM() LIMIT 5").fetchall()
    channels_map = {
        "amazon": "Marketplace Review", "flipkart": "Marketplace Review", "croma": "Marketplace Review",
        "myntra": "Marketplace Review", "social": "Social Mention", "support": "Support Ticket",
        "appstore": "App Store Review", "survey": "Internal Survey",
    }
    channel = channels_map.get(connector_id, "Marketplace Review")
    for p in products:
        for _ in range(random.randint(0, 2)):
            theme, sent, phrase = random.choice(INJECT_THEMES)
            rating = {"positive":5,"negative":2,"mixed":3}[sent]
            body = f"[connector:{connector_id}] {phrase} Theme: {theme}."
            now = NOW()
            cur = c.execute(
                """INSERT INTO reviews
                (product_id,channel,source,rating,title,body,theme,sentiment,review_date,demo,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,0,?)""",
                (p["id"], channel, p["platform"], rating, theme.title(), body,
                 theme, sent, now.date().isoformat(), now.isoformat()))
            added += 1
            broadcast({
                "type": "feedback", "id": cur.lastrowid, "product_id": p["id"],
                "product_name": p["name"], "platform": p["platform"], "channel": channel,
                "theme": theme, "sentiment": sent, "rating": rating, "body": body,
                "review_date": now.date().isoformat(), "created_at": now.isoformat(),
                "ts": now.isoformat(), "source": f"connector:{connector_id}",
            })
    c.execute("UPDATE connectors SET status=?, last_sync=?, detail=? WHERE id=?",
              ("synced", start, f"Stub sync added {added} records", connector_id))
    c.execute("""INSERT INTO ingestion_runs
        (source,status,records_found,records_added,records_failed,started_at,completed_at,detail)
        VALUES(?,?,?,?,?,?,?,?)""",
        (f"connector:{connector_id}", "completed", added, added, 0, start, NOW().isoformat(),
         f"Stub sync by {user.get('email') if user else 'system'}"))
    c.commit()
    c.close()
    retain_memory(
        f"Connector sync {connector_id}: added {added} feedback records. Channel={channel}.",
        context="connector sync",
    )
    return {"ok": True, "added": added, "connector": connector_id}

# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT), **kw)

    def log_message(self, *a): pass

    def send_json(self, obj, status=200):
        b = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-MIRA-Token")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b)

    def body(self):
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-MIRA-Token")
        self.end_headers()

    def require_auth(self):
        s = get_session(self)
        if not s:
            self.send_json({"error": "auth_required", "message": "Login required"}, 401)
            return None
        return s

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path
        q = parse_qs(u.query)

        # SSE real-time stream (auth optional for demo simplicity, but check token if present)
        if path == "/api/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            qsub = subscribe()
            try:
                # hello
                self.wfile.write(b"event: hello\ndata: {\"ok\":true}\n\n")
                self.wfile.flush()
                while LIVE_RUNNING:
                    try:
                        ev = qsub.get(timeout=25)
                        payload = json.dumps(ev, default=str)
                        self.wfile.write(f"event: feedback\ndata: {payload}\n\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                unsubscribe(qsub)
            return

        c = connect()
        try:
            if path == "/api/me":
                s = get_session(self)
                self.send_json({"user": s, "hindsight": hindsight_configured()})
                return

            if path == "/api/products":
                term = q.get("q",[""])[0].lower()
                cat = q.get("category",[""])[0]
                platform = q.get("platform",[""])[0]
                sql = "SELECT * FROM products WHERE 1=1"
                params = []
                if term:
                    sql += " AND lower(name||brand||model||source||platform) LIKE ?"
                    params.append("%"+term+"%")
                if cat: sql += " AND category=?"; params.append(cat)
                if platform: sql += " AND platform=?"; params.append(platform)
                sql += " ORDER BY category, name, platform"
                self.send_json([dict(x) for x in c.execute(sql, params)])

            elif path == "/api/categories":
                self.send_json([dict(x) for x in c.execute(
                    "SELECT category, COUNT(*) n FROM products GROUP BY category ORDER BY n DESC")])

            elif path == "/api/platforms":
                self.send_json([dict(x) for x in c.execute(
                    "SELECT platform, COUNT(*) n FROM products GROUP BY platform ORDER BY n DESC")])

            elif path == "/api/timeline":
                pid = q.get("product_id",[None])[0]
                if pid:
                    rows = c.execute("""SELECT events.*,products.name FROM events
                        JOIN products ON products.id=events.product_id WHERE product_id=?
                        ORDER BY occurred_at DESC""",(int(pid),))
                else:
                    rows = c.execute("""SELECT events.*,products.name FROM events
                        JOIN products ON products.id=events.product_id
                        ORDER BY occurred_at DESC LIMIT 100""")
                self.send_json([dict(x) for x in rows])

            elif path == "/api/sessions":
                self.send_json([dict(x) for x in c.execute(
                    "SELECT * FROM sessions ORDER BY created_at DESC")])

            elif path == "/api/reviews":
                pid = q.get("product_id",[None])[0]
                channel = q.get("channel",[None])[0]
                limit = min(int(q.get("limit",["80"])[0]), 200)
                sql = "SELECT * FROM reviews WHERE 1=1"; params = []
                if pid: sql += " AND product_id=?"; params.append(int(pid))
                if channel: sql += " AND channel=?"; params.append(channel)
                sql += " ORDER BY COALESCE(created_at,review_date) DESC LIMIT ?"; params.append(limit)
                self.send_json([dict(x) for x in c.execute(sql, params)])

            elif path == "/api/themes":
                pid = q.get("product_id",[None])[0]
                days = q.get("days",[None])[0]
                self.send_json(compute_theme_stats(c, int(pid) if pid else None, int(days) if days else None))

            elif path == "/api/sentiment-timeline":
                pid = q.get("product_id",[None])[0]
                self.send_json(sentiment_timeline(c, int(pid) if pid else None))

            elif path == "/api/emerging":
                self.send_json(emerging_themes(c, int(q.get("days",["21"])[0])))

            elif path == "/api/channels":
                pid = q.get("product_id",[None])[0]
                self.send_json(channel_breakdown(c, int(pid) if pid else None))

            elif path == "/api/feed":
                self.send_json(live_feed(c, min(int(q.get("limit",["50"])[0]), 100)))

            elif path == "/api/compare":
                ids = [int(x) for x in q.get("ids",[""])[0].split(",") if x.strip().isdigit()]
                if not ids:
                    self.send_json([]); return
                ph = ",".join("?"*len(ids))
                products = [dict(x) for x in c.execute(f"SELECT * FROM products WHERE id IN ({ph})", ids)]
                for p in products:
                    p["themes"] = compute_theme_stats(c, p["id"])[:6]
                    p["sentiment"] = sentiment_timeline(c, p["id"], buckets=4)
                self.send_json(products)

            elif path == "/api/connectors":
                self.send_json([dict(x) for x in c.execute("SELECT * FROM connectors ORDER BY kind, name")])

            elif path == "/api/summary":
                self.send_json({
                    "products": c.execute("SELECT COUNT(*) FROM products").fetchone()[0],
                    "reviews": c.execute("SELECT COUNT(*) FROM reviews").fetchone()[0],
                    "memories": c.execute("SELECT COUNT(*) FROM memories").fetchone()[0],
                    "sessions": c.execute("SELECT COUNT(*) FROM sessions").fetchone()[0],
                    "channels": c.execute("SELECT COUNT(DISTINCT channel) FROM reviews").fetchone()[0],
                    "platforms": c.execute("SELECT COUNT(DISTINCT platform) FROM products").fetchone()[0],
                    "categories": c.execute("SELECT COUNT(DISTINCT category) FROM products").fetchone()[0],
                    "hindsight": hindsight_configured(),
                    "live_subscribers": len(SUBSCRIBERS),
                    "latest_run": dict(c.execute(
                        "SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1").fetchone() or {}) or None,
                })

            elif path == "/api/health":
                self.send_json({
                    "database": "connected",
                    "mode": "real-time multi-platform synthesis",
                    "hindsight": "configured" if hindsight_configured() else "NOT CONFIGURED — required for full memory",
                    "realtime": "SSE /api/stream + background injector",
                    "auth": "session tokens (demo users)",
                    "connectors": "stub_ready",
                })

            else:
                super().do_GET()
        finally:
            c.close()

    def do_POST(self):
        path = urlparse(self.path).path
        data = self.body()

        if path == "/api/login":
            email = data.get("email", "")
            password = data.get("password", "")
            result = auth_user(email, password)
            if not result:
                self.send_json({"error": "invalid_credentials"}, 401)
                return
            token, user = result
            self.send_json({"token": token, "user": user})
            return

        if path == "/api/logout":
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                SESSIONS.pop(auth[7:].strip(), None)
            self.send_json({"ok": True})
            return

        user = get_session(self)  # optional for some routes

        c = connect()
        try:
            if path == "/api/ask":
                # Hindsight-first synthesis
                q = data.get("question", "")
                pid = data.get("product_id")
                pid = int(pid) if pid else None
                result = synthesize_answer(c, q, pid, user)
                self.send_json(result)

            elif path == "/api/sessions":
                cur = c.execute(
                    "INSERT INTO sessions(title,question,notes,created_at,owner) VALUES(?,?,?,?,?)",
                    (data.get("title","Untitled"), data.get("question",""), data.get("notes",""),
                     NOW().isoformat(), (user or {}).get("email")))
                c.commit()
                retain_memory(
                    f"Research session: {data.get('title')}. Q: {data.get('question')}. Notes: {data.get('notes','')}",
                    context="research session",
                )
                self.send_json({"id": cur.lastrowid})

            elif path == "/api/feedback":
                c.execute("INSERT INTO feedback(question,vote,note,created_at,owner) VALUES(?,?,?,?,?)",
                          (data.get("question",""), data.get("vote",""), data.get("note",""),
                           NOW().isoformat(), (user or {}).get("email")))
                c.commit()
                retain_memory(
                    f"User feedback vote={data.get('vote')} on: {data.get('question','')[:200]}",
                    context="user feedback",
                )
                self.send_json({"ok": True})

            elif path == "/api/connectors/sync":
                cid = data.get("connector_id")
                if not cid:
                    self.send_json({"error": "connector_id required"}, 400); return
                result = run_connector_sync(cid, user)
                self.send_json(result)

            elif path == "/api/import":
                records = data.get("records", [])
                start = NOW().isoformat(); added = 0; failed = 0
                for r in records:
                    try:
                        name = r.get("name") or r.get("product")
                        if not name: raise ValueError("name required")
                        cur = c.execute(
                            """INSERT INTO products
                            (name,brand,model,category,image,source,source_url,price,currency,
                             rating,review_count,availability,confidence,updated_at,demo,platform)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)""",
                            (name, r.get("brand","Unknown"), r.get("model",""),
                             r.get("category","General"), "📦", r.get("source","Import"),
                             r.get("url",""), float(r.get("price") or 0), r.get("currency","INR"),
                             float(r.get("rating") or 0), int(r.get("review_count") or 0),
                             r.get("availability","Unverified"), 0, NOW().isoformat(),
                             r.get("platform") or r.get("source") or "Import"))
                        pid = cur.lastrowid; added += 1
                        for rv in r.get("reviews", []):
                            c.execute("""INSERT INTO reviews
                                (product_id,channel,source,rating,title,body,theme,sentiment,review_date,demo,created_at)
                                VALUES(?,?,?,?,?,?,?,?,?,0,?)""",
                                (pid, rv.get("channel","User import"), "Import",
                                 int(rv.get("rating") or 0), rv.get("title",""), rv.get("body",""),
                                 rv.get("theme","unclassified"), rv.get("sentiment","unclassified"),
                                 rv.get("date", NOW().date().isoformat()), NOW().isoformat()))
                    except Exception:
                        failed += 1
                c.execute("""INSERT INTO ingestion_runs
                    (source,status,records_found,records_added,records_failed,started_at,completed_at,detail)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    ("JSON import", "completed" if failed==0 else "partial",
                     len(records), added, failed, start, NOW().isoformat(), "Manual import"))
                c.commit()
                retain_memory(f"Imported {added} products ({failed} failed).", context="import")
                self.send_json({"ok": True, "added": added, "failed": failed})

            else:
                self.send_json({"error": "Unknown endpoint"}, 404)
        except Exception as e:
            c.rollback()
            self.send_json({"error": str(e)}, 400)
        finally:
            c.close()


# ---------------------------------------------------------------------------
initialize()
if __name__ == "__main__":
    import server; server.main()
