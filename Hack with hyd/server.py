"""MIRA server: real accounts, tracked sources, live polling, change-impact. Run: python server.py"""
import app, connectors as cx, json, os, hashlib, secrets, threading, time, datetime
from urllib.parse import urlparse, parse_qs
NOW = app.NOW
SYNC_EVERY = int(os.getenv("SYNC_INTERVAL_SEC", "300"))

def migrate():
    c = app.connect()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(email TEXT PRIMARY KEY, name TEXT, role TEXT, salt TEXT, pw TEXT);
    CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY, product_id INTEGER, kind TEXT, ref TEXT,
        status TEXT DEFAULT 'new', last_sync TEXT, detail TEXT, total INTEGER DEFAULT 0, UNIQUE(product_id,kind,ref));
    """)
    cols = [r[1] for r in c.execute("PRAGMA table_info(reviews)")]
    if "ext_id" not in cols: c.execute("ALTER TABLE reviews ADD COLUMN ext_id TEXT")
    if "score" not in cols: c.execute("ALTER TABLE reviews ADD COLUMN score REAL")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ext ON reviews(product_id, ext_id)")
    c.execute("DELETE FROM connectors")
    demo = [r[0] for r in c.execute("SELECT id FROM products WHERE demo=1")]
    for t in ("reviews", "events", "price_history"): c.execute(f"DELETE FROM {t} WHERE product_id IN (SELECT id FROM products WHERE demo=1)")
    c.execute("DELETE FROM products WHERE demo=1"); c.execute("DELETE FROM memories WHERE source LIKE 'seeded%'"); c.execute("DELETE FROM sessions WHERE owner='product@mira.demo'")
    c.commit(); c.close()

def hpw(pw, salt): return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 200_000).hex()

def ingest(c, pid, recs, source_label):
    p = c.execute("SELECT name,platform FROM products WHERE id=?", (pid,)).fetchone(); added = 0
    for r in recs:
        if not (r["body"] or r["title"]): continue
        theme, sent, score = cx.analyze(r["title"], r["body"], r["rating"])
        try:
            cur = c.execute("""INSERT INTO reviews(product_id,channel,source,rating,title,body,theme,sentiment,review_date,demo,created_at,ext_id,score)
                VALUES(?,?,?,?,?,?,?,?,?,0,?,?,?)""", (pid, r["channel"], source_label, r["rating"] or 0, r["title"], r["body"][:2000],
                theme, sent, r["date"], NOW().isoformat(), r["ext_id"], score))
        except Exception: continue  # duplicate ext_id
        added += 1
        app.broadcast({"type": "feedback", "id": cur.lastrowid, "product_id": pid, "product_name": p["name"], "platform": source_label,
                       "channel": r["channel"], "theme": theme, "sentiment": sent, "rating": r["rating"] or 0, "body": r["body"][:300],
                       "review_date": r["date"], "created_at": NOW().isoformat()})
    c.commit(); return added

def sync_source(sid):
    c = app.connect()
    try:
        s = c.execute("SELECT * FROM sources WHERE id=?", (sid,)).fetchone()
        try:
            recs = cx.ADAPTERS[s["kind"]](s["ref"]); added = ingest(c, s["product_id"], recs, s["kind"])
            if s["kind"] == "apple":
                first = {}
                for r in recs:
                    if r.get("version") and (r["version"] not in first or r["date"] < first[r["version"]]): first[r["version"]] = r["date"]
                for ver, dt in sorted(first.items(), key=lambda x: x[1])[1:]:
                    t = f"App version {ver} rolled out"
                    if not c.execute("SELECT 1 FROM events WHERE product_id=? AND title=?", (s["product_id"], t)).fetchone():
                        c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)", (s["product_id"], "product_change", t, "First seen in customer reviews (auto-detected from live App Store data).", dt + "T00:00:00"))
            c.execute("UPDATE sources SET status='ok',last_sync=?,detail=?,total=total+? WHERE id=?", (NOW().isoformat(), f"fetched {len(recs)}, {added} new", added, sid))
            c.commit()
            if added: app.retain_memory(f"Synced {s['kind']}:{s['ref']} -> {added} new feedback items.", context="live connector")
            return {"ok": True, "fetched": len(recs), "added": added}
        except Exception as e:
            c.execute("UPDATE sources SET status='error',last_sync=?,detail=? WHERE id=?", (NOW().isoformat(), str(e)[:200], sid)); c.commit()
            return {"ok": False, "error": str(e)}
    finally: c.close()

DEFAULTS = [("Amazon Shopping", "Amazon"), ("Flipkart Online Shopping", "Flipkart"), ("Myntra", "Myntra"), ("Nykaa", "Nykaa"),
            ("Meesho", "Meesho"), ("Croma", "Croma"), ("AJIO", "AJIO"), ("Blinkit", "Blinkit"), ("Zepto", "Zepto")]

def bootstrap():
    """First run: discover real e-commerce apps and start pulling their live customer feedback. No demo data."""
    c = app.connect()
    if c.execute("SELECT COUNT(*) FROM products").fetchone()[0]: c.close(); return
    for term, label in DEFAULTS:
        try: hits = cx.apple_find(term)
        except Exception as e: print("resolve failed", term, e); continue
        r = next((h for h in hits if label.lower() in h["trackName"].lower()), None)
        if not r: continue
        cur = c.execute("""INSERT INTO products(name,brand,model,category,image,source,source_url,price,currency,rating,review_count,availability,confidence,updated_at,demo,platform)
            VALUES(?,?,?,?,?,?,?,0,'INR',?,?,'Live',100,?,0,?)""", (label, label, "", "E-commerce", r.get("artworkUrl100", ""), "App Store", r.get("trackViewUrl", ""),
            r.get("averageUserRating") or 0, r.get("userRatingCount") or 0, NOW().isoformat(), label))
        pid = cur.lastrowid
        c.execute("INSERT OR IGNORE INTO sources(product_id,kind,ref) VALUES(?,?,?)", (pid, "apple", f"{r['trackId']}:in"))
        c.execute("INSERT OR IGNORE INTO sources(product_id,kind,ref) VALUES(?,?,?)", (pid, "hackernews", label + " app"))
        c.commit()
    c.close()

def poller():
    bootstrap()
    time.sleep(5)
    while True:
        c = app.connect(); ids = [r[0] for r in c.execute("SELECT id FROM sources")]; c.close()
        for i in ids: sync_source(i)
        time.sleep(SYNC_EVERY)

class H(app.Handler):
    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query); p = u.path
        if p in ("/api/impact", "/api/bursts", "/api/adapters", "/api/connectors", "/api/emerging"):
            c = app.connect()
            try:
                pid = int(q["product_id"][0]) if q.get("product_id") else None
                if p == "/api/impact": return self.send_json(cx.change_impact(c, pid))
                if p == "/api/bursts" or p == "/api/emerging": return self.send_json(cx.burst_themes(c, pid))
                if p == "/api/adapters": return self.send_json(cx.ADAPTER_HELP)
                rows = c.execute("""SELECT s.id, p.name||' · '||s.kind name, s.kind, s.status, s.last_sync, COALESCE(s.detail,'')||' ['||s.ref||'] total '||s.total detail
                                    FROM sources s JOIN products p ON p.id=s.product_id""").fetchall()
                return self.send_json([dict(r) for r in rows])
            finally: c.close()
        return super().do_GET()

    def do_POST(self):
        p = urlparse(self.path).path
        if p in ("/api/login", "/api/register", "/api/track", "/api/sources/sync", "/api/connectors/sync", "/api/changes", "/api/ingest"):
            d = self.body()
            c = app.connect()
            try:
                if p == "/api/register":
                    first = c.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
                    if not first and not app.get_session(self): return self.send_json({"error": "Only an admin can add users after the first account"}, 403)
                    if len(d.get("password", "")) < 8: return self.send_json({"error": "Password must be 8+ chars"}, 400)
                    salt = secrets.token_hex(8)
                    c.execute("INSERT INTO users VALUES(?,?,?,?,?)", (d["email"].lower().strip(), d.get("name") or d["email"], "admin" if first else "member", salt, hpw(d["password"], salt)))
                    c.commit(); p = "/api/login"
                if p == "/api/login":
                    r = c.execute("SELECT * FROM users WHERE email=?", (d.get("email", "").lower().strip(),)).fetchone()
                    if not r or not secrets.compare_digest(r["pw"], hpw(d.get("password", ""), r["salt"])): return self.send_json({"error": "invalid_credentials"}, 401)
                    t = app.make_token(); app.SESSIONS[t] = {"email": r["email"], "name": r["name"], "role": r["role"], "exp": time.time() + 86400 * 7}
                    return self.send_json({"token": t, "user": app.SESSIONS[t]})
                if not app.get_session(self) and p != "/api/ingest": return self.send_json({"error": "auth_required"}, 401)
                if p == "/api/track":  # create product + attach live sources
                    cur = c.execute("""INSERT INTO products(name,brand,model,category,image,source,source_url,price,currency,rating,review_count,availability,confidence,updated_at,demo,platform)
                        VALUES(?,?,?,?,?,?,?,0,'INR',0,0,'',100,?,0,?)""", (d["name"], d.get("brand", ""), "", d.get("category", "General"), "📦", "tracked", "", NOW().isoformat(), "multi-channel"))
                    pid = cur.lastrowid
                    for s in d.get("sources", []):
                        if s["kind"] in cx.ADAPTERS: c.execute("INSERT OR IGNORE INTO sources(product_id,kind,ref) VALUES(?,?,?)", (pid, s["kind"], s["ref"].strip()))
                    c.commit()
                    ids = [r[0] for r in c.execute("SELECT id FROM sources WHERE product_id=?", (pid,))]
                    threading.Thread(target=lambda: [sync_source(i) for i in ids], daemon=True).start()
                    return self.send_json({"ok": True, "product_id": pid})
                if p in ("/api/sources/sync", "/api/connectors/sync"):
                    return self.send_json(sync_source(int(d.get("connector_id") or d.get("id"))))
                if p == "/api/changes":  # log a product change (release, price change, fix...)
                    c.execute("INSERT INTO events(product_id,kind,title,detail,occurred_at) VALUES(?,?,?,?,?)",
                              (d["product_id"], "product_change", d["title"], d.get("detail", ""), (d.get("date") or NOW().date().isoformat()) + "T00:00:00"))
                    c.commit(); return self.send_json({"ok": True})
                if p == "/api/ingest":  # webhook: Zendesk/Intercom/Freshdesk/CSV->JSON. header X-Ingest-Key
                    if d.get("key") != os.getenv("INGEST_KEY") or not os.getenv("INGEST_KEY"): return self.send_json({"error": "bad key"}, 401)
                    recs = [{"ext_id": "hook:" + hashlib.sha1((r.get("id", "") + r.get("body", "")).encode()).hexdigest()[:16], "channel": r.get("channel", "Support Ticket"),
                             "rating": r.get("rating"), "title": r.get("title", ""), "body": r.get("body", ""), "date": r.get("date") or NOW().date().isoformat()} for r in d.get("records", [])]
                    return self.send_json({"ok": True, "added": ingest(c, int(d["product_id"]), recs, d.get("source", "webhook"))})
            except Exception as e: return self.send_json({"error": str(e)}, 400)
            finally: c.close()
        return super().do_POST()

def main():
    migrate(); threading.Thread(target=poller, daemon=True).start()
    port = int(os.getenv("PORT", "4173"))
    print(f"MIRA → http://127.0.0.1:{port}  (first account you register becomes admin)  hindsight={app.hindsight_configured()}")
    app.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()

if __name__ == "__main__": main()
