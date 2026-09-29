"""MIRA connectors + analysis. Every connector fetches LIVE data; nothing is generated.
Stdlib only. Adapters return: {ext_id, channel, rating, title, body, date, url}."""
import json, re, os, math, hashlib, urllib.request, urllib.parse, datetime, html
from statistics import mean, pstdev

UA = "Mozilla/5.0 (compatible; MIRA-FeedbackBot/1.0; +respects-robots)"

def _get(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def _iso(s):
    try: return datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00")).date().isoformat()
    except Exception: return datetime.date.today().isoformat()

# ---------------- adapters ----------------
def apple_reviews(ref):
    """ref = 'APPID' or 'APPID:country' (official public iTunes RSS, newest first)."""
    app_id, _, cc = ref.partition(":"); cc = cc or "in"
    d = json.loads(_get(f"https://itunes.apple.com/{cc}/rss/customerreviews/id={app_id}/sortby=mostrecent/json"))
    out = []
    for e in (d.get("feed", {}).get("entry") or []):
        if "im:rating" not in e: continue
        out.append({"ext_id": "apple:" + e["id"]["label"], "channel": "App Store Review",
                    "rating": int(e["im:rating"]["label"]), "title": e["title"]["label"],
                    "body": e["content"]["label"], "date": _iso(e["updated"]["label"]), "url": ""})
    return out

def apple_releases(ref):
    """Real release history -> product-change events."""
    app_id, _, cc = ref.partition(":"); cc = cc or "in"
    d = json.loads(_get(f"https://itunes.apple.com/lookup?id={app_id}&country={cc}"))
    r = (d.get("results") or [{}])[0]
    if not r: return []
    return [{"title": f"v{r.get('version')} released", "detail": (r.get("releaseNotes") or "")[:600],
             "date": _iso(r.get("currentVersionReleaseDate"))}]

def reddit(ref):
    q = urllib.parse.quote(ref)
    d = json.loads(_get(f"https://www.reddit.com/search.json?q={q}&sort=new&limit=50&t=month"))
    return [{"ext_id": "reddit:" + c["data"]["id"], "channel": "Social Mention", "rating": None,
             "title": c["data"]["title"], "body": c["data"].get("selftext", "")[:1500],
             "date": datetime.datetime.utcfromtimestamp(c["data"]["created_utc"]).date().isoformat(),
             "url": "https://reddit.com" + c["data"]["permalink"]} for c in d["data"]["children"]]

def hackernews(ref):
    q = urllib.parse.quote(ref)
    d = json.loads(_get(f"https://hn.algolia.com/api/v1/search_by_date?query={q}&tags=comment&hitsPerPage=50"))
    return [{"ext_id": "hn:" + h["objectID"], "channel": "Social Mention", "rating": None, "title": h.get("story_title") or "",
             "body": html.unescape(re.sub("<[^>]+>", " ", h.get("comment_text") or "")), "date": _iso(h["created_at"]),
             "url": f"https://news.ycombinator.com/item?id={h['objectID']}"} for h in d["hits"] if h.get("comment_text")]

def jsonld_page(ref):
    """Any product page exposing schema.org Review markup (many Shopify/Woo/D2C stores do)."""
    page = _get(ref); out = []
    def walk(n):
        if isinstance(n, list): [walk(x) for x in n]
        elif isinstance(n, dict):
            if n.get("@type") == "Review":
                body = n.get("reviewBody") or n.get("description") or ""
                rt = (n.get("reviewRating") or {}).get("ratingValue")
                out.append({"ext_id": "ld:" + hashlib.sha1((ref + body).encode()).hexdigest()[:16], "channel": "Marketplace Review",
                            "rating": int(float(rt)) if rt else None, "title": n.get("name", ""), "body": body,
                            "date": _iso(n.get("datePublished")), "url": ref})
            for v in n.values(): walk(v)
    for m in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', page, re.S):
        try: walk(json.loads(m))
        except Exception: pass
    return out

def rainforest_amazon(ref):
    """Amazon reviews via licensed provider (Amazon offers no public review API). ref='ASIN[:domain]', needs RAINFOREST_API_KEY."""
    key = os.getenv("RAINFOREST_API_KEY")
    if not key: raise RuntimeError("Set RAINFOREST_API_KEY (Amazon has no public reviews API)")
    asin, _, dom = ref.partition(":")
    d = json.loads(_get(f"https://api.rainforestapi.com/request?api_key={key}&type=reviews&amazon_domain={dom or 'amazon.in'}&asin={asin}&sort_by=most_recent"))
    return [{"ext_id": "amz:" + r["id"], "channel": "Marketplace Review", "rating": int(r.get("rating") or 0) or None,
             "title": r.get("title", ""), "body": r.get("body", ""), "date": _iso((r.get("date") or {}).get("utc")),
             "url": r.get("link", "")} for r in d.get("reviews", [])]

ADAPTERS = {"apple": apple_reviews, "reddit": reddit, "hackernews": hackernews,
            "jsonld": jsonld_page, "amazon": rainforest_amazon}
ADAPTER_HELP = {"apple": "App Store app id (e.g. 310633997 or 310633997:in)", "reddit": "Search query (brand/model)",
                "hackernews": "Search query", "jsonld": "Product page URL with schema.org reviews",
                "amazon": "ASIN[:amazon.in] (needs RAINFOREST_API_KEY)"}

# ---------------- analyzer (aspect-based, negation aware) ----------------
ASPECTS = {
 "battery life": "battery charge charging drain backlife standby", "sound quality": "sound audio bass treble volume speaker",
 "noise cancellation": "anc noise cancellation cancelling", "microphone / calls": "mic microphone call calls",
 "connectivity": "bluetooth pairing connect connection disconnect wifi", "app experience": "app application login crash crashes ui interface",
 "performance": "slow lag laggy fast speed performance freeze freezes", "build quality": "build plastic quality broke broken cheap flimsy sturdy premium",
 "comfort / fit": "comfort comfortable fit sizing size tight loose ear", "display": "display screen brightness resolution",
 "camera": "camera photo photos video", "heating": "heat heating hot overheat warm", "price / value": "price expensive cheap value money worth cost",
 "delivery": "delivery delivered shipping packaging arrived late", "customer support": "support service refund return replacement warranty agent",
 "software update": "update firmware patch version upgrade", "reliability": "stopped defective faulty died dead issue issues problem",
}
POS = set("good great excellent amazing love loved best perfect awesome fantastic superb smooth impressed happy nice solid fast reliable improved better fixed comfortable premium worth recommend".split())
NEG = set("bad poor terrible worst awful hate hated disappointing disappointed slow broke broken useless waste garbage defective faulty drain drains crash crashes laggy cheap flimsy overheat overheating refund return problem issue issues worse fails failed stopped dead".split())
NEGATORS = {"not", "no", "never", "cannot", "cant", "can't", "dont", "don't", "isn't", "wasn't", "doesn't", "didn't", "without", "hardly"}

def _tok(t): return re.findall(r"[a-z']+", t.lower())

def analyze(title, body, rating=None):
    toks = _tok(f"{title}. {body}"); score = 0; hits = 0
    for i, w in enumerate(toks):
        v = 1 if w in POS else -1 if w in NEG else 0
        if v:
            if any(t in NEGATORS for t in toks[max(0, i - 3):i]): v = -v
            score += v; hits += 1
    lex = max(-1, min(1, score / max(2, hits + 1))) if hits else 0.0
    if rating: score_f = 0.6 * ((rating - 3) / 2) + 0.4 * lex
    else: score_f = lex
    sent = "positive" if score_f > 0.2 else "negative" if score_f < -0.2 else "mixed"
    tset = set(toks); best, bn = "general", 0
    for a, kw in ASPECTS.items():
        n = len(tset.intersection(kw.split()))
        if n > bn: best, bn = a, n
    return best, sent, round(score_f, 3)

# ---------------- change-impact engine ----------------
def _welch_p(a, b):
    if len(a) < 3 or len(b) < 3: return None
    va, vb = pstdev(a) ** 2 / len(a), pstdev(b) ** 2 / len(b)
    se = math.sqrt(va + vb) or 1e-9; z = abs(mean(b) - mean(a)) / se
    return round(math.erfc(z / math.sqrt(2)), 4)

def change_impact(c, product_id=None, window=21):
    """Before/after comparison around every logged product change, incl. aspect-specific effect."""
    q = "SELECT e.*, p.name pname FROM events e JOIN products p ON p.id=e.product_id WHERE e.kind='product_change'"
    rows = c.execute(q + (" AND e.product_id=?" if product_id else "") + " ORDER BY occurred_at DESC", (product_id,) if product_id else ()).fetchall()
    out = []
    for e in rows:
        d0 = datetime.date.fromisoformat(e["occurred_at"][:10]); lo = (d0 - datetime.timedelta(days=window)).isoformat()
        hi = (d0 + datetime.timedelta(days=window)).isoformat(); mid = d0.isoformat()
        rv = c.execute("SELECT review_date d, score, theme FROM reviews WHERE product_id=? AND review_date BETWEEN ? AND ? AND score IS NOT NULL", (e["product_id"], lo, hi)).fetchall()
        b = [r["score"] for r in rv if r["d"] < mid]; a = [r["score"] for r in rv if r["d"] >= mid]
        tt = set(_tok(e["title"] + " " + (e["detail"] or "")))
        asps = {a for a, kw in ASPECTS.items() if tt.intersection(kw.split())} - {"software update"}
        asp = ", ".join(sorted(asps)) or "general"
        ba = [r["score"] for r in rv if r["d"] < mid and r["theme"] in asps]; aa = [r["score"] for r in rv if r["d"] >= mid and r["theme"] in asps]
        out.append({"event_id": e["id"], "product": e["pname"], "change": e["title"], "date": mid, "target_aspect": asp,
                    "n_before": len(b), "n_after": len(a),
                    "sentiment_delta": round(mean(a) - mean(b), 3) if a and b else None, "p_value": _welch_p(b, a),
                    "aspect_delta": round(mean(aa) - mean(ba), 3) if aa and ba else None, "aspect_n": [len(ba), len(aa)],
                    "verdict": ("insufficient data yet" if len(a) < 5 or len(b) < 5 else
                                "likely improved" if mean(a) - mean(b) > 0.1 else "likely regressed" if mean(a) - mean(b) < -0.1 else "no clear effect")})
    return out

def burst_themes(c, product_id=None, recent=14, base=56):
    """Emerging = aspect whose recent share is a z-score outlier vs its trailing baseline (share of weekly windows)."""
    today = datetime.date.today()
    def cnt(d0, d1):
        sql = "SELECT theme, SUM(sentiment='negative') neg, COUNT(*) n FROM reviews WHERE review_date>=? AND review_date<?"
        p = [d0.isoformat(), d1.isoformat()]
        if product_id: sql += " AND product_id=?"; p.append(product_id)
        return {r["theme"]: (r["n"], r["neg"]) for r in c.execute(sql + " GROUP BY theme", p)}
    rec = cnt(today - datetime.timedelta(days=recent), today + datetime.timedelta(days=1))
    weeks = [cnt(today - datetime.timedelta(days=recent + 7 * (i + 1)), today - datetime.timedelta(days=recent + 7 * i)) for i in range(base // 7)]
    tr = sum(v[0] for v in rec.values()) or 1; out = []
    for th, (n, neg) in rec.items():
        sh = [w.get(th, (0, 0))[0] / (sum(x[0] for x in w.values()) or 1) for w in weeks if w]
        if len(sh) < 2 or n < 3: 
            if n >= 5 and not any(w.get(th) for w in weeks): out.append({"theme": th, "recent": n, "z": None, "neg_share": round(neg / n, 2), "signal": "new"})
            continue
        mu, sd = mean(sh), max(pstdev(sh), 0.02); z = (n / tr - mu) / sd
        if z > 1.5: out.append({"theme": th, "recent": n, "z": round(z, 2), "neg_share": round(neg / n, 2), "signal": "spike"})
    return sorted(out, key=lambda x: -(x["z"] or 9))


def apple_find(term, cc="in"):
    d = json.loads(_get(f"https://itunes.apple.com/search?term={urllib.parse.quote(term)}&country={cc}&entity=software&limit=3"))
    return d.get("results") or []

def apple_reviews_deep(ref):
    app_id, _, cc = ref.partition(":"); cc = cc or "in"; out = []
    for pg in range(1, 6):
        try: d = json.loads(_get(f"https://itunes.apple.com/{cc}/rss/customerreviews/page={pg}/id={app_id}/sortby=mostrecent/json"))
        except Exception:
            if pg == 1: raise
            break
        es = d.get("feed", {}).get("entry") or []
        es = [es] if isinstance(es, dict) else es; n = 0
        for e in es:
            if "im:rating" not in e: continue
            n += 1
            out.append({"ext_id": "apple:" + e["id"]["label"], "channel": "App Store Review", "rating": int(e["im:rating"]["label"]),
                        "title": e["title"]["label"], "body": e["content"]["label"], "date": _iso(e["updated"]["label"]), "url": "",
                        "version": (e.get("im:version") or {}).get("label")})
        if not n: break
    return out
ADAPTERS["apple"] = apple_reviews_deep
