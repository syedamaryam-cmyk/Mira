# MIRA — real-data feedback synthesis for e-commerce product teams

No demo data. `python server.py` → http://127.0.0.1:4173 → **Create account** (first user = admin) → Sources → track a product.

## Live sources
| Adapter | Data | Notes |
|---|---|---|
| apple | App Store reviews + release notes (auto-logged as product changes) | official public iTunes RSS/lookup |
| reddit / hackernews | social mentions by brand/model query | public JSON endpoints; Reddit may rate-limit |
| jsonld | any product page exposing schema.org Review markup | check the site's ToS/robots.txt |
| amazon | Amazon.in/.com reviews | via Rainforest API (`RAINFOREST_API_KEY`); Amazon has no public review API |
| webhook `POST /api/ingest` | Zendesk / Intercom / Freshdesk / CSV→JSON / surveys | set `INGEST_KEY` |

Flipkart, Croma, Myntra, Nykaa expose no public review API and forbid scraping: use a licensed provider (add an adapter in `connectors.py::ADAPTERS`), JSON-LD if the page has it, or the webhook/CSV route.

## Synthesis
- Aspect-based sentiment (negation-aware lexicon blended with star rating), dedupe by source id
- Emerging themes: z-score burst of an aspect's share vs its 8-week baseline
- Change linkage: logged/auto-detected changes → 21-day before/after sentiment delta, Welch p-value, aspect-specific effect, verdict
- Hindsight: every sync and Ask uses retain/recall/reflect (set `HINDSIGHT_API_URL/API_KEY/BANK_ID`)
- Polling every `SYNC_INTERVAL_SEC` (300) + SSE push to the Live Feed
