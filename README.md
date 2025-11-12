## Crypto Treasury Parser (MVP)

FastAPI-based service that discovers, parses, and structures Digital Asset Treasury (DAT) events from authoritative sources (EDGAR, company IR, exchange notices), stores normalized events in Postgres, and emits alerts/exports.

### Quickstart

1. Create and activate a virtual environment
```bash
python -m venv .venv && .venv\\Scripts\\activate
```
2. Install dependencies
```bash
pip install -r requirements.txt
```
3. Configure environment
```bash
copy .env.example .env
# edit .env with your Postgres DSN and settings
```
4. Run API
```bash
uvicorn app.main:app --reload
```

### Components
- Ingestion: RSS (EDGAR + IR + exchanges), scheduled with APScheduler
- Parsing: heuristic classifier, regex extractors, optional LLM fact card
- Storage: Postgres via SQLAlchemy
- API: FastAPI (`/events`, `/companies/{ticker}/events`)
- Alerts: Slack webhook for new high-confidence events; daily CSV export

### MVP Targets
- ≥90% precision on DAT relevance
- ≤2h median latency from publish → event
- Dedupe within 48h and auditable trace (raw doc + extracted JSON)


