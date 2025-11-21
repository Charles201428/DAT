# Crypto Treasury Parser

FastAPI-based service that discovers, parses, and structures Digital Asset Treasury (DAT) events from CryptoPanic news feeds. The service uses GPT for classification and extraction, enriches data with stock and token prices from Alpha Vantage and CoinGecko, and exports structured data to CSV.

## Features

- **News Ingestion**: Automated fetching from CryptoPanic API (Growth plan) with keyword filtering
- **GPT Classification**: Binary classification of DAT events using OpenAI GPT-4o-mini
- **Structured Extraction**: GPT-based extraction of company, token, and transaction details
- **Price Enrichment**: 
  - Stock prices and performance metrics via Alpha Vantage API
  - Token prices and performance metrics via CoinGecko Pro API
- **Performance Metrics**: Calculates forward-looking (1D, 7D, 30D) and backward-looking (-7D) performance metrics
- **Deduplication**: Utility to deduplicate files based on stock ticker, token, and announcement date
- **CSV Export**: Combine JSON files into CSV with URL tracking and filtering

## Quickstart

### Prerequisites

- Python 3.11+ (recommended, Python 3.13 may have compatibility issues)
- Conda (optional, for virtual environment management)
- API Keys:
  - CryptoPanic API key (Growth plan recommended)
  - OpenAI API key (for GPT classification and extraction)
  - Alpha Vantage API key (for stock price data)
  - CoinGecko API key (Pro plan recommended for historical data)

### Setup

1. **Create and activate a virtual environment** (using Conda recommended)
```bash
conda create -n treasury python=3.11
conda activate treasury
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment**
```bash
copy .env.example .env
# Edit .env with your API keys and settings
```

Required environment variables:
- `CRYPTOPANIC_TOKEN`: Your CryptoPanic API token
- `OPENAI_API_KEY`: Your OpenAI API key
- `ALPHAVANTAGE_API_KEY`: Your Alpha Vantage API key
- `COINGECKO_API_KEY`: Your CoinGecko API key (Pro keys start with `CG-`)

4. **Run API**
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

## API Endpoints

### Ingestion

- `POST /events/ingest?hours=24` - Trigger CryptoPanic ingestion for the last N hours (max 720 hours / 30 days)
- `GET /events` - List all events
- `DELETE /events` - Clear all events and raw documents

### News Documents

- `GET /news` - List all stored raw news documents
- `GET /news/{id}` - Get full content of a specific news document
- `POST /news/fetch-original` - Download original article text for a folder of files

### Analysis & Classification

- `POST /analyze/classify-local?dir=path&limit=100` - Classify local `.txt` files as DAT events
- `POST /analyze/format-local?dir=path&limit=100` - Extract structured data from local `.txt` files into JSON
- `POST /analyze/enrich-full?dir=path` - Enrich JSON files with both stock and token prices
- `POST /analyze/enrich-stock-av?dir=path` - Enrich with Alpha Vantage stock prices
- `POST /analyze/enrich-token-cg?dir=path` - Enrich with CoinGecko token prices
- `POST /analyze/dedup?dir=path&keep=largest` - Deduplicate JSON files
- `POST /analyze/json-to-csv?dir=path&exclude_no_token=true` - Export JSON files to CSV

## Workflow

### 1. Ingest News

```powershell
# Fetch last 24 hours of news
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/events/ingest?hours=24"
```

News articles are saved to `news_text/YYYYMMDD_HHMMSSZ/` folders.

### 2. Classify DAT Events

```powershell
# Classify local .txt files
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/analyze/classify-local?dir=news_text/20251120_190823Z&limit=100"
```

Positive classifications are exported to `positive_DAT/YYYYMMDD_HHMMSSZ/` folders.

### 3. Extract Structured Data

```powershell
# Format .orig.txt files into JSON
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/analyze/format-local?dir=positive_DAT/20251120_190823Z&limit=100"
```

### 4. Enrich with Price Data

```powershell
# Enrich with both stock and token prices
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/analyze/enrich-full?dir=positive_DAT/20251120_190823Z"
```

### 5. Deduplicate

```powershell
# Remove duplicates, keeping the largest file
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/analyze/dedup?dir=positive_DAT/20251120_190823Z&keep=largest&remove_duplicates=true"
```

### 6. Export to CSV

```powershell
# Export to CSV (excludes entries with Token=N/A)
Invoke-RestMethod -Method POST -Uri "http://localhost:8000/analyze/json-to-csv?dir=positive_DAT/20251120_190823Z&exclude_no_token=true"
```

## Performance Metrics

The enrichment calculates the following performance metrics:

- **Forward-looking (after announcement)**:
  - `1D Stock/Token Perf`: Performance from announcement date (D) to D+1
  - `7D Stock/Token Perf`: Performance from D to D+7
  - `30D Stock Perf`: Performance from D to D+30

- **Backward-looking (before announcement)**:
  - `-7D Stock/Token Perf`: Performance from D-7 to D
  - `-7 to -1D Stock/Token Perf`: Performance from D-7 to D-1 (excluding announcement day)

- **Day-of-announcement**:
  - `D Stock/Token Perf`: Performance from D-1 to D

All metrics are anchored to the "Raise Ann. Date" field. If no announcement date is provided, it defaults to yesterday for token enrichment.

## Data Structure

### JSON Schema

Each extracted event includes:
- Company information (Stock Ticker, Stock Name, Exchange, Country)
- Token information (Token symbol, prices, performance metrics)
- Transaction details (Raise Amount, Type of Raise, Announcement Date)
- Performance metrics (Stock and Token performance over various timeframes)

### CSV Export

The CSV export includes:
- URL column (first column) - extracted from corresponding `.txt` files
- All JSON fields as columns
- Filtered to exclude entries where Token is "N/A" (by default)

## Configuration

Key settings in `.env`:

```env
# CryptoPanic
CRYPTOPANIC_TOKEN=your_token_here
CRYPTOPANIC_BASE=https://cryptopanic.com/api/growth/v2
CRYPTOPANIC_REQUIRE_KEYWORD=true  # Filter by keyword locally

# OpenAI
OPENAI_API_KEY=sk-proj-...
OPENAI_CLASSIFY_WORKERS=5  # Parallel workers for classification

# Alpha Vantage
ALPHAVANTAGE_API_KEY=your_key_here

# CoinGecko (Pro keys start with CG-)
COINGECKO_API_KEY=CG-your_pro_key_here

# Database (SQLite for local dev)
POSTGRES_DSN=sqlite+aiosqlite:///./treasury.db
```

## Architecture

- **Ingestion**: `app/ingest/cryptopanic.py` - Fetches news from CryptoPanic API
- **Classification**: `app/analyze/gpt.py` - GPT-based binary classification
- **Extraction**: `app/analyze/gpt.py` - GPT-based structured extraction
- **Enrichment**: 
  - `app/enrich/alpha.py` - Alpha Vantage stock price enrichment
  - `app/enrich/coingecko.py` - CoinGecko token price enrichment
- **Utilities**: `app/utils/dedupe.py` - Deduplication logic
- **API**: FastAPI routers in `app/routers/`

## Notes

- **Database**: Uses SQLite (`treasury.db`) by default for local development. Can be configured to use Postgres.
- **File Storage**: Raw news articles are saved to `news_text/` folders. Classified positives are saved to `positive_DAT/` folders.
- **Deduplication**: Files are grouped by Stock Ticker, Token, and Raise Ann. Date. Duplicates are moved to `_dedup_trash/` folder.
- **Token Extraction**: The classification prompt requires a specific token to be identifiable. Articles mentioning "digital assets" without specifying a token will be filtered out.

## License

[Add your license here]
