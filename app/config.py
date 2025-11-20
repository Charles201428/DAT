from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

    app_env: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    postgres_dsn: str = Field(default="sqlite+aiosqlite:///./treasury.db", alias="POSTGRES_DSN")

    slack_webhook_url: str | None = Field(default=None, alias="SLACK_WEBHOOK_URL")
    user_agent: str = Field(default="CryptoTreasuryBot/0.1 (you@example.com)", alias="USER_AGENT")

    edgar_rss_base: str = Field(
        default=(
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=100&owner=exclude"
        ),
        alias="EDGAR_RSS_BASE",
    )

    ingest_concurrency: int = Field(default=5, alias="INGEST_CONCURRENCY")
    schedule_cron_rss: str = Field(default="*/15 * * * *", alias="SCHEDULE_CRON_RSS")
    min_classifier_score: int = Field(default=30, alias="MIN_CLASSIFIER_SCORE")

    # CryptoPanic
    cryptopanic_base: str = Field(default="https://cryptopanic.com/api/growth/v2/posts/", alias="CRYPTOPANIC_BASE")
    cryptopanic_token: str | None = Field(default=None, alias="CRYPTOPANIC_TOKEN")
    cryptopanic_public: bool = Field(default=True, alias="CRYPTOPANIC_PUBLIC")
    cryptopanic_filter: str | None = Field(default=None, alias="CRYPTOPANIC_FILTER")
    cryptopanic_currencies: str | None = Field(default=None, alias="CRYPTOPANIC_CURRENCIES")
    cryptopanic_kind: str = Field(default="all", alias="CRYPTOPANIC_KIND")  # news|media|all
    cryptopanic_size: int = Field(default=50, alias="CRYPTOPANIC_SIZE")  # 1..500 depending on plan
    cryptopanic_pages: int = Field(default=1, alias="CRYPTOPANIC_PAGES")  # how many pages to pull per ingest
    # Local content filter keyword (since Developer plan lacks API search)
    cryptopanic_require_keyword: str | None = Field(default=None, alias="CRYPTOPANIC_REQUIRE_KEYWORD")

    # OpenAI
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_classify_workers: int = Field(default=10, alias="OPENAI_CLASSIFY_WORKERS")  # Parallel workers for classification

    # Local storage for raw news texts
    news_text_dir: str = Field(default="news_text", alias="NEWS_TEXT_DIR")
    positive_text_dir: str = Field(default="positive_DAT", alias="POSITIVE_TEXT_DIR")

    # Scheduler toggle (disabled by default for manual runs)
    enable_scheduler: bool = Field(default=False, alias="ENABLE_SCHEDULER")

    # Alpha Vantage
    alphavantage_api_key: str | None = Field(default=None, alias="ALPHAVANTAGE_API_KEY")
    alphavantage_base: str = Field(default="https://www.alphavantage.co/query", alias="ALPHAVANTAGE_BASE")

    # CoinGecko
    coingecko_api_key: str | None = Field(default=None, alias="CG_API_KEY")


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


