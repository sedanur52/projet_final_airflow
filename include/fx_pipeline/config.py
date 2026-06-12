import os


PIPELINE_NAME = "fx_rates_pipeline"
FRANKFURTER_BASE_URL = os.getenv("FRANKFURTER_BASE_URL", "https://api.frankfurter.app")
FRANKFURTER_BASE_CURRENCY = os.getenv("FRANKFURTER_BASE_CURRENCY", "EUR")
FRANKFURTER_TARGET_CURRENCIES = os.getenv(
    "FRANKFURTER_TARGET_CURRENCIES",
    "USD,GBP,JPY,CHF,CAD",
)

FX_ALERT_THRESHOLD = float(os.getenv("FX_ALERT_THRESHOLD", "0.05"))
FX_FRESHNESS_THRESHOLD_DAYS = int(os.getenv("FX_FRESHNESS_THRESHOLD_DAYS", "2"))
FX_FORCE_INVALID_RATE_FOR = os.getenv("FX_FORCE_INVALID_RATE_FOR", "").strip().upper()
FX_FORCE_INVALID_CODE_FOR = os.getenv("FX_FORCE_INVALID_CODE_FOR", "").strip().upper()

TASK_RETRIES = int(os.getenv("TASK_RETRIES", "2"))
TASK_RETRY_DELAY_SECONDS = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "30"))
TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT_SECONDS", "120"))

AIRFLOW_POSTGRES_CONN_ID = os.getenv("AIRFLOW_POSTGRES_CONN_ID", "postgres_fx")


def get_target_currencies() -> list[str]:
    return [
        currency.strip()
        for currency in FRANKFURTER_TARGET_CURRENCIES.split(",")
        if currency.strip()
    ]
