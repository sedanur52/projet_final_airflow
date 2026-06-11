import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from fx_pipeline.alerts import detect_fx_alerts
from fx_pipeline.config import (
    FRANKFURTER_BASE_CURRENCY,
    PIPELINE_NAME,
    TASK_RETRIES,
    TASK_RETRY_DELAY_SECONDS,
    TASK_TIMEOUT_SECONDS,
    get_target_currencies,
)
from fx_pipeline.extract import extract_fx_rates
from fx_pipeline.load import load_fx_rates, load_fx_rejections
from fx_pipeline.quality import evaluate_fx_quality
from fx_pipeline.raw_store import store_raw_payload
from fx_pipeline.run_logging import log_fx_run
from fx_pipeline.runtime import get_runtime_params
from fx_pipeline.transform import transform_fx_rates


LOGGER = logging.getLogger(__name__)
DEFAULT_TASK_OPTIONS = {
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(seconds=TASK_RETRY_DELAY_SECONDS),
    "execution_timeout": timedelta(seconds=TASK_TIMEOUT_SECONDS),
}


@dag(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    params={
        "base_currency": FRANKFURTER_BASE_CURRENCY,
        "target_currencies": get_target_currencies(),
    },
    tags=["fx", "frankfurter", "postgresql", "final-project"],
)
def fx_rates_pipeline():
    @task(task_id="extract_fx_rates", **DEFAULT_TASK_OPTIONS)
    def extract_fx_rates_task() -> dict:
        LOGGER.info("Debut de l'extraction des taux de change")
        return extract_fx_rates(get_runtime_params())

    @task(task_id="store_raw_response", **DEFAULT_TASK_OPTIONS)
    def store_raw_response_task(extracted_payload: dict) -> dict:
        LOGGER.info("Stockage brut de la reponse API")
        return store_raw_payload(extracted_payload)

    @task(task_id="transform_fx_rates", **DEFAULT_TASK_OPTIONS)
    def transform_fx_rates_task(raw_payload: dict) -> dict:
        LOGGER.info("Transformation des taux de change")
        return transform_fx_rates(raw_payload)

    @task(task_id="quality_check_fx_rates", **DEFAULT_TASK_OPTIONS)
    def quality_check_fx_rates_task(transformed_payload: dict) -> dict:
        LOGGER.info("Controle qualite des taux de change")
        return evaluate_fx_quality(transformed_payload)

    @task.branch(task_id="branch_on_quality", **DEFAULT_TASK_OPTIONS)
    def branch_on_quality_task(quality_payload: dict) -> str:
        if quality_payload["rows_rejected_count"] > 0:
            LOGGER.warning("Des lignes invalides ont ete detectees, branchement vers les rejets")
            return "load_fx_rejections"
        LOGGER.info("Toutes les lignes sont valides, branchement nominal")
        return "load_fx_rates"

    @task(task_id="load_fx_rates", **DEFAULT_TASK_OPTIONS)
    def load_fx_rates_task(quality_payload: dict) -> dict:
        LOGGER.info("Chargement des taux de change valides")
        return load_fx_rates(quality_payload)

    @task(task_id="load_fx_rejections", **DEFAULT_TASK_OPTIONS)
    def load_fx_rejections_task(quality_payload: dict) -> dict:
        LOGGER.warning("Chargement des lignes rejetees dans fx_rejections")
        return load_fx_rejections(quality_payload)

    @task(task_id="fail_on_quality_rejections", **DEFAULT_TASK_OPTIONS)
    def fail_on_quality_rejections_task(rejection_result: dict) -> None:
        raise AirflowFailException(
            f"{rejection_result['rows_rejected']} lignes rejetees par le controle qualite"
        )

    @task(task_id="detect_fx_alerts", **DEFAULT_TASK_OPTIONS)
    def detect_fx_alerts_task(run_result: dict) -> dict:
        LOGGER.info("Detection des alertes sur les taux")
        return detect_fx_alerts(run_result)

    @task(task_id="log_fx_success_run", **DEFAULT_TASK_OPTIONS)
    def log_fx_success_run_task(run_result: dict) -> None:
        LOGGER.info("Journalisation de l'execution nominale")
        log_fx_run(run_result)

    @task(task_id="log_fx_rejection_run", **DEFAULT_TASK_OPTIONS)
    def log_fx_rejection_run_task(run_result: dict) -> None:
        LOGGER.info("Journalisation de l'execution avec rejets")
        LOGGER.info("Journalisation de l'execution")
        log_fx_run(run_result)

    extracted_payload = extract_fx_rates_task()
    raw_payload = store_raw_response_task(extracted_payload)
    transformed_payload = transform_fx_rates_task(raw_payload)
    quality_payload = quality_check_fx_rates_task(transformed_payload)
    branch_result = branch_on_quality_task(quality_payload)

    load_result = load_fx_rates_task(quality_payload)
    rejection_result = load_fx_rejections_task(quality_payload)
    branch_result >> [load_result, rejection_result]

    alert_result = detect_fx_alerts_task(load_result)
    log_success = log_fx_success_run_task(alert_result)

    fail_quality = fail_on_quality_rejections_task(rejection_result)
    log_rejections = log_fx_rejection_run_task(rejection_result)

    alert_result >> log_success
    rejection_result >> [log_rejections, fail_quality]


fx_rates_pipeline()
