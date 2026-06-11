from airflow.providers.postgres.hooks.postgres import PostgresHook

from fx_pipeline.config import AIRFLOW_POSTGRES_CONN_ID


def get_postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=AIRFLOW_POSTGRES_CONN_ID)
