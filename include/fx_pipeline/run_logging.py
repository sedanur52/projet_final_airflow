from typing import Any

from fx_pipeline.config import PIPELINE_NAME
from fx_pipeline.storage import get_postgres_hook


def log_fx_run(run_result: dict[str, Any]) -> None:
    hook = get_postgres_hook()
    hook.run(
        """
        INSERT INTO fx_run_logs (
            run_id,
            pipeline_name,
            status,
            rows_received,
            rows_valid,
            rows_rejected,
            rows_inserted,
            alert_count,
            message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            run_result["run_id"],
            PIPELINE_NAME,
            run_result["status"],
            run_result["rows_received"],
            run_result["rows_valid"],
            run_result["rows_rejected"],
            run_result["rows_inserted"],
            run_result.get("alert_count", 0),
            run_result["message"],
        ),
    )
