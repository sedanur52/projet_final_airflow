from typing import Any

from airflow.operators.python import get_current_context

from fx_pipeline.config import FRANKFURTER_BASE_CURRENCY, get_target_currencies


def get_runtime_params() -> dict[str, Any]:
    context = get_current_context()
    params = context["params"]
    return {
        "base_currency": params.get("base_currency", FRANKFURTER_BASE_CURRENCY),
        "target_currencies": params.get("target_currencies", get_target_currencies()),
        "run_id": context["run_id"],
        "ts_nodash": context["ts_nodash"],
    }
