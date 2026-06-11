import logging
from typing import Any

import requests

from fx_pipeline.config import FRANKFURTER_BASE_URL


LOGGER = logging.getLogger(__name__)


def extract_fx_rates(runtime_params: dict[str, Any]) -> dict[str, Any]:
    base_currency = runtime_params["base_currency"]
    target_currencies = runtime_params["target_currencies"]

    request_params = {
        "from": base_currency,
        "to": ",".join(target_currencies),
    }

    LOGGER.info(
        "Extraction Frankfurter pour %s vers %s",
        base_currency,
        ", ".join(target_currencies),
    )
    
    try:
        response = requests.get(f"{FRANKFURTER_BASE_URL}/latest",
            params=request_params,
            timeout=30,
        )
    except requests.RequestException as exc:
        LOGGER.exception(
            "Echec de l'appel Frankfurter pour %s vers %s",
            base_currency,
            ", ".join(target_currencies),
        )
        raise
    
    response.raise_for_status()
    payload = response.json()

    return {
        "run_id": runtime_params["run_id"],
        "base_currency": base_currency,
        "target_currencies": target_currencies,
        "payload": payload,
    }
