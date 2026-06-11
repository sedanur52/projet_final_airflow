import json
from typing import Any

from fx_pipeline.storage import get_postgres_hook


def store_raw_payload(extracted_payload: dict[str, Any]) -> dict[str, Any]:
    payload = extracted_payload["payload"]
    hook = get_postgres_hook()
    hook.run(
        """
        INSERT INTO fx_raw_ingestion (
            run_id,
            base_currency,
            symbols_requested,
            api_date,
            payload_json
        )
        VALUES (%s, %s, %s, %s, %s::jsonb)
        """,
        parameters=(
            extracted_payload["run_id"],
            extracted_payload["base_currency"],
            ",".join(extracted_payload["target_currencies"]),
            payload["date"],
            json.dumps(payload),
        ),
    )
    return extracted_payload
