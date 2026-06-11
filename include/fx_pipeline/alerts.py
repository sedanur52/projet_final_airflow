import logging
from typing import Any

from fx_pipeline.config import FX_ALERT_THRESHOLD
from fx_pipeline.storage import get_postgres_hook


LOGGER = logging.getLogger(__name__)


def detect_fx_alerts(run_result: dict[str, Any]) -> dict[str, Any]:
    loaded_rows = run_result.get("loaded_rows", [])
    if not loaded_rows:
        run_result["alert_count"] = 0
        run_result["message"] = f"{run_result['message']} | no alert candidates"
        return run_result

    hook = get_postgres_hook()
    alert_count = 0

    for row in loaded_rows:
        previous_rows = hook.get_records(
            """
            SELECT exchange_rate, rate_date
            FROM fx_rates
            WHERE base_currency = %s
              AND quote_currency = %s
              AND rate_date < %s
            ORDER BY rate_date DESC
            LIMIT 1
            """,
            parameters=(
                row["base_currency"],
                row["quote_currency"],
                row["rate_date"],
            ),
        )

        if not previous_rows:
            continue

        previous_rate = float(previous_rows[0][0])
        current_rate = float(row["exchange_rate"])
        if previous_rate == 0:
            continue

        rate_delta_ratio = abs(current_rate - previous_rate) / previous_rate
        if rate_delta_ratio < FX_ALERT_THRESHOLD:
            continue

        hook.run(
            """
            INSERT INTO fx_alerts (
                run_id,
                rate_date,
                currency_pair,
                previous_rate,
                current_rate,
                rate_delta,
                threshold_used
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                run_result["run_id"],
                row["rate_date"],
                row["currency_pair"],
                previous_rate,
                current_rate,
                rate_delta_ratio,
                FX_ALERT_THRESHOLD,
            ),
        )
        alert_count += 1
        LOGGER.warning(
            "Alerte detectee pour %s : previous=%s current=%s delta=%s",
            row["currency_pair"],
            previous_rate,
            current_rate,
            rate_delta_ratio,
        )

    run_result["alert_count"] = alert_count
    run_result["message"] = f"{run_result['message']} | {alert_count} alerts detected"
    return run_result
