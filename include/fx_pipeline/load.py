from typing import Any

from fx_pipeline.storage import get_postgres_hook


def load_fx_rates(quality_payload: dict[str, Any]) -> dict[str, Any]:
    rows = quality_payload["rows_valid"]
    hook = get_postgres_hook()

    for row in rows:
        hook.run(
            """
            INSERT INTO fx_rates (
                run_id,
                rate_date,
                base_currency,
                quote_currency,
                currency_pair,
                exchange_rate
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (rate_date, base_currency, quote_currency)
            DO UPDATE SET
                exchange_rate = EXCLUDED.exchange_rate,
                ingested_at = CURRENT_TIMESTAMP
            """,
            parameters=(
                row["run_id"],
                row["rate_date"],
                row["base_currency"],
                row["quote_currency"],
                row["currency_pair"],
                row["exchange_rate"],
            ),
        )

    return {
        "run_id": quality_payload["run_id"],
        "rows_received": quality_payload["rows_received"],
        "rows_valid": quality_payload["rows_valid_count"],
        "rows_rejected": quality_payload["rows_rejected_count"],
        "rows_inserted": len(rows),
        "status": "success",
        "message": f"{len(rows)} FX rows loaded into fx_rates",
        "loaded_rows": rows,
    }


def load_fx_rejections(quality_payload: dict[str, Any]) -> dict[str, Any]:
    rows = quality_payload["rows_rejected"]
    hook = get_postgres_hook()

    for row in rows:
        hook.run(
            """
            INSERT INTO fx_rejections (
                run_id,
                rate_date,
                base_currency,
                quote_currency,
                currency_pair,
                raw_rate,
                rejection_reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                row["run_id"],
                row["rate_date"],
                row["base_currency"],
                row["quote_currency"],
                row["currency_pair"],
                row["raw_rate"],
                row["rejection_reason"],
            ),
        )

    return {
        "run_id": quality_payload["run_id"],
        "rows_received": quality_payload["rows_received"],
        "rows_valid": quality_payload["rows_valid_count"],
        "rows_rejected": quality_payload["rows_rejected_count"],
        "rows_inserted": 0,
        "status": "quality_rejections_logged",
        "message": f"{len(rows)} FX rows inserted into fx_rejections",
    }
