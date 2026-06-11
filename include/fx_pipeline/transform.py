from typing import Any


def transform_fx_rates(extracted_payload: dict[str, Any]) -> dict[str, Any]:
    payload = extracted_payload["payload"]
    base_currency = extracted_payload["base_currency"]
    rate_date = payload["date"]
    target_currencies = extracted_payload["target_currencies"]

    transformed_rows = []
    for quote_currency, exchange_rate in payload.get("rates", {}).items():
        transformed_rows.append(
            {
                "run_id": extracted_payload["run_id"],
                "rate_date": rate_date,
                "base_currency": base_currency,
                "quote_currency": quote_currency,
                "currency_pair": f"{base_currency}/{quote_currency}",
                "exchange_rate": exchange_rate,
            }
        )

    return {
        "run_id": extracted_payload["run_id"],
        "base_currency": base_currency,
        "target_currencies": target_currencies,
        "rate_date": rate_date,
        "rows_received": len(transformed_rows),
        "rows": transformed_rows,
    }
