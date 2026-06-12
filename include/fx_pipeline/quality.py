from datetime import date, datetime
from typing import Any

from fx_pipeline.config import (
    FX_FRESHNESS_THRESHOLD_DAYS,
    FX_FORCE_INVALID_CODE_FOR,
    FX_FORCE_INVALID_RATE_FOR,
)


def evaluate_fx_quality(transformed_payload: dict[str, Any]) -> dict[str, Any]:
    target_currencies = transformed_payload["target_currencies"]
    rows = transformed_payload["rows"]
    rows_valid = []
    rows_rejected = []

    available_quotes = {row["quote_currency"] for row in rows}
    for expected_currency in target_currencies:
        if expected_currency not in available_quotes:
            rows_rejected.append(
                {
                    "run_id": transformed_payload["run_id"],
                    "rate_date": transformed_payload["rate_date"],
                    "base_currency": transformed_payload["base_currency"],
                    "quote_currency": expected_currency,
                    "currency_pair": (
                        f"{transformed_payload['base_currency']}/{expected_currency}"
                    ),
                    "raw_rate": None,
                    "rejection_reason": "Devise cible manquante dans la reponse API",
                }
            )

    api_date = datetime.strptime(transformed_payload["rate_date"], "%Y-%m-%d").date()
    freshness_gap = (date.today() - api_date).days

    for row in rows:
        rejection_reason = None
        row_to_check = row.copy()

        # Etape de simulation d'un taux invalide pour les tests de qualite.
        if (
            FX_FORCE_INVALID_RATE_FOR
            and row_to_check["quote_currency"] == FX_FORCE_INVALID_RATE_FOR
        ):
            row_to_check["exchange_rate"] = 0

        # Etape de simulation d'un code devise invalide pour les tests de qualite.
        if (
            FX_FORCE_INVALID_CODE_FOR
            and row_to_check["quote_currency"] == FX_FORCE_INVALID_CODE_FOR
        ):
            row_to_check["quote_currency"] = "XX"

        if not row_to_check["base_currency"] or not row_to_check["quote_currency"]:
            rejection_reason = "Structure invalide sur les codes devises"
        elif row_to_check["exchange_rate"] is None:
            rejection_reason = "Taux manquant"
        elif float(row_to_check["exchange_rate"]) <= 0:
            rejection_reason = "Taux negatif ou nul"
        elif (
            len(row_to_check["base_currency"]) != 3
            or len(row_to_check["quote_currency"]) != 3
        ):
            rejection_reason = "Code devise invalide"
        elif freshness_gap > FX_FRESHNESS_THRESHOLD_DAYS:
            rejection_reason = "Donnee trop ancienne selon le seuil de fraicheur"

        if rejection_reason:
            rows_rejected.append(
                {
                    "run_id": row_to_check["run_id"],
                    "rate_date": row_to_check["rate_date"],
                    "base_currency": row_to_check["base_currency"],
                    "quote_currency": row_to_check["quote_currency"],
                    "currency_pair": (
                        f"{row_to_check['base_currency']}/{row_to_check['quote_currency']}"
                    ),
                    "raw_rate": str(row_to_check["exchange_rate"]),
                    "rejection_reason": rejection_reason,
                }
            )
        else:
            rows_valid.append(row_to_check)

    return {
        "run_id": transformed_payload["run_id"],
        "is_valid": len(rows_rejected) == 0 and len(rows_valid) > 0,
        "rows_received": transformed_payload["rows_received"],
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rows_valid_count": len(rows_valid),
        "rows_rejected_count": len(rows_rejected),
    }
