import unittest
from unittest.mock import patch

import pandas as pd

from actus.intents.credit_ops_snapshot import _extract_root_causes, _lookup_root_causes


class TestCreditRootCauseMetadata(unittest.TestCase):
    def test_extract_root_causes_from_legacy_metadata_shape(self) -> None:
        rows = [
            {
                "chunk_type": "ticket_summary",
                "metadata": {
                    "root_cause": "Price discrepancy",
                    "root_causes_all": ["Price discrepancy", "Item should be PPD"],
                },
            }
        ]

        primary, all_causes = _extract_root_causes(rows)

        self.assertEqual("Price discrepancy", primary)
        self.assertEqual(
            ["Price discrepancy", "Item should be PPD"],
            all_causes,
        )

    def test_extract_root_causes_from_new_design_metadata_shape(self) -> None:
        rows = [
            {
                "chunk_type": "ticket_summary",
                "metadata": {
                    "root_cause_primary_id": "ppd_mismatch",
                    "root_cause_ids": ["ppd_mismatch", "sub_price_mismatch"],
                },
            }
        ]

        primary, all_causes = _extract_root_causes(rows)

        self.assertEqual("Item should be PPD", primary)
        self.assertEqual(
            ["Item should be PPD", "Item not price matched when subbing"],
            all_causes,
        )

    def test_lookup_root_causes_prefers_invoice_item_scope_from_canonical_ticket(self) -> None:
        class _FakeService:
            def __init__(self) -> None:
                self.required_ticket_ids: set[str] | None = None

            def get_canonical_tickets(self, *, required_ticket_ids: set[str] | None = None):
                self.required_ticket_ids = required_ticket_ids
                return {
                    "R-1": {
                        "root_cause_primary_label": "Price discrepancy",
                        "root_cause_labels": [
                            "Price discrepancy",
                            "Item should be PPD",
                        ],
                        "line_map": {
                            "INV-1|ITEM-1": [
                                {
                                    "invoice_number": "INV-1",
                                    "item_number": "ITEM-1",
                                    "root_cause_primary_label": "Item should be PPD",
                                    "root_cause_labels": ["Item should be PPD"],
                                }
                            ],
                            "INV-2|ITEM-2": [
                                {
                                    "invoice_number": "INV-2",
                                    "item_number": "ITEM-2",
                                    "root_cause_primary_label": "Price discrepancy",
                                    "root_cause_labels": ["Price discrepancy"],
                                }
                            ],
                        },
                    }
                }

        service = _FakeService()
        with patch("actus.intents.credit_ops_snapshot.get_runtime_service", return_value=service):
            out = _lookup_root_causes(
                pd.Series(["R-1"]),
                pd.Series(["INV-1"]),
                pd.Series(["ITEM-1"]),
            )

        self.assertEqual({"R-1"}, service.required_ticket_ids)
        self.assertEqual("Item should be PPD", out.at[0, "Root Causes (Primary)"])
        self.assertEqual("Item should be PPD", out.at[0, "Root Causes (All)"])
        self.assertFalse(bool(out.at[0, "Root Cause Mixed"]))


if __name__ == "__main__":
    unittest.main()
