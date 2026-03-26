import unittest

from actus.intents.credit_ops_snapshot import _extract_root_causes


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


if __name__ == "__main__":
    unittest.main()
