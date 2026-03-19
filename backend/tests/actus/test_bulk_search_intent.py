import unittest

import pandas as pd

from actus.intents.bulk_search import intent_bulk_search


class TestBulkSearchIntent(unittest.TestCase):
    def test_bulk_item_lookup_matches_hyphenated_and_numeric_item_numbers(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Item Number": "009-PCDB1633S",
                    "Customer Number": "ABC",
                    "Invoice Number": "14068709",
                    "Credit Request Total": 12.34,
                },
                {
                    "Item Number": "1007325",
                    "Customer Number": "XYZ",
                    "Invoice Number": "14068710",
                    "Credit Request Total": 45.67,
                },
            ]
        )

        response = intent_bulk_search(
            "Actus, bulk lookup item numbers: 009-PCDB1633S, 1007325",
            df,
        )

        self.assertIsNotNone(response)
        assert response is not None
        text, rows, meta = response
        self.assertIn("Bulk search results:", text)
        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(2, len(rows))
        self.assertEqual(
            {"009-PCDB1633S", "1007325"},
            set(rows["Item Number"].dropna().tolist()),
        )
        self.assertTrue(meta.get("show_table"))
        self.assertEqual(2, meta.get("csv_row_count"))


if __name__ == "__main__":
    unittest.main()
