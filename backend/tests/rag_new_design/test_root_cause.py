import unittest

from app.rag.new_design.models import RootCauseRule
from app.rag.new_design.root_cause import detect_root_causes, load_root_cause_rules


class TestRootCause(unittest.TestCase):
    def test_detect_primary_by_priority_then_count(self) -> None:
        rules = [
            RootCauseRule(id="price_discrepancy", label="Price", priority=40, threshold=1, keywords=["wrong price"]),
            RootCauseRule(id="ppd_mismatch", label="PPD", priority=90, threshold=1, keywords=["ppd"]),
        ]
        result = detect_root_causes(["wrong price here", "ppd issue"], rules)
        self.assertEqual("ppd_mismatch", result["root_cause_primary_id"])
        self.assertEqual(["ppd_mismatch", "price_discrepancy"], result["root_cause_ids"])

    def test_root_cause_determinism_across_runs(self) -> None:
        rules = [
            RootCauseRule(id="alpha", label="A", priority=50, threshold=1, keywords=["foo"]),
            RootCauseRule(id="beta", label="B", priority=50, threshold=1, keywords=["foo", "bar"]),
            RootCauseRule(id="gamma", label="C", priority=80, threshold=1, keywords=["baz"]),
        ]
        texts = ["foo bar", "foo", "baz"]

        primaries = []
        for _ in range(5):
            out = detect_root_causes(texts, rules)
            primaries.append(out["root_cause_primary_id"])

        self.assertEqual(["gamma"] * 5, primaries)

    def test_rules_include_price_loaded_after_invoice(self) -> None:
        rules = load_root_cause_rules()
        by_id = {rule.id: rule for rule in rules}
        self.assertIn("price_loaded_after_invoice", by_id)
        self.assertIn("price loaded after invoice", by_id["price_loaded_after_invoice"].keywords)

    def test_fallback_detects_price_loaded_after_invoice(self) -> None:
        rules = load_root_cause_rules()
        text = "Crediting after item was invoiced. Pricing has been updated."
        result = detect_root_causes([text], rules)
        self.assertEqual("price_loaded_after_invoice", result["root_cause_primary_id"])

    def test_fallback_detects_price_discrepancy(self) -> None:
        rules = load_root_cause_rules()
        text = "Price for item was not correct and customer was overbilled."
        result = detect_root_causes([text], rules)
        self.assertEqual("price_discrepancy", result["root_cause_primary_id"])


if __name__ == "__main__":
    unittest.main()
