import unittest

import pandas as pd

from actus.intents.system_updates import intent_system_updates


class TestSystemUpdatesIntent(unittest.TestCase):
    def test_system_rtn_updates_analysis_returns_metrics(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-02-05",
                    "Ticket Number": "R-100001",
                    "Invoice Number": "INV1",
                    "Item Number": "1001",
                    "Customer Number": "ABC01",
                    "Credit Request Total": "120.00",
                    "RTN_CR_No": "CR-1",
                    "Status": "[2026-02-10 09:00:00] Updated by the system",
                },
                {
                    "Date": "2026-03-10",
                    "Ticket Number": "R-100002",
                    "Invoice Number": "INV2",
                    "Item Number": "1002",
                    "Customer Number": "ABC02",
                    "Credit Request Total": "240.00",
                    "RTN_CR_No": "CR-2",
                    "Status": "[2026-03-15 09:00:00] Updated by the system",
                },
                {
                    "Date": "2026-03-09",
                    "Ticket Number": "R-100003",
                    "Invoice Number": "INV3",
                    "Item Number": "1003",
                    "Customer Number": "ABC03",
                    "Credit Request Total": "360.00",
                    "RTN_CR_No": "CR-3",
                    "Status": "[2026-03-15 10:00:00] Updated by the system",
                },
                {
                    "Date": "2026-01-29",
                    "Ticket Number": "R-100099",
                    "Invoice Number": "INV9",
                    "Item Number": "1009",
                    "Customer Number": "ABC09",
                    "Credit Request Total": "900.00",
                    "RTN_CR_No": "CR-9",
                    "Status": "[2026-03-20 10:00:00] Updated by the system",
                },
                {
                    "Date": "2026-01-10",
                    "Ticket Number": "R-000010",
                    "Invoice Number": "INV0",
                    "Item Number": "1000",
                    "Customer Number": "ABC00",
                    "Credit Request Total": "50.00",
                    "RTN_CR_No": "CR-0",
                    "Status": "[2026-01-15 10:00:00] Updated by the system",
                },
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for the last 2 months",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("System RTN updates analysis:", text)
        self.assertIn("Window used: **2026-01-30 → 2026-03-30**", text)
        self.assertIn("System-updated records with RTN/CR: **4** / **$1,620.00**", text)
        self.assertIn("Batch update dates: **3** total, **1** with multi-record batches affecting **2** record(s) / **$600.00**", text)
        self.assertIn("Largest batch: **2** record(s) on **2026-03-15** / **$600.00**", text)
        self.assertIn("R-100099", text)
        self.assertIn("Batch Credit Total", rows.columns)
        self.assertIn("Days To System Credit", rows.columns)
        self.assertIn("System Update Outlier", rows.columns)
        self.assertIn("Primary Update Source", rows.columns)
        self.assertIn("Reopened After Terminal", rows.columns)
        self.assertEqual({"system"}, set(rows["Primary Update Source"].tolist()))
        self.assertEqual({False}, set(rows["Reopened After Terminal"].tolist()))
        self.assertEqual(4, meta["system_updates_summary"]["total_records"])
        self.assertEqual(1620.0, meta["system_updates_summary"]["credit_total"])
        self.assertEqual(1, meta["system_updates_summary"]["outlier_count"])
        self.assertEqual(1, meta["system_updates_summary"]["batched_dates"])
        self.assertEqual(600.0, meta["system_updates_summary"]["batched_credit_total"])
        self.assertEqual("2026-03-15", meta["system_updates_summary"]["largest_batch_date"])
        self.assertEqual(600.0, meta["system_updates_summary"]["largest_batch_credit_total"])
        self.assertIn("R-100099", meta["system_updates_summary"]["outlier_ticket_ids"])
        self.assertEqual(
            [
                {
                    "id": "system_updates",
                    "label": "System RTN updates preview (2026-01-30 → 2026-03-30)",
                    "prefix": "system rtn updates analysis from 2026-01-30 to 2026-03-30",
                }
            ],
            meta["suggestions"],
        )

    def test_current_month_uses_month_start_window(self) -> None:
        now = pd.Timestamp.now()
        month_start = now.normalize().replace(day=1)
        prior_month = month_start - pd.Timedelta(days=5)
        current_update = month_start + pd.Timedelta(days=3, hours=9)

        df = pd.DataFrame(
            [
                {
                    "Date": month_start.strftime("%Y-%m-%d"),
                    "Ticket Number": "R-200001",
                    "Invoice Number": "INV20",
                    "Item Number": "2001",
                    "Customer Number": "CUR01",
                    "Credit Request Total": "1250.00",
                    "RTN_CR_No": "CR-20",
                    "Status": f"[{current_update.strftime('%Y-%m-%d %H:%M:%S')}] Updated by the system",
                },
                {
                    "Date": prior_month.strftime("%Y-%m-%d"),
                    "Ticket Number": "R-199999",
                    "Invoice Number": "INV19",
                    "Item Number": "1999",
                    "Customer Number": "OLD01",
                    "Credit Request Total": "800.00",
                    "RTN_CR_No": "CR-19",
                    "Status": f"[{prior_month.strftime('%Y-%m-%d 10:00:00')}] Updated by the system",
                },
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for the current month",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        expected_start = month_start.strftime("%Y-%m-%d")
        expected_end = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
        self.assertIn(f"Window used: **{expected_start} → {expected_end}**", text)
        self.assertIn("System-updated records with RTN/CR: **1** / **$1,250.00**", text)
        self.assertEqual(1, meta["system_updates_summary"]["total_records"])
        self.assertEqual(1250.0, meta["system_updates_summary"]["credit_total"])
        self.assertEqual(
            f"System RTN updates preview ({expected_start} → {expected_end})",
            meta["suggestions"][0]["label"],
        )

    def test_manual_rtn_updates_without_system_status_are_reported(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-03-01",
                    "Ticket Number": "R-300001",
                    "Invoice Number": "INV30",
                    "Item Number": "3001",
                    "Customer Number": "MAN01",
                    "Credit Request Total": "500.00",
                    "RTN_CR_No": "CR-30",
                    "Status": "[2026-03-12 09:00:00] Credited manually",
                },
                {
                    "Date": "2026-03-02",
                    "Ticket Number": "R-300002",
                    "Invoice Number": "INV31",
                    "Item Number": "3002",
                    "Customer Number": "MAN02",
                    "Credit Request Total": "700.00",
                    "RTN_CR_No": "CR-31",
                    "Status": "[2026-03-12 09:30:00] Credited manually",
                },
                {
                    "Date": "2026-03-03",
                    "Ticket Number": "R-300003",
                    "Invoice Number": "INV32",
                    "Item Number": "3003",
                    "Customer Number": "SYS03",
                    "Credit Request Total": "900.00",
                    "RTN_CR_No": "CR-32",
                    "Status": "[2026-03-13 10:00:00] Updated by the system",
                },
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for this month",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertIn("Manual RTN / closure updates captured in status history:", text)
        self.assertIn("Manual records with RTN/CR: **2** / **$1,200.00**", text)
        self.assertIn("Manual batch dates: **1** total, **1** with multi-record batches affecting **2** record(s) / **$1,200.00**", text)
        self.assertIn("Largest manual batch: **2** record(s) on **2026-03-12** / **$1,200.00**", text)
        self.assertIn("Update Source", rows.columns)
        self.assertIn("Update Mix Status", rows.columns)
        self.assertEqual({"manual_only", "system_only"}, set(rows["Update Mix Status"].tolist()))
        self.assertEqual(1, meta["system_updates_summary"]["total_records"])
        self.assertEqual(2, meta["system_updates_summary"]["manual_record_count"])
        self.assertEqual(1200.0, meta["system_updates_summary"]["manual_credit_total"])
        self.assertEqual(2, meta["system_updates_summary"]["manual_batched_records"])
        self.assertEqual(1200.0, meta["system_updates_summary"]["manual_batched_credit_total"])

    def test_later_manual_credit_number_event_is_counted_alongside_system_event(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2026-02-18",
                    "Ticket Number": "R-400001",
                    "Invoice Number": "INV40",
                    "Item Number": "4001",
                    "Customer Number": "CRD02",
                    "Credit Request Total": "4.33",
                    "RTN_CR_No": "RTNCM0046563",
                    "Status": (
                        "[2026-02-18 12:09:55] WIP: On macro. Went through investigation. "
                        "[2026-02-24 09:45:36] [DW] Submitted to Billing: February 24th, 2026 "
                        "[2026-03-02 12:21:12] [SYSTEM] Updated by the system. "
                        "[2026-03-09 14:01:21] [DW] Closed: credit number provided. Ticket is resolved and will be closed automatically in 14 days."
                    ),
                }
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for this month",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(2, len(rows))
        self.assertEqual({"system", "manual"}, set(rows["Update Source"].tolist()))
        self.assertEqual({"mixed"}, set(rows["Update Mix Status"].tolist()))
        self.assertEqual({"system"}, set(rows["Primary Update Source"].tolist()))
        self.assertEqual({False}, set(rows["Reopened After Terminal"].tolist()))
        self.assertIn("Manual RTN / closure updates captured in status history:", text)
        self.assertIn("Manual records with RTN/CR: **1** / **$4.33**", text)
        self.assertEqual(1, meta["system_updates_summary"]["total_records"])
        self.assertEqual(1, meta["system_updates_summary"]["manual_record_count"])
        self.assertEqual(4.33, meta["system_updates_summary"]["manual_credit_total"])
        self.assertEqual(2, meta["system_updates_summary"]["preview_total_records"])

    def test_manual_closure_wins_before_later_system_verification(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2025-02-26",
                    "Ticket Number": "R-028741",
                    "Invoice Number": "INV28741",
                    "Item Number": "1009999",
                    "Customer Number": "CRT01",
                    "Credit Request Total": "928.57",
                    "RTN_CR_No": "RTNCM0033836",
                    "Status": (
                        "Reason: Per CS (sbrooks) [Ticket #R-028741] | Credit Request No.: RTNCM0033836 "
                        "[2025-07-16 11:54:54] Update: Credit request received and approved. "
                        "Ticket resolved on March 18th, 2025, closed on April 1st, 2025. "
                        "[2025-12-18 01:41:06] [SYSTEM] Update: CR number verified and credit processing completed. "
                        "Ticket automatically closed by the system."
                    ),
                }
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for the last 12 months",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(1, len(rows))
        manual_row = rows.iloc[0]
        self.assertEqual("manual_closure", manual_row["Update Event Type"])
        self.assertEqual(pd.Timestamp("2025-07-16 11:54:54"), pd.Timestamp(manual_row["Update Event Time"]))
        self.assertEqual("2025-03-18", manual_row["Mentioned Resolved Date"])
        self.assertEqual("2025-04-01", manual_row["Mentioned Closed Date"])
        self.assertEqual("manual", manual_row["Update Source"])
        self.assertEqual("manual", manual_row["Primary Update Source"])
        self.assertFalse(bool(manual_row["Reopened After Terminal"]))
        self.assertEqual(0, meta["system_updates_summary"]["total_records"])
        self.assertEqual(1, meta["system_updates_summary"]["manual_record_count"])
        self.assertIn("System-updated records with RTN/CR: **0** / **$0.00**", text)
        self.assertIn("Manual RTN / closure updates captured in status history:", text)

    def test_reopened_after_manual_terminal_allows_later_system_update(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Date": "2025-10-10",
                    "Ticket Number": "R-500001",
                    "Invoice Number": "INV50",
                    "Item Number": "5001",
                    "Customer Number": "PCR08",
                    "Credit Request Total": "125.00",
                    "RTN_CR_No": "RTNINT0050001",
                    "Status": (
                        "[2025-12-17 17:49:48] [DW] Credit number provided. Ticket is resolved and will closed officially in 14 days. "
                        "[2026-01-20 15:29:08] [DW] There's no credit number. It is not available on the Billing Master. "
                        "[2026-01-20 15:29:24] [DW] Submitted to Billing: January 16th, 2026. "
                        "[2026-02-02 14:19:29] [SYSTEM] Updated by the system."
                    ),
                }
            ]
        )

        text, rows, meta = intent_system_updates(
            "show me system RTN updates analysis for the last 12 months",
            df,
        )

        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(2, len(rows))
        self.assertEqual({"manual", "system"}, set(rows["Update Source"].tolist()))
        self.assertEqual({"mixed"}, set(rows["Update Mix Status"].tolist()))
        self.assertEqual({"system"}, set(rows["Primary Update Source"].tolist()))
        self.assertEqual({True}, set(rows["Reopened After Terminal"].tolist()))
        system_row = rows[rows["Update Source"].eq("system")].iloc[0]
        self.assertEqual("system_update", system_row["Update Event Type"])
        self.assertEqual(pd.Timestamp("2026-02-02 14:19:29"), pd.Timestamp(system_row["Update Event Time"]))
        self.assertEqual(1, meta["system_updates_summary"]["total_records"])
        self.assertEqual(1, meta["system_updates_summary"]["manual_record_count"])
        self.assertIn("System-updated records with RTN/CR: **1** / **$125.00**", text)


if __name__ == "__main__":
    unittest.main()
