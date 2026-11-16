from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from orchard_grade.application import Application, load_fixture, run, verify
from orchard_grade.contracts.batches import ContractError, records_from_fixture, stable_hash
from orchard_grade.exports.packhouse import verify_bundle
from orchard_grade.trace.lot_journal import Repository


FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "acceptance.json"
DELIVERED_FILES = {"records.csv", "rules.csv", "summary.json", "decisions.json",
                   "audit-events.json", "manifest.json"}


class DeliveryRecoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / "run.sqlite3"
        self.fixture = load_fixture(FIXTURE)
        self.records = records_from_fixture(self.fixture)

    def read(self, output: Path, name: str):
        return json.loads((output / name).read_text(encoding="utf-8"))

    def snapshot_of(self):
        return Repository(self.database).snapshot()

    def test_second_delivery_to_new_directory_restores_the_same_run(self):
        first = run(FIXTURE, self.database, self.root / "team-a")
        second = run(FIXTURE, self.database, self.root / "team-b")

        self.assertEqual({path.name for path in (self.root / "team-b").iterdir()}, DELIVERED_FILES)
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["manifest"]["manifest_hash"], second["manifest"]["manifest_hash"])
        self.assertEqual(first["delivery_hash"], second["delivery_hash"])
        self.assertEqual(verify_bundle(self.root / "team-b"), [])
        self.assertEqual(self.read(self.root / "team-a", "summary.json")["record_count"],
                         self.read(self.root / "team-b", "summary.json")["record_count"])

    def test_recovery_does_not_advance_grading_and_keeps_the_conclusion(self):
        first = run(FIXTURE, self.database, self.root / "team-a")
        before = self.snapshot_of()
        second = run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual(before, self.snapshot_of())
        self.assertEqual(first["snapshot"], second["snapshot"])
        self.assertEqual((self.root / "team-a" / "records.csv").read_text(encoding="utf-8"),
                         (self.root / "team-b" / "records.csv").read_text(encoding="utf-8"))

    def test_recovery_uses_frozen_material_instead_of_revised_database(self):
        run(FIXTURE, self.database, self.root / "team-a")
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE records SET state='released', version=version+1")
            connection.execute("UPDATE decisions SET outcome='approve', score=1.0")
        restored = run(FIXTURE, self.database, self.root / "team-b")
        summary = self.read(self.root / "team-b", "summary.json")
        self.assertEqual(summary["repository"]["states"]["recalled"], 8)
        self.assertEqual(restored["snapshot"]["states"], {"recalled": 8})
        self.assertNotIn("approve", (self.root / "team-b" / "records.csv").read_text(encoding="utf-8"))

    def test_recovery_reports_trusted_and_created_files(self):
        run(FIXTURE, self.database, self.root / "team-a")
        second = run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual(second["delivery"]["trusted_files"], [])
        self.assertEqual(set(second["delivery"]["created_files"]),
                         {"records.csv", "rules.csv", "summary.json", "decisions.json",
                          "audit-events.json", "manifest.json"})
        self.assertEqual(second["delivery"]["repaired_files"], [])
        third = run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual(set(third["delivery"]["trusted_files"]), set(second["delivery"]["created_files"]))
        self.assertIn("manifest.json", third["delivery"]["trusted_files"])
        self.assertEqual(third["delivery"]["repaired_files"], [])

    def test_damaged_file_is_rebuilt_and_declared(self):
        run(FIXTURE, self.database, self.root / "team-a")
        (self.root / "team-b").mkdir()
        (self.root / "team-b" / "summary.json").write_text('{"record_count": 1}', encoding="utf-8")
        (self.root / "team-b" / "records.csv").write_text("record_id,state\n", encoding="utf-8")
        restored = run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual(restored["delivery"]["repaired_files"], ["records.csv", "summary.json"])
        self.assertEqual(restored["delivery"]["needs_reverification"], ["records.csv", "summary.json"])
        self.assertEqual(verify_bundle(self.root / "team-b"), [])

    def test_partial_write_is_completed_before_success_is_returned(self):
        run(FIXTURE, self.database, self.root / "team-a")
        partial = self.root / "team-b"
        partial.mkdir()
        (partial / "manifest.json").write_text('{"run_id": "torn"', encoding="utf-8")
        restored = run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual({path.name for path in partial.iterdir()}, DELIVERED_FILES)
        self.assertEqual(verify_bundle(partial), [])
        self.assertEqual(restored["delivery"]["created_files"],
                         ["audit-events.json", "decisions.json", "records.csv", "rules.csv", "summary.json"])
        self.assertIn("manifest.json", restored["delivery"]["repaired_files"])

    def test_damaged_frozen_material_refuses_recovery_and_keeps_conclusions(self):
        first = run(FIXTURE, self.database, self.root / "team-a")
        conclusions = self.snapshot_of()
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE deliveries SET artifacts_json=?", ('{"run_id": "torn"',))
        with self.assertRaisesRegex(ContractError, "unreadable|failed verification"):
            run(FIXTURE, self.database, self.root / "team-b")
        self.assertEqual(conclusions, self.snapshot_of())
        check = verify(self.database, self.root / "team-a")
        self.assertFalse(check["ok"])
        self.assertFalse(check["delivery"]["trusted"])
        self.assertEqual(first["snapshot"]["states"], conclusions["states"])

    def test_tampered_frozen_material_is_reported(self):
        run(FIXTURE, self.database, self.root / "team-a")
        with sqlite3.connect(self.database) as connection:
            row = connection.execute("SELECT artifacts_json FROM deliveries").fetchone()
            artifact = json.loads(row[0])
            artifact["decisions"][0]["outcome"] = "approve"
            connection.execute("UPDATE deliveries SET artifacts_json=?", (json.dumps(artifact),))
        with self.assertRaisesRegex(ContractError, "failed verification"):
            run(FIXTURE, self.database, self.root / "team-b")
        report = Repository(self.database).delivery_report(
            json.loads((self.root / "team-a" / "manifest.json").read_text())["run_id"])
        self.assertFalse(report["trusted"])
        self.assertTrue(report["errors"])

    def test_output_of_another_run_is_not_overwritten(self):
        run(FIXTURE, self.database, self.root / "team-a")
        other = dict(self.fixture, scenario="another batch")
        with self.assertRaisesRegex(ContractError, "different processing run"):
            run(FIXTURE, self.database, self.root / "team-a") if False else Application(self.database).run_fixture(
                other, self.root / "team-a")
        self.assertTrue(verify_bundle(self.root / "team-a") == [])

    def test_interrupted_first_attempt_is_refused_not_reprocessed(self):
        run_id = Repository(self.database).begin_run(stable_hash(self.fixture), len(self.records))
        Application(self.database).process(self.records[0])
        before = self.snapshot_of()
        with self.assertRaisesRegex(ContractError, "incomplete delivery"):
            Application(self.database).run_fixture(self.fixture, self.root / "team-b")
        self.assertEqual(before, self.snapshot_of())
        self.assertFalse((self.root / "team-b" / "manifest.json").exists())

    def test_verify_links_delivery_to_one_processing_run_after_restart(self):
        first = run(FIXTURE, self.database, self.root / "team-a")
        run(FIXTURE, self.database, self.root / "team-b")
        reopened = Repository(self.database)
        for output in (self.root / "team-a", self.root / "team-b"):
            check = verify(self.database, output)
            self.assertTrue(check["ok"], check)
            self.assertEqual(check["delivery"]["run_id"], first["run_id"])
            self.assertEqual(check["delivery"]["delivery_hash"], first["delivery_hash"])
            self.assertTrue(check["delivery"]["trusted"])
        self.assertEqual(reopened.delivery_report(first["run_id"])["status"], "ready")

    def test_frozen_material_cannot_be_replaced(self):
        run(FIXTURE, self.database, self.root / "team-a")
        repository = Repository(self.database)
        run_id = json.loads((self.root / "team-a" / "manifest.json").read_text())["run_id"]
        artifacts, _ = repository.delivery_material(run_id)
        with self.assertRaisesRegex(ContractError, "cannot be replaced"):
            repository.save_delivery(run_id, dict(artifacts, fixture_hash="0" * 64))


if __name__ == "__main__":
    unittest.main()
