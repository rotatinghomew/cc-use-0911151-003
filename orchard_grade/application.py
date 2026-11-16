from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from orchard_grade.contracts.batches import ContractError, Decision, DomainRecord, records_from_fixture, stable_hash
from orchard_grade.trace.lot_journal import Repository
from orchard_grade.operations.release_flow import advance_for_outcome, decide
from orchard_grade.exports.packhouse import assert_output_available, install_bundle, prepare_export
from orchard_grade.sensing.fusion import derive_context
from orchard_grade.inspection.receipts import InspectionReceipt

from orchard_grade.inspection.work_orders import InspectionWork

class Application:
    def __init__(self, database: str | Path, context: Mapping[str, Any] | None = None):
        self.repository = Repository(database)
        self.inspection_work = InspectionWork(self.repository)
        self.context = dict(context or {})

    def ingest(self, record: DomainRecord) -> DomainRecord:
        current = self.repository.get_record(record.record_id)
        if current is None:
            self.repository.save_record(record)
            return record
        if current.fingerprint == record.fingerprint:
            return current
        replacement = DomainRecord(record.record_id, record.payload, current.state,
                                   current.version + 1, record.occurred_at, record.source)
        self.repository.save_record(replacement, expected_version=current.version)
        return replacement

    def process(self, record: DomainRecord) -> tuple[DomainRecord, Decision]:
        stored = self.ingest(record)
        context = dict(self.context)
        context.update(derive_context(stored, self.repository))
        decision = decide(stored, context)
        self.repository.save_decision(decision)
        advanced = advance_for_outcome(stored, decision)
        if advanced.fingerprint != stored.fingerprint:
            self.repository.save_record(advanced, expected_version=stored.version)
        return advanced, decision

    def run_fixture(self, fixture: Mapping[str, Any], output: str | Path) -> dict[str, Any]:
        records = records_from_fixture(fixture)
        fixture_hash = stable_hash(fixture)
        # Serialise first deliveries of the same batch: two teams asking at
        # the same time must not each advance the grading workflow.
        with self.repository.processing(fixture_hash):
            run_id = self.repository.begin_run(fixture_hash, len(records))
            completed = self.repository.completed_run(run_id)
            if completed is not None:
                return self.deliver(run_id, completed, output)
            # Refuse to replace another run's delivery before touching the
            # database, so a rejected request cannot alter grading evidence.
            assert_output_available(output, run_id)
            interrupted = self.repository.interrupted_run_evidence(run_id, records)
            if interrupted:
                raise ContractError("this batch was already processed by an incomplete delivery; "
                                     "the frozen result is unavailable, so it needs re-verification: "
                                     + "; ".join(interrupted))
            decisions: dict[str, Decision] = {}
            final_records: list[DomainRecord] = []
            for record in records:
                final_record, decision = self.process(record)
                final_records.append(final_record)
                decisions[record.record_id] = decision
            snapshot = self.repository.snapshot()
            if snapshot["chain_errors"]:
                raise ContractError("audit chain verification failed")
            artifacts = prepare_export(run_id, fixture_hash, final_records, decisions, snapshot,
                                       self.repository.event_rows())
            result = {"run_id": run_id, "fixture_hash": fixture_hash, "records": len(final_records),
                      "decisions": len(decisions), "manifest": artifacts["manifest"], "snapshot": snapshot,
                      "delivery_hash": artifacts["delivery_hash"]}
            # Freeze the material together with the finished marker; a run
            # that cannot record its delivery must not hand out a directory
            # that cannot be restored.
            self.repository.finish_run(run_id, result, artifacts)
        report = install_bundle(output, artifacts)
        return result | {"delivery": {key: value for key, value in report.items() if not key.startswith("_")}}

    def deliver(self, run_id: str, result: Mapping[str, Any], output: str | Path) -> dict[str, Any]:
        """Restore a completed delivery from frozen material without reprocessing.

        Nothing in this path writes records, decisions, batch states or the
        audit chain: it only reproduces the statistics, final batch rows and
        audit evidence that were frozen when the run finished.
        """
        artifacts, _ = self.repository.delivery_material(run_id)
        if artifacts.get("fixture_hash") != result.get("fixture_hash") or artifacts.get("run_id") != run_id:
            raise ContractError("delivery material does not belong to this processing run")
        before = self.repository.snapshot()
        report = install_bundle(output, artifacts)
        after = self.repository.snapshot()
        if before != after:
            raise ContractError("delivery recovery changed the recorded grading outcome")
        restored = {
            "run_id": run_id,
            "fixture_hash": result.get("fixture_hash"),
            "records": result.get("records"),
            "decisions": result.get("decisions"),
            "manifest": artifacts["manifest"],
            "snapshot": artifacts["snapshot"],
            "delivery_hash": artifacts["delivery_hash"],
            "delivery": {key: value for key, value in report.items() if not key.startswith("_")},
        }
        return restored

    def inspect(self) -> dict[str, Any]:
        return {"snapshot": self.repository.snapshot(), "records": [record.__dict__ for record in self.repository.list_records()],
                "decisions": self.repository.decision_rows()}

    def receive_inspection(self, receipt: InspectionReceipt) -> bool:
        return self.repository.add_inspection(receipt)

    def dispatch(self, lot_id: str) -> dict[str, Any]:
        return self.repository.dispatch(lot_id).__dict__


def load_fixture(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read fixture: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractError("fixture root must be an object")
    return value


def run(fixture_path: str | Path, database: str | Path, output: str | Path,
        context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return Application(database, context).run_fixture(load_fixture(fixture_path), output)


def verify(database: str | Path, output: str | Path) -> dict[str, Any]:
    return Repository(database).verify(output)
