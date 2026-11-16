from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from orchard_grade.contracts.batches import Decision, DomainRecord, ContractError, canonical_json, stable_hash, utc_now
from orchard_grade.inspection.receipts import InspectionReceipt, prepare_receipts, record_receipt, receipts_for_lot
from orchard_grade.operations.packhouse_queue import QueueAssignment, assign_released_lot, prepare_queue
from orchard_grade.exports.packhouse import delivery_fingerprint, verify_artifacts, verify_bundle


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS records(
    record_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS record_versions(
    record_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    stored_at TEXT NOT NULL,
    PRIMARY KEY(record_id, version)
);
CREATE TABLE IF NOT EXISTS decisions(
    decision_id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    score REAL NOT NULL,
    decision_json TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    FOREIGN KEY(record_id) REFERENCES records(record_id)
);
CREATE TABLE IF NOT EXISTS events(
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    UNIQUE(aggregate_id, aggregate_version, event_type)
);
CREATE TABLE IF NOT EXISTS runs(
    run_id TEXT PRIMARY KEY,
    fixture_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS deliveries(
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    delivery_hash TEXT NOT NULL,
    artifacts_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_records_state ON records(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events(aggregate_id, event_id);
CREATE INDEX IF NOT EXISTS idx_decisions_record ON decisions(record_id, decided_at);
"""


class Repository:
    def __init__(self, database: str | Path):
        self.database = str(database)
        self._lock = threading.RLock()
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            prepare_receipts(connection)
            prepare_queue(connection)

    def add_inspection(self, receipt: InspectionReceipt) -> bool:
        with self.transaction() as connection:
            row = connection.execute("SELECT payload_json FROM records WHERE record_id=?", (receipt.lot_id,)).fetchone()
            if row is None:
                raise ContractError("inspection lot does not exist")
            payload = json.loads(row[0])
            if str(payload.get("origin_code")) != receipt.origin_code:
                raise ContractError("inspection origin disagrees with lot")
            return record_receipt(connection, receipt)

    def inspection_rows(self, lot_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return receipts_for_lot(connection, lot_id)

    def dispatch(self, lot_id: str) -> QueueAssignment:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM records WHERE record_id=?", (lot_id,)).fetchone()
            if row is None:
                raise ContractError("dispatch lot does not exist")
            record = DomainRecord(row["record_id"], json.loads(row["payload_json"]),
                                  row["state"], row["version"], row["occurred_at"], row["source"])
            return assign_released_lot(connection, record)

    def queue_rows(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM packhouse_queue ORDER BY lot_id")]

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @contextmanager
    def processing(self, fixture_hash: str) -> Iterator[None]:
        """Serialise the first delivery of one batch across threads and processes.

        Holding this lock for the whole processing run means a second team
        asking for the same batch waits and then restores the frozen result
        instead of advancing the grading workflow a second time.
        """
        with self._lock:
            lock_path = Path(f"{self.database}.{stable_hash({'fixture': fixture_hash})[:24]}.lock")
            handle = open(lock_path, "a+b")
            try:
                try:
                    import fcntl
                except ImportError:  # pragma: no cover - non-POSIX platforms
                    fcntl = None
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _last_hash(self, connection: sqlite3.Connection) -> str:
        row = connection.execute("SELECT event_hash FROM events ORDER BY event_id DESC LIMIT 1").fetchone()
        return row[0] if row else "0" * 64

    def append_event(self, connection: sqlite3.Connection, event_type: str,
                     aggregate_id: str, aggregate_version: int, payload: Mapping[str, Any]) -> str:
        previous = self._last_hash(connection)
        created_at = utc_now()
        event_hash = stable_hash({"event_type": event_type, "aggregate_id": aggregate_id,
                                  "aggregate_version": aggregate_version, "payload": payload,
                                  "previous_hash": previous, "created_at": created_at})
        try:
            connection.execute("INSERT INTO events(event_type,aggregate_id,aggregate_version,payload_json,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?)",
                               (event_type, aggregate_id, aggregate_version, canonical_json(payload),
                                previous, event_hash, created_at))
        except sqlite3.IntegrityError:
            row = connection.execute("SELECT event_hash,payload_json FROM events WHERE aggregate_id=? AND aggregate_version=? AND event_type=?",
                                     (aggregate_id, aggregate_version, event_type)).fetchone()
            if row and row[1] == canonical_json(payload):
                return row[0]
            raise ContractError("conflicting event replay") from None
        return event_hash

    def save_record(self, record: DomainRecord, expected_version: int | None = None) -> str:
        with self.transaction() as connection:
            current = connection.execute("SELECT version,fingerprint FROM records WHERE record_id=?", (record.record_id,)).fetchone()
            if current and expected_version is not None and current[0] != expected_version:
                raise ContractError("optimistic version conflict")
            if current and current[1] == record.fingerprint:
                return record.fingerprint
            if current and record.version <= current[0]:
                raise ContractError("record version must advance")
            payload = canonical_json(record.payload)
            connection.execute("INSERT INTO records(record_id,version,state,payload_json,fingerprint,occurred_at,source,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(record_id) DO UPDATE SET version=excluded.version,state=excluded.state,payload_json=excluded.payload_json,fingerprint=excluded.fingerprint,occurred_at=excluded.occurred_at,source=excluded.source,updated_at=excluded.updated_at",
                               (record.record_id, record.version, record.state, payload, record.fingerprint,
                                record.occurred_at, record.source, utc_now()))
            connection.execute("INSERT INTO record_versions(record_id,version,state,payload_json,fingerprint,stored_at) VALUES(?,?,?,?,?,?)",
                               (record.record_id, record.version, record.state, payload, record.fingerprint, utc_now()))
            self.append_event(connection, "record.saved", record.record_id, record.version,
                              {"state": record.state, "fingerprint": record.fingerprint})
            return record.fingerprint

    def get_record(self, record_id: str) -> DomainRecord | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE record_id=?", (record_id,)).fetchone()
        if not row:
            return None
        return DomainRecord(record_id=row["record_id"], payload=json.loads(row["payload_json"]),
                            state=row["state"], version=row["version"], occurred_at=row["occurred_at"],
                            source=row["source"])

    def list_records(self, state: str | None = None) -> list[DomainRecord]:
        query = "SELECT * FROM records"
        params: tuple[Any, ...] = ()
        if state:
            query += " WHERE state=?"
            params = (state,)
        query += " ORDER BY record_id"
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [DomainRecord(record_id=row["record_id"], payload=json.loads(row["payload_json"]),
                             state=row["state"], version=row["version"], occurred_at=row["occurred_at"],
                             source=row["source"]) for row in rows]

    def save_decision(self, decision: Decision) -> str:
        decision_id = decision.fingerprint
        with self.transaction() as connection:
            if not connection.execute("SELECT 1 FROM records WHERE record_id=?", (decision.record_id,)).fetchone():
                raise ContractError("decision record does not exist")
            body = canonical_json(decision.as_dict())
            connection.execute("INSERT OR IGNORE INTO decisions(decision_id,record_id,outcome,score,decision_json,decided_at) VALUES(?,?,?,?,?,?)",
                               (decision_id, decision.record_id, decision.outcome, decision.score, body, decision.decided_at))
            self.append_event(connection, "decision.saved", decision.record_id,
                              self.event_count(decision.record_id, connection) + 1,
                              {"decision_id": decision_id, "outcome": decision.outcome, "score": decision.score})
        return decision_id

    def event_count(self, aggregate_id: str, connection: sqlite3.Connection | None = None) -> int:
        owns = connection is None
        connection = connection or self.connect()
        try:
            row = connection.execute("SELECT COUNT(*) FROM events WHERE aggregate_id=?", (aggregate_id,)).fetchone()
            return int(row[0])
        finally:
            if owns:
                connection.close()

    def begin_run(self, fixture_hash: str, record_count: int) -> str:
        run_id = stable_hash({"fixture_hash": fixture_hash, "record_count": record_count})[:24]
        with self.transaction() as connection:
            row = connection.execute("SELECT fixture_hash,record_count FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row and (row[0] != fixture_hash or row[1] != record_count):
                raise ContractError("run identifier collision")
            connection.execute("INSERT OR IGNORE INTO runs(run_id,fixture_hash,status,record_count,result_json,started_at) VALUES(?,?,?,?,?,?)",
                               (run_id, fixture_hash, "running", record_count, "{}", utc_now()))
        return run_id

    def completed_run(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT status,result_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["status"] != "finished":
            return None
        try:
            result = json.loads(row["result_json"])
        except (TypeError, ValueError) as exc:
            raise ContractError("stored run result is invalid") from exc
        if not isinstance(result, dict):
            raise ContractError("stored run result is invalid")
        return result

    def interrupted_run_evidence(self, run_id: str, records: Sequence[DomainRecord]) -> list[str]:
        """Report whether an earlier attempt already advanced this run's grading.

        A run that never froze its delivery cannot be resumed safely: re-running
        the decision sequence would advance batches a second time.  Detecting
        that evidence lets the caller refuse instead of changing conclusions.
        """
        with self.connect() as connection:
            row = connection.execute("SELECT status,started_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != "running":
                return []
            started_at = row["started_at"]
            stored = {item["record_id"]: item for item in connection.execute(
                "SELECT record_id,version,state,payload_json,updated_at FROM records WHERE record_id IN (%s)"
                % ",".join("?" * len(records)), [record.record_id for record in records])}
            decided_at = {item[0] for item in connection.execute(
                "SELECT DISTINCT record_id FROM decisions WHERE decided_at>=? AND record_id IN (%s)"
                % ",".join("?" * len(records)), [started_at, *(record.record_id for record in records)])}
        evidence: list[str] = []
        for record in records:
            current = stored.get(record.record_id)
            if current is None:
                continue
            advanced = (current["version"] != record.version or current["state"] != record.state
                        or json.loads(current["payload_json"]) != record.payload)
            if advanced and current["updated_at"] >= started_at:
                evidence.append(f"record {record.record_id} was advanced by the interrupted attempt")
            if record.record_id in decided_at:
                evidence.append(f"record {record.record_id} already has a decision from the interrupted attempt")
        return evidence

    def finish_run(self, run_id: str, result: Mapping[str, Any], artifacts: Mapping[str, Any] | None = None) -> None:
        """Mark the run finished and freeze its delivery material atomically."""
        body = None
        delivery_hash = None
        if artifacts is not None:
            body = canonical_json(dict(artifacts))
            delivery_hash = delivery_fingerprint(artifacts)
        with self.transaction() as connection:
            row = connection.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is not None and row["status"] == "finished":
                if artifacts is not None:
                    self._check_delivery_unchanged(connection, run_id, delivery_hash)
                return
            changed = connection.execute("UPDATE runs SET status='finished',result_json=?,finished_at=? WHERE run_id=?",
                                         (canonical_json(result), utc_now(), run_id)).rowcount
            if changed != 1:
                raise ContractError("unknown run")
            self.append_event(connection, "run.finished", run_id, 1, dict(result))
            if body is not None:
                self._insert_delivery(connection, run_id, artifacts, delivery_hash, body)

    def _check_delivery_unchanged(self, connection: sqlite3.Connection, run_id: str, delivery_hash: str) -> None:
        row = connection.execute("SELECT status,delivery_hash FROM deliveries WHERE run_id=?", (run_id,)).fetchone()
        if row is None or row["delivery_hash"] != delivery_hash or row["status"] != "ready":
            raise ContractError("delivery material for this run is already recorded and cannot be replaced")

    def _insert_delivery(self, connection: sqlite3.Connection, run_id: str,
                         artifacts: Mapping[str, Any], delivery_hash: str, body: str) -> None:
        connection.execute("INSERT INTO deliveries(run_id,status,delivery_hash,artifacts_json,created_at) VALUES(?,?,?,?,?)",
                           (run_id, "ready", delivery_hash, body, utc_now()))
        self.append_event(connection, "delivery.recorded", run_id, 1,
                          {"delivery_hash": delivery_hash,
                           "manifest_hash": artifacts.get("manifest", {}).get("manifest_hash")})

    def save_delivery(self, run_id: str, artifacts: Mapping[str, Any]) -> bool:
        """Freeze the deliverable material of a processing run exactly once.

        The stored copy is what later deliveries are restored from, so a
        database that was revised after the run can never masquerade as the
        original result.  A second identical save is idempotent; any attempt
        to replace the material of a finished run is rejected.
        """
        body = canonical_json(dict(artifacts))
        delivery_hash = delivery_fingerprint(artifacts)
        with self.transaction() as connection:
            row = connection.execute("SELECT status,delivery_hash FROM deliveries WHERE run_id=?",
                                     (run_id,)).fetchone()
            if row is not None:
                if row["delivery_hash"] == delivery_hash and row["status"] == "ready":
                    return False
                raise ContractError("delivery material for this run is already recorded and cannot be replaced")
            run = connection.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if run is None:
                raise ContractError("delivery material requires an existing run")
            self._insert_delivery(connection, run_id, artifacts, delivery_hash, body)
        return True

    def _delivery_row(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT status,delivery_hash,artifacts_json FROM deliveries WHERE run_id=?",
                                     (run_id,)).fetchone()
        if row is None:
            return None
        try:
            artifacts = json.loads(row["artifacts_json"])
        except (TypeError, ValueError) as exc:
            return {"status": "corrupt", "delivery_hash": row["delivery_hash"], "artifacts": None,
                    "errors": [f"stored delivery material is unreadable: {exc}"]}
        return {"status": row["status"], "delivery_hash": row["delivery_hash"], "artifacts": artifacts}

    def delivery_material(self, run_id: str) -> tuple[dict[str, Any], list[str]]:
        """Return trustworthy frozen delivery material for a run.

        Partial writes, damaged rows and material that no longer matches its
        recorded hash are reported instead of being used or silently ignored.
        The classification never depends on the current contents of the live
        records, decisions or events tables.
        """
        row = self._delivery_row(run_id)
        if row is None:
            raise ContractError("no frozen delivery material exists for this run")
        if row.get("artifacts") is None:
            raise ContractError("; ".join(row.get("errors") or ["delivery material is unreadable"]))
        artifacts = row["artifacts"]
        errors = verify_artifacts(artifacts)
        if row["delivery_hash"] != artifacts.get("delivery_hash"):
            errors.append("stored delivery hash disagrees with the delivery material")
        if row["status"] != "ready":
            errors.append("delivery material is not marked ready")
        if errors:
            raise ContractError("frozen delivery material failed verification: " + "; ".join(errors))
        return artifacts, []

    def delivery_report(self, run_id: str) -> dict[str, Any] | None:
        row = self._delivery_row(run_id)
        if row is None:
            return None
        material_errors: list[str] = []
        if row.get("artifacts") is None:
            material_errors = row.get("errors") or ["delivery material is unreadable"]
        else:
            material_errors = verify_artifacts(row["artifacts"])
            if row["delivery_hash"] != row["artifacts"].get("delivery_hash"):
                material_errors.append("stored delivery hash disagrees with the delivery material")
        if row["status"] != "ready":
            material_errors.append("delivery material is not marked ready")
        return {"run_id": run_id, "status": row["status"], "delivery_hash": row["delivery_hash"],
                "trusted": not material_errors, "errors": material_errors}

    def verify_chain(self) -> list[str]:
        errors: list[str] = []
        previous = "0" * 64
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except (TypeError, ValueError):
                errors.append(f"event {row['event_id']} payload is unreadable")
                previous = row["event_hash"]
                continue
            expected = stable_hash({"event_type": row["event_type"], "aggregate_id": row["aggregate_id"],
                                    "aggregate_version": row["aggregate_version"], "payload": payload,
                                    "previous_hash": row["previous_hash"], "created_at": row["created_at"]})
            if row["previous_hash"] != previous:
                errors.append(f"event {row['event_id']} previous hash mismatch")
            if row["event_hash"] != expected:
                errors.append(f"event {row['event_id']} content hash mismatch")
            previous = row["event_hash"]
        return errors

    def verify(self, output: str | Path) -> dict[str, Any]:
        """Cross-check the database, the delivery in ``output`` and the frozen material."""
        errors = self.verify_chain()
        bundle_errors = verify_bundle(output)
        delivery = None
        manifest = None
        try:
            manifest = json.loads((Path(output) / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = None
        if isinstance(manifest, dict) and manifest.get("run_id"):
            delivery = self.delivery_report(str(manifest["run_id"]))
            if delivery is None:
                delivery = {"run_id": manifest["run_id"], "status": "missing", "trusted": False,
                            "delivery_hash": manifest.get("delivery_hash"),
                            "errors": ["no frozen delivery material exists for this run"]}
            elif delivery["delivery_hash"] != manifest.get("delivery_hash"):
                delivery = dict(delivery) | {"trusted": False,
                                             "errors": list(delivery["errors"]) + ["delivery belongs to another processing run"]}
        delivery_ok = True if delivery is None else bool(delivery.get("trusted"))
        return {"ok": not errors and not bundle_errors and delivery_ok,
                "chain_errors": errors, "bundle_errors": bundle_errors,
                "delivery": delivery, "snapshot": self.snapshot()}

    def snapshot(self) -> dict[str, Any]:
        with self.connect() as connection:
            counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                      for table in ("records", "record_versions", "decisions", "events", "runs")}
            states = {row[0]: row[1] for row in connection.execute("SELECT state,COUNT(*) FROM records GROUP BY state")}
        return {"counts": counts, "states": states, "chain_errors": self.verify_chain()}

    def decision_rows(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT decision_json FROM decisions ORDER BY record_id,decided_at").fetchall()
        return [json.loads(row[0]) for row in rows]

    def event_rows(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        return [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows]
