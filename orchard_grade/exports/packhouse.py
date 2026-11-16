from __future__ import annotations

import contextlib
import csv
import hashlib
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping

from orchard_grade.contracts.batches import ContractError, Decision, DomainRecord, stable_hash, utc_now


RECORD_FIELDS = ["record_id", "state", "version", "source", "fingerprint", "outcome", "score"]
RULE_FIELDS = ["record_id", "code", "passed", "score", "severity", "message"]
JSON_FILES = ("summary.json", "decisions.json", "audit-events.json")
CSV_FILES = ("records.csv", "rules.csv")
#: Every value that participates in the immutable delivery fingerprint.
DELIVERY_KEYS = ("run_id", "fixture_hash", "record_rows", "rule_rows",
                 "summary", "decisions", "events", "snapshot")


def ensure_output(path: str | Path) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output


def _json_body(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _record_rows(records: Iterable[DomainRecord], decisions: Mapping[str, Decision]) -> list[dict[str, Any]]:
    rows = []
    for record in records:
        decision = decisions.get(record.record_id)
        rows.append({"record_id": record.record_id, "state": record.state, "version": record.version,
                     "source": record.source, "fingerprint": record.fingerprint,
                     "outcome": decision.outcome if decision else "", "score": decision.score if decision else ""})
    return rows


def _rule_rows(decisions: Mapping[str, Decision]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record_id, decision in sorted(decisions.items()):
        for result in decision.rule_results:
            rows.append({"record_id": record_id, "code": result.code, "passed": result.passed,
                         "score": result.score, "severity": result.severity, "message": result.message})
    return rows


def render_records_csv(rows: Iterable[Mapping[str, Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=RECORD_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def render_rules_csv(rows: Iterable[Mapping[str, Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=RULE_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def build_summary(records: list[DomainRecord], decisions: Mapping[str, Decision], snapshot: Mapping[str, Any]) -> dict[str, Any]:
    outcomes: dict[str, int] = {}
    for decision in decisions.values():
        outcomes[decision.outcome] = outcomes.get(decision.outcome, 0) + 1
    scores = [decision.score for decision in decisions.values()]
    return {"product": '果品智能分选质量闭环平台', "entity": 'fruit_lot', "generated_at": utc_now(),
            "record_count": len(records), "decision_count": len(decisions), "outcomes": outcomes,
            "average_score": sum(scores) / len(scores) if scores else 0.0,
            "minimum_score": min(scores) if scores else 0.0, "maximum_score": max(scores) if scores else 0.0,
            "repository": snapshot}


def delivery_fingerprint(artifacts: Mapping[str, Any]) -> str:
    """Fingerprint of the immutable material a delivery is reproduced from."""
    return stable_hash({key: artifacts[key] for key in DELIVERY_KEYS})


def artifact_file_hashes(artifacts: Mapping[str, Any]) -> dict[str, str]:
    """Return the digest of every file produced by a delivery artifact bundle."""
    return {
        "summary.json": stable_hash(artifacts["summary"]),
        "decisions.json": stable_hash(artifacts["decisions"]),
        "audit-events.json": stable_hash(artifacts["events"]),
        "records.csv": _sha256_bytes(render_records_csv(artifacts["record_rows"]).encode("utf-8")),
        "rules.csv": _sha256_bytes(render_rules_csv(artifacts["rule_rows"]).encode("utf-8")),
    }


def prepare_export(run_id: str, fixture_hash: str, records: list[DomainRecord],
                   decisions: Mapping[str, Decision], snapshot: Mapping[str, Any],
                   events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the immutable material a delivery is reproduced from.

    The bundle contains the statistics, the final batch rows and the audit
    evidence as they existed when the processing run completed.  ``run_id``
    and the audit chain bind these materials to one specific processing run.
    """
    summary = build_summary(records, decisions, snapshot)
    record_rows = _record_rows(records, decisions)
    rule_rows = _rule_rows(decisions)
    decision_values = [value.as_dict() for _, value in sorted(decisions.items())]
    artifacts: dict[str, Any] = {
        "run_id": run_id,
        "fixture_hash": fixture_hash,
        "record_rows": record_rows,
        "rule_rows": rule_rows,
        "summary": summary,
        "decisions": decision_values,
        "events": events,
        "snapshot": snapshot,
    }
    delivery_hash = delivery_fingerprint(artifacts)
    artifacts["delivery_hash"] = delivery_hash
    manifest = {
        "run_id": run_id,
        "fixture_hash": fixture_hash,
        "delivery_hash": delivery_hash,
        "files": {
            "summary.json": stable_hash(summary),
            "decisions.json": stable_hash(decision_values),
            "audit-events.json": stable_hash(events),
        },
        "csv_files": {
            "records.csv": _sha256_bytes(render_records_csv(record_rows).encode("utf-8")),
            "rules.csv": _sha256_bytes(render_rules_csv(rule_rows).encode("utf-8")),
        },
        "record_rows": len(record_rows),
        "rule_rows": len(rule_rows),
        "created_at": utc_now(),
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    artifacts["manifest"] = manifest
    return artifacts


def verify_artifacts(artifacts: Any) -> list[str]:
    """Check that a stored artifact bundle is internally complete and untouched."""
    if not isinstance(artifacts, Mapping):
        return ["delivery artifacts are missing"]
    errors: list[str] = []
    missing = [key for key in DELIVERY_KEYS if key not in artifacts]
    if missing:
        return [f"delivery artifact is missing: {key}" for key in missing]
    if not isinstance(artifacts.get("snapshot"), Mapping) or not isinstance(artifacts.get("summary"), Mapping):
        return ["delivery artifacts are malformed"]
    try:
        expected_delivery = delivery_fingerprint(artifacts)
    except (TypeError, ValueError, KeyError) as exc:
        return [f"delivery artifacts cannot be hashed: {exc}"]
    if artifacts.get("delivery_hash") != expected_delivery:
        errors.append("delivery artifact hash mismatch")
    summary = artifacts["summary"]
    snapshot = artifacts["snapshot"]
    if summary.get("repository") != snapshot:
        errors.append("delivery summary is not backed by its repository snapshot")
    if summary.get("record_count") != len(artifacts.get("record_rows") or []):
        errors.append("delivery summary record count disagrees with the batch rows")
    if summary.get("decision_count") != len(artifacts.get("decisions") or []):
        errors.append("delivery summary decision count disagrees with the decisions")
    if len(artifacts.get("decisions") or []) != len(artifacts.get("record_rows") or []):
        errors.append("delivery decisions do not cover every batch row")
    if snapshot.get("chain_errors"):
        errors.append("delivery audit evidence contains chain errors")
    manifest = artifacts.get("manifest")
    if not isinstance(manifest, Mapping):
        errors.append("delivery manifest is missing")
        return errors
    body = {key: value for key, value in manifest.items() if key != "manifest_hash"}
    try:
        if stable_hash(body) != manifest.get("manifest_hash"):
            errors.append("delivery manifest hash mismatch")
        expected_files = artifact_file_hashes(artifacts)
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        errors.append(f"delivery artifacts cannot be verified: {exc}")
        return errors
    if manifest.get("run_id") != artifacts.get("run_id"):
        errors.append("delivery manifest belongs to another processing run")
    if manifest.get("fixture_hash") != artifacts.get("fixture_hash"):
        errors.append("delivery manifest fixture hash mismatch")
    if manifest.get("delivery_hash") != artifacts.get("delivery_hash"):
        errors.append("delivery manifest delivery hash mismatch")
    if manifest.get("files") != {name: expected_files[name] for name in JSON_FILES}:
        errors.append("delivery manifest JSON file hashes disagree with the artifacts")
    if manifest.get("csv_files") != {name: expected_files[name] for name in CSV_FILES}:
        errors.append("delivery manifest CSV file hashes disagree with the artifacts")
    if manifest.get("record_rows") != len(artifacts.get("record_rows") or []):
        errors.append("delivery manifest record row count mismatch")
    if manifest.get("rule_rows") != len(artifacts.get("rule_rows") or []):
        errors.append("delivery manifest rule row count mismatch")
    return errors


def write_bundle(output: str | Path, artifacts: Mapping[str, Any]) -> dict[str, Any]:
    return install_bundle(output, artifacts)["_manifest"]


def _atomic_write(path: Path, body: bytes) -> None:
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _existing_manifest_state(path: Path, expected_hash: Any) -> str:
    manifest = read_manifest(path)
    if manifest is None:
        return "corrupt" if path.exists() else "missing"
    return "trusted" if manifest.get("manifest_hash") == expected_hash else "corrupt"


def _existing_file_state(path: Path, expected: str, name: str) -> str:
    if not path.is_file():
        return "missing"
    try:
        if name in JSON_FILES:
            value = json.loads(path.read_text(encoding="utf-8"))
            return "trusted" if stable_hash(value) == expected else "corrupt"
        return "trusted" if _sha256_bytes(path.read_bytes()) == expected else "corrupt"
    except (OSError, ValueError, TypeError):
        return "corrupt"


def _output_lock(root: Path):
    """Serialise concurrent deliveries to the same directory across processes."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - non-POSIX platforms
        return contextlib.nullcontext()
    lock_path = root.parent / f".{root.name}.delivery.lock"
    handle = open(lock_path, "a+b")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return contextlib.closing(handle)


def read_manifest(path: str | Path) -> dict[str, Any] | None:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def assert_output_available(output: str | Path, run_id: str) -> None:
    """Refuse to overwrite a complete delivery that belongs to another run."""
    existing = read_manifest(Path(output) / "manifest.json")
    if isinstance(existing, dict) and existing.get("run_id") not in (None, run_id):
        raise ContractError("output directory belongs to a different processing run")


def install_bundle(output: str | Path, artifacts: Mapping[str, Any]) -> dict[str, Any]:
    """Write every delivery file from the artifact bundle and verify the result.

    Files are written atomically and the manifest is written last, so an
    interrupted delivery is always recognisable and can be restored in full.
    The returned report distinguishes material that was already trustworthy
    from material that had to be rebuilt.
    """
    errors = verify_artifacts(artifacts)
    if errors:
        raise ContractError("delivery artifacts are not trustworthy: " + "; ".join(errors))
    run_id = str(artifacts["run_id"])
    root = ensure_output(output)
    with _output_lock(root):
        assert_output_available(root, run_id)
        expected = artifact_file_hashes(artifacts)
        states = {name: _existing_file_state(root / name, digest, name) for name, digest in expected.items()}
        manifest = dict(artifacts["manifest"])
        states["manifest.json"] = _existing_manifest_state(root / "manifest.json", manifest.get("manifest_hash"))
        expected["manifest.json"] = str(manifest.get("manifest_hash"))

        bodies: dict[str, bytes] = {
            "records.csv": render_records_csv(artifacts["record_rows"]).encode("utf-8"),
            "rules.csv": render_rules_csv(artifacts["rule_rows"]).encode("utf-8"),
            "summary.json": _json_body(artifacts["summary"]).encode("utf-8"),
            "decisions.json": _json_body(artifacts["decisions"]).encode("utf-8"),
            "audit-events.json": _json_body(artifacts["events"]).encode("utf-8"),
        }
        for name in CSV_FILES + JSON_FILES:
            _atomic_write(root / name, bodies[name])
        _atomic_write(root / "manifest.json", _json_body(manifest).encode("utf-8"))

        verification = verify_bundle(root)
        if verification:
            raise ContractError("delivery verification failed: " + "; ".join(verification))

    repaired = sorted(name for name, state in states.items() if state == "corrupt")
    report = {
        "run_id": run_id,
        "delivery_hash": artifacts["delivery_hash"],
        "manifest_hash": manifest.get("manifest_hash"),
        "ok": True,
        "files": {name: {"hash": expected[name], "trusted": True} for name in sorted(expected)},
        "trusted_files": sorted(name for name, state in states.items() if state == "trusted"),
        "created_files": sorted(name for name, state in states.items() if state == "missing"),
        "repaired_files": repaired,
        "needs_reverification": repaired,
        "verified_files": sorted(expected),
        "_manifest": manifest,
    }
    return report


def export_bundle(output: str | Path, records: list[DomainRecord], decisions: Mapping[str, Decision],
                  snapshot: Mapping[str, Any], events: list[dict[str, Any]],
                  run_id: str = "", fixture_hash: str = "") -> dict[str, Any]:
    if not run_id:
        run_id = stable_hash({"records": [record.fingerprint for record in records]})[:24]
    if not fixture_hash:
        fixture_hash = stable_hash({"record_ids": [record.record_id for record in records]})
    artifacts = prepare_export(run_id, fixture_hash, records, decisions, snapshot, events)
    return write_bundle(output, artifacts)


def verify_bundle(output: str | Path) -> list[str]:
    root = Path(output)
    errors: list[str] = []
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        return ["manifest.json is missing"]
    manifest = read_manifest(manifest_path)
    if manifest is None:
        return ["manifest.json is invalid"]
    body = {key: value for key, value in manifest.items() if key != "manifest_hash"}
    try:
        if stable_hash(body) != manifest.get("manifest_hash"):
            errors.append("manifest hash mismatch")
    except (TypeError, ValueError) as exc:
        errors.append(f"manifest cannot be hashed: {exc}")
    files = manifest.get("files")
    if not isinstance(files, Mapping):
        errors.append("manifest file list is invalid")
        files = {}
    for name, expected in files.items():
        path = root / name
        if not path.is_file():
            errors.append(f"{name} is missing")
            continue
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            errors.append(f"{name} is invalid")
            continue
        if stable_hash(value) != expected:
            errors.append(f"{name} hash mismatch")
    csv_files = manifest.get("csv_files")
    if isinstance(csv_files, Mapping):
        for name, expected in csv_files.items():
            path = root / name
            if not path.is_file():
                errors.append(f"{name} is missing")
                continue
            try:
                digest = _sha256_bytes(path.read_bytes())
            except OSError:
                errors.append(f"{name} cannot be read")
                continue
            if digest != expected:
                errors.append(f"{name} hash mismatch")
    return errors
