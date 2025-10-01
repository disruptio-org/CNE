"""Helpers to compute per-document processing progress for the dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
from typing import Dict, Iterable, List, Mapping

StageName = str

STAGE_ORDER: tuple[StageName, ...] = (
    "ingest",
    "ocr",
    "operator_a",
    "operator_b",
    "match",
    "review",
    "approve",
    "export",
)


@dataclass(slots=True)
class StageMetrics:
    """Representation of a single stage snapshot."""

    state: str
    label: str
    metrics: List[Mapping[str, object]] = field(default_factory=list)
    updated_at: str | None = None

    def as_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "state": self.state,
            "label": self.label,
        }
        if self.metrics:
            payload["metrics"] = list(self.metrics)
        if self.updated_at:
            payload["updated_at"] = self.updated_at
        return payload


def fetch_document_progress(db_path: str | Path) -> List[Dict[str, object]]:
    """Return dashboard-friendly progress information for all documents."""

    db_path = Path(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        documents = list(conn.execute("SELECT * FROM documents ORDER BY created_at DESC"))

    progress_rows: List[Dict[str, object]] = []
    for row in documents:
        stages = _build_stage_snapshots(db_path, row)
        completion_ratio = _compute_completion_ratio(stages.values())
        status_value = str(row["status"]).upper()
        status_state = "pending"
        if status_value == "APPROVED":
            status_state = "completed"
        elif status_value in {"OCR_DONE", "PROCESSED"}:
            status_state = "in_progress"
        elif status_value == "FAILED":
            status_state = "in_progress"

        progress_rows.append(
            {
                "id": row["id"],
                "file_name": row["file_name"],
                "detected_type": row["detected_type"],
                "status": status_value,
                "status_state": status_state,
                "completion": completion_ratio,
                "stages": {name: metrics.as_dict() for name, metrics in stages.items()},
            }
        )
    return progress_rows


def _build_stage_snapshots(db_path: Path, document_row: sqlite3.Row) -> Dict[StageName, StageMetrics]:
    stages: Dict[StageName, StageMetrics] = {}
    for name in STAGE_ORDER:
        stages[name] = StageMetrics(state="pending", label=name.replace("_", " ").title())

    stages["ingest"] = StageMetrics(
        state="completed",
        label="Ingested",
        metrics=[
            {
                "label": "File size",
                "value": f"{document_row['file_size']} bytes",
                "description": f"Uploaded on {document_row['created_at']}",
                "state": "completed",
            }
        ],
        updated_at=document_row["created_at"],
    )

    ocr_state, ocr_metrics = _ocr_metrics(document_row)
    stages["ocr"] = StageMetrics(
        state=ocr_state,
        label="OCR",
        metrics=ocr_metrics,
        updated_at=document_row["ocr_completed_at"] or document_row["ocr_started_at"],
    )

    stages["operator_a"] = _operator_metrics(db_path, document_row["id"], "operator_a_results", "Operator A")
    stages["operator_b"] = _operator_metrics(db_path, document_row["id"], "operator_b_results", "Operator B")

    stages["match"] = _match_metrics(db_path, document_row["id"])
    stages["review"] = _review_metrics(db_path, document_row["id"])
    stages["approve"] = _approval_metrics(document_row)
    stages["export"] = _export_metrics(db_path, document_row["id"], stages["approve"].state)

    return stages


def _ocr_metrics(row: sqlite3.Row) -> tuple[str, List[Mapping[str, object]]]:
    metrics: List[Mapping[str, object]] = []
    state = "pending"
    if row["ocr_completed_at"]:
        state = "completed"
    elif row["ocr_started_at"]:
        state = "in_progress"
    else:
        state = "pending"

    metrics.append(
        {
            "label": "Detected type",
            "value": row["detected_type"],
            "state": "completed" if state == "completed" else "pending",
        }
    )
    if row["ocr_text_path"]:
        metrics.append(
            {
                "label": "Transcript",
                "value": row["ocr_text_path"],
                "state": "completed" if state == "completed" else state,
            }
        )
    return state, metrics


def _operator_metrics(db_path: Path, document_id: int, table: str, label: str) -> StageMetrics:
    query = f"""
        SELECT COUNT(*) AS total, MAX(created_at) AS latest
        FROM {table}
        WHERE document_id = ?
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(query, (document_id,))
        total, latest = cursor.fetchone()

    state = "completed" if total else "pending"
    metrics = [
        {
            "label": "Rows",
            "value": int(total),
            "state": state,
            "description": "Total extracted rows stored for this document.",
        }
    ]
    return StageMetrics(state=state, label=label, metrics=metrics, updated_at=latest)


def _match_metrics(db_path: Path, document_id: int) -> StageMetrics:
    query = """
        SELECT status, COUNT(*) as total
        FROM candidate_comparisons
        WHERE document_id = ?
        GROUP BY status
    """
    totals: Dict[str, int] = {}
    latest: str | None = None
    with sqlite3.connect(db_path) as conn:
        for status, count in conn.execute(query, (document_id,)):
            totals[str(status)] = int(count)
        cursor = conn.execute(
            "SELECT MAX(created_at) FROM candidate_comparisons WHERE document_id = ?",
            (document_id,),
        )
        (latest,) = cursor.fetchone()

    total_rows = sum(totals.values())
    disputes = totals.get("dispute", 0)
    state = "completed" if total_rows else "pending"
    if disputes:
        state = "in_progress"

    metrics = [
        {
            "label": "Total rows",
            "value": total_rows,
            "state": "completed" if total_rows else "pending",
            "description": "Comparison rows available for review.",
        },
        {
            "label": "Disputes",
            "value": disputes,
            "state": "in_progress" if disputes else "completed",
            "description": "Rows requiring manual review.",
        },
    ]
    return StageMetrics(state=state, label="Match", metrics=metrics, updated_at=latest)


def _review_metrics(db_path: Path, document_id: int) -> StageMetrics:
    query = """
        SELECT COUNT(*) FROM review_decisions WHERE document_id = ?
    """
    with sqlite3.connect(db_path) as conn:
        (total_decisions,) = conn.execute(query, (document_id,)).fetchone()
        cursor = conn.execute(
            "SELECT MAX(decided_at) FROM review_decisions WHERE document_id = ?",
            (document_id,),
        )
        (latest,) = cursor.fetchone()

    state = "completed" if total_decisions else "pending"
    metrics = [
        {
            "label": "Decisions",
            "value": int(total_decisions),
            "state": state,
            "description": "Reviewer decisions captured for this document.",
        }
    ]
    return StageMetrics(state=state, label="Review", metrics=metrics, updated_at=latest)


def _approval_metrics(row: sqlite3.Row) -> StageMetrics:
    status = str(row["status"]).upper()
    state = "completed" if status == "APPROVED" else "pending"
    metrics = [
        {
            "label": "Status",
            "value": status,
            "state": state,
            "description": "Current document lifecycle status.",
        }
    ]
    return StageMetrics(state=state, label="Approve", metrics=metrics)


def _export_metrics(db_path: Path, document_id: int, approval_state: str) -> StageMetrics:
    action_query = """
        SELECT MAX(created_at) FROM audit_log
        WHERE document_id = ? AND action LIKE 'export%'
    """
    with sqlite3.connect(db_path) as conn:
        (latest,) = conn.execute(action_query, (document_id,)).fetchone()

    if approval_state != "completed":
        state = "pending"
        label = "Export (awaiting approval)"
    elif latest:
        state = "completed"
        label = "Exported"
    else:
        state = "ready"
        label = "Ready to export"

    metrics = [
        {
            "label": "Last export",
            "value": latest or "Not exported",
            "state": state if latest else "pending",
            "description": "Timestamp of the most recent export bundle for this document.",
        }
    ]
    return StageMetrics(state=state, label=label, metrics=metrics, updated_at=latest)


def _compute_completion_ratio(stages: Iterable[StageMetrics]) -> float:
    stage_list = list(stages)
    if not stage_list:
        return 0.0
    completed = sum(1 for stage in stage_list if stage.state == "completed")
    total = len(stage_list)
    return completed / total if total else 0.0


__all__ = ["fetch_document_progress", "STAGE_ORDER"]
