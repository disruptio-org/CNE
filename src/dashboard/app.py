"""FastAPI application powering the processing dashboard."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from exporter import CsvExporter
from ingestion.service import IngestionService
from matching.comparator import CandidateComparator
from ocr.pipeline import OcrPipeline
from operators.operator_a import OperatorA
from operators.operator_b import OperatorB
from review.service import ReviewService

from .progress import fetch_document_progress

FRONTEND_PREFIX = "/app"


class StagePayload(BaseModel):
    approver_id: str | None = None
    summary: str | None = None
    status: str | None = None
    output_dir: str | None = None


class PipelineOrchestrator:
    """Wrapper responsible for executing individual processing stages."""

    def __init__(self, db_path: Path | str = Path("data/documents.db")) -> None:
        self.db_path = Path(db_path)
        self.ingestion = IngestionService(db_path=self.db_path)
        self.ocr = OcrPipeline(db_path=self.db_path)
        self.operator_a = OperatorA(db_path=self.db_path)
        self.operator_b = OperatorB(db_path=self.db_path)
        self.comparator = CandidateComparator(db_path=self.db_path)
        self.review = ReviewService(db_path=self.db_path)

    # ------------------------------------------------------------------
    # Stage runners
    # ------------------------------------------------------------------
    def run_stage(self, stage: str, document_id: int, payload: StagePayload) -> Dict[str, Any]:
        stage = stage.lower()
        if stage == "ingest":
            record = self._get_document(document_id)
            return {"message": "Ingestion metadata refreshed.", "details": self._document_details(record)}
        if stage == "ocr":
            record = self.ocr.run_for_document(document_id)
            return {"message": "OCR pipeline executed.", "details": self._document_details(record)}
        if stage == "operator_a":
            rows = self._run_operator(self.operator_a, document_id)
            return {"message": f"Operator A stored {len(rows)} rows.", "details": self._simple_metric("Rows", len(rows))}
        if stage == "operator_b":
            rows = self._run_operator(self.operator_b, document_id)
            return {"message": f"Operator B stored {len(rows)} rows.", "details": self._simple_metric("Rows", len(rows))}
        if stage == "match":
            comparisons = self._run_comparator(document_id)
            return {
                "message": f"{len(comparisons)} comparison rows generated.",
                "details": self._simple_metric("Comparisons", len(comparisons)),
            }
        if stage == "review":
            status_filter = payload.status or None
            comparisons = self.review.fetch_comparisons(document_id, status=status_filter)
            metrics = self._simple_metric(
                "Rows fetched",
                len(comparisons),
                description="Rows available for reviewer inspection.",
            )
            return {
                "message": f"Fetched {len(comparisons)} comparison rows for review.",
                "details": metrics,
            }
        if stage == "approve":
            approver = (payload.approver_id or "dashboard").strip()
            if not approver:
                raise ValueError("An approver identifier is required.")
            self.review.approve_document(document_id=document_id, approver_id=approver, summary=payload.summary)
            return {
                "message": f"Document {document_id} marked as approved.",
                "details": self._simple_metric("Approver", approver),
            }
        if stage == "export":
            exporter = CsvExporter(db_path=self.db_path, output_dir=payload.output_dir)
            result = exporter.export()
            self._record_export_event(document_id)
            stats = result.stats
            mtime = datetime.fromtimestamp(result.csv_path.stat().st_mtime, timezone.utc).isoformat()
            metrics = {
                "updated_at": mtime,
                "metrics": [
                    {
                        "label": "CSV path",
                        "value": str(result.csv_path),
                        "state": "completed",
                    },
                    {
                        "label": "QA report",
                        "value": str(result.qa_path),
                        "state": "completed",
                    },
                    {
                        "label": "Rows exported",
                        "value": stats.get("rows", 0),
                        "state": "completed" if stats.get("rows") else "pending",
                    },
                ],
            }
            return {
                "message": "Export bundle generated successfully.",
                "details": metrics,
            }
        raise ValueError(f"Unsupported stage {stage!r}.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _run_operator(self, operator: Any, document_id: int) -> list:
        record = self._get_document(document_id)
        text_path = record.ocr_text_path
        if text_path:
            text_file = Path(text_path)
            if not text_file.is_absolute():
                text_file = (self.db_path.parent / text_file).resolve()
            text = text_file.read_text(encoding="utf-8", errors="ignore")
        else:
            upload_path = self.db_path.parent / "uploads" / f"{record.file_hash}.txt"
            text = ""
            if upload_path.exists():
                text = upload_path.read_text(encoding="utf-8", errors="ignore")
        if not text:
            raise ValueError("No textual source available for operator execution.")
        rows = operator.run(
            document_id=document_id,
            text=text,
            dtmnfr=record.file_name,
            orgao="",
            sigla="",
            simbolo="",
            nome_lista=record.file_name,
        )
        return rows

    def _run_comparator(self, document_id: int):
        operator_a_rows = self._fetch_operator_rows("operator_a_results", document_id)
        operator_b_rows = self._fetch_operator_rows("operator_b_results", document_id)
        if not operator_a_rows and not operator_b_rows:
            raise ValueError("Operators must run before matching.")
        return self.comparator.compare(operator_a_rows, operator_b_rows)

    def _fetch_operator_rows(self, table: str, document_id: int):
        query = f"SELECT * FROM {table} WHERE document_id = ? ORDER BY tipo, num_ordem"
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(query, (document_id,))]
        return rows

    def _get_document(self, document_id: int):
        record = self.ingestion.get_document(document_id)
        if record is None:
            raise ValueError(f"Document {document_id} not found.")
        return record

    def _document_details(self, record: Any) -> Dict[str, Any]:
        return {
            "metrics": [
                {
                    "label": "Status",
                    "value": getattr(record.status, "value", str(record.status)),
                    "state": "completed",
                },
                {
                    "label": "Detected type",
                    "value": getattr(record.detected_type, "value", str(record.detected_type)),
                    "state": "completed",
                },
            ],
            "updated_at": getattr(record, "ocr_completed_at", None) or getattr(record, "created_at", None),
        }

    def _simple_metric(self, label: str, value: Any, *, description: str | None = None) -> Dict[str, Any]:
        return {
            "metrics": [
                {
                    "label": label,
                    "value": value,
                    "state": "completed" if value else "pending",
                    "description": description,
                }
            ]
        }

    def _record_export_event(self, document_id: int) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO audit_log (document_id, actor_id, action, summary, created_at)
                VALUES (?, ?, 'export_bundle', NULL, ?)
                """,
                (document_id, "dashboard", timestamp),
            )
            conn.commit()


def create_app(db_path: Path | str = Path("data/documents.db")) -> FastAPI:
    """Return a configured FastAPI application for the dashboard."""

    db_path = Path(db_path)
    orchestrator = PipelineOrchestrator(db_path=db_path)

    app = FastAPI(title="CNE Processing Console")

    app.include_router(orchestrator.ingestion.build_router())

    @app.get("/api/documents/progress")
    def documents_progress() -> JSONResponse:
        return JSONResponse(fetch_document_progress(db_path))

    @app.post("/api/documents/{document_id}/stages/{stage}")
    def run_stage(
        document_id: int,
        stage: str,
        payload: StagePayload | None = Body(default=None),
    ) -> JSONResponse:
        try:
            result = orchestrator.run_stage(stage, document_id, payload or StagePayload())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(result)

    frontend_dir = Path(__file__).resolve().parents[2] / "frontend"
    if frontend_dir.exists():
        app.mount(
            FRONTEND_PREFIX,
            StaticFiles(directory=frontend_dir, html=True),
            name="frontend",
        )

        index_path = frontend_dir / "index.html"

        @app.get("/", response_class=HTMLResponse, include_in_schema=False)
        def root() -> HTMLResponse:
            return HTMLResponse(index_path.read_text(encoding="utf-8"))

        @app.get("/sw.js", include_in_schema=False)
        def service_worker() -> RedirectResponse:
            return RedirectResponse(url=f"{FRONTEND_PREFIX}/sw.js")

    return app


__all__ = ["create_app", "PipelineOrchestrator"]
