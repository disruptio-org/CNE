import sqlite3
import sys
import types
from pathlib import Path

import pytest


if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")

    class _FastAPIStub:
        def __init__(self, *args, **kwargs):
            pass

    class _HTTPExceptionStub(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _APIRouterStub:
        def __init__(self, *args, **kwargs):
            self.routes = []

        def _record(self, method: str, path: str | None) -> None:
            self.routes.append((method, path))

        def get(self, path: str, *args, **kwargs):
            self._record("GET", path)

            def decorator(func):
                return func

            return decorator

        def post(self, path: str, *args, **kwargs):
            self._record("POST", path)

            def decorator(func):
                return func

            return decorator

    def _form_stub(default):
        return default

    fastapi_stub.Body = lambda *args, **kwargs: None
    fastapi_stub.FastAPI = _FastAPIStub
    fastapi_stub.HTTPException = _HTTPExceptionStub
    fastapi_stub.APIRouter = _APIRouterStub
    fastapi_stub.Form = _form_stub
    sys.modules["fastapi"] = fastapi_stub

    responses_stub = types.ModuleType("fastapi.responses")

    class _ResponseStub:
        def __init__(self, *args, **kwargs):
            pass

    responses_stub.HTMLResponse = _ResponseStub
    responses_stub.JSONResponse = _ResponseStub
    responses_stub.RedirectResponse = _ResponseStub
    responses_stub.FileResponse = _ResponseStub
    sys.modules["fastapi.responses"] = responses_stub
    fastapi_stub.responses = responses_stub

    staticfiles_stub = types.ModuleType("fastapi.staticfiles")

    class _StaticFilesStub:
        def __init__(self, *args, **kwargs):
            pass

    staticfiles_stub.StaticFiles = _StaticFilesStub
    sys.modules["fastapi.staticfiles"] = staticfiles_stub
    fastapi_stub.staticfiles = staticfiles_stub

if "pydantic" not in sys.modules:
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModelStub:
        pass

    pydantic_stub.BaseModel = _BaseModelStub
    sys.modules["pydantic"] = pydantic_stub

from fastapi import HTTPException

from dashboard.app import PipelineOrchestrator, StagePayload
from ingestion.service import DocumentStatus, DocumentType


class DummyOperator:
    def __init__(self):
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return ["ok"]


def test_run_operator_resolves_relative_transcript(tmp_path: Path):
    data_dir = tmp_path / "data"
    db_path = data_dir / "documents.db"

    orchestrator = PipelineOrchestrator(db_path=db_path)

    transcript_rel = Path("ocr") / "sample.txt"
    transcript_path = data_dir / transcript_rel
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    transcript_path.write_text("Hello world", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO documents (file_name, file_hash, file_size, detected_type, status, ocr_text_path)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "sample.pdf",
                "hash123",
                0,
                DocumentType.PDF_SCANNED.value,
                DocumentStatus.OCR_DONE.value,
                str(transcript_rel),
            ),
        )
        conn.commit()
        document_id = conn.execute(
            "SELECT id FROM documents WHERE file_hash = ?",
            ("hash123",),
        ).fetchone()[0]

    operator = DummyOperator()
    rows = orchestrator._run_operator(operator, document_id)

    assert rows == ["ok"]
    assert operator.calls[0]["text"] == "Hello world"


def test_run_stage_ocr_failure_raises_http_error(tmp_path: Path):
    orchestrator = PipelineOrchestrator(db_path=tmp_path / "documents.db")

    class _FailingOcr:
        def run_for_document(self, document_id: int):
            raise RuntimeError("tesseract boom")

    orchestrator.ocr = _FailingOcr()

    with pytest.raises(HTTPException) as excinfo:
        orchestrator.run_stage("ocr", 123, StagePayload())

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "tesseract boom"
