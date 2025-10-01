import io
import sys
import types
from pathlib import Path


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

from dashboard.app import PipelineOrchestrator
from ingestion.service import DocumentStatus, DocumentType, IngestionService
from ocr.pipeline import OcrPipeline


def _build_searchable_pdf(text: str) -> bytes:
    """Construct a minimal searchable PDF containing the provided text."""

    escaped = (
        text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    )
    content_lines = [
        b"BT",
        b"/F1 12 Tf",
        b"72 720 Td",
        f"({escaped}) Tj".encode("latin-1"),
        b"ET",
    ]
    content = b"\n".join(content_lines) + b"\n"

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"
        ),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    stream_object = (
        b"<< /Length "
        + str(len(content)).encode("latin-1")
        + b" >>\nstream\n"
        + content
        + b"endstream"
    )

    buffer = io.BytesIO()
    buffer.write(b"%PDF-1.4\n")
    buffer.write(b"%\xe2\xe3\xcf\xd3\n")

    offsets = [0]
    for index, obj in enumerate(objects[:3], start=1):
        offsets.append(buffer.tell())
        buffer.write(f"{index} 0 obj\n".encode("latin-1"))
        buffer.write(obj)
        buffer.write(b"\nendobj\n")

    offsets.append(buffer.tell())
    buffer.write(b"4 0 obj\n")
    buffer.write(stream_object)
    buffer.write(b"\nendobj\n")

    offsets.append(buffer.tell())
    buffer.write(b"5 0 obj\n")
    buffer.write(objects[3])
    buffer.write(b"\nendobj\n")

    xref_offset = buffer.tell()
    total_objects = 5
    buffer.write(f"xref\n0 {total_objects + 1}\n".encode("latin-1"))
    buffer.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        buffer.write(f"{offset:010d} 00000 n \n".encode("latin-1"))
    buffer.write(
        f"trailer\n<< /Root 1 0 R /Size {total_objects + 1} >>\n".encode("latin-1")
    )
    buffer.write(f"startxref\n{xref_offset}\n".encode("latin-1"))
    buffer.write(b"%%EOF\n")

    return buffer.getvalue()


class DummyOperator:
    def __init__(self) -> None:
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return ["ok"]


def test_pipeline_extracts_text_from_searchable_pdf(tmp_path: Path):
    data_dir = tmp_path / "data"
    uploads_dir = data_dir / "uploads"
    ocr_dir = data_dir / "ocr"
    db_path = data_dir / "documents.db"

    ingestion = IngestionService(upload_dir=uploads_dir, db_path=db_path)

    payload = _build_searchable_pdf("Hello searchable PDF")
    record = ingestion.ingest_upload(payload, "searchable.pdf")
    assert record.detected_type == DocumentType.PDF_SEARCHABLE

    pipeline = OcrPipeline(
        db_path=db_path,
        upload_dir=uploads_dir,
        ocr_output_dir=ocr_dir,
    )

    updated = pipeline.run_for_document(record.id)

    assert updated.status == DocumentStatus.OCR_DONE
    assert updated.ocr_text_path
    assert updated.ocr_pdf_path

    text_rel = Path(updated.ocr_text_path)
    if text_rel.is_absolute():
        text_file = text_rel
    else:
        text_file = (db_path.parent / text_rel).resolve()
    assert text_file.exists()
    text_content = text_file.read_text(encoding="utf-8").strip()
    assert "Hello searchable PDF" in text_content

    orchestrator = PipelineOrchestrator(db_path=db_path)
    operator = DummyOperator()
    rows = orchestrator._run_operator(operator, updated.id)

    assert rows == ["ok"]
    assert operator.calls
    assert "Hello searchable PDF" in operator.calls[0]["text"]
