"""Ingestion service for handling document uploads.

This module provides the :class:`IngestionService` which is responsible for
persisting uploaded files to disk and recording metadata about the documents in
an on-disk SQLite database. The service can be embedded into an API layer (for
example a FastAPI router) and is designed to expose helpers that the dashboard
can call to trigger ingest operations.

The service supports the following responsibilities:

* Accept uploads for PDF, DOCX, and XLSX files (other file types are rejected).
* Calculate a deterministic hash for each file for deduplication and storage.
* Persist the file payload to a configurable uploads directory on disk.
* Record document metadata (name, hash, size, detected type, status, etc.)
  inside a ``documents`` table.
* Detect and classify PDF documents into searchable vs scanned (image-based)
  variants by examining the PDF text layer.
* Default new document records to the ``NEW`` status to signal downstream
  processing pipelines.
* Provide convenience helpers for wiring the ingestion service to API layers.

The implementation intentionally sticks to Python's standard library plus
optional dependencies. When a third-party dependency is unavailable at runtime,
the service gracefully degrades to heuristic detection while still accepting the
file. This makes the module portable across environments.
"""

from __future__ import annotations

import hashlib
import io
import mimetypes
import sqlite3
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import BinaryIO, Dict, Iterable, List, Optional


class DocumentStatus(str, Enum):
    """Enumeration of document lifecycle states."""

    NEW = "NEW"
    INGESTED = "INGESTED"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class DocumentType(str, Enum):
    """Enumeration of supported document classifications."""

    PDF_SEARCHABLE = "PDF_SEARCHABLE"
    PDF_SCANNED = "PDF_SCANNED"
    DOCX = "DOCX"
    XLSX = "XLSX"
    UNKNOWN = "UNKNOWN"


@dataclass
class DocumentRecord:
    """Representation of a row stored in the ``documents`` table."""

    id: int
    file_name: str
    file_hash: str
    file_size: int
    detected_type: DocumentType
    status: DocumentStatus
    created_at: str

    def dict(self) -> Dict[str, object]:
        """Return a JSON-serialisable representation of the record."""

        raw = asdict(self)
        raw["detected_type"] = self.detected_type.value
        raw["status"] = self.status.value
        return raw


class IngestionService:
    """Service responsible for ingesting documents and persisting metadata."""

    def __init__(
        self,
        upload_dir: Path | str = Path("data/uploads"),
        db_path: Path | str = Path("data/documents.db"),
    ) -> None:
        self.upload_dir = Path(upload_dir)
        self.db_path = Path(db_path)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise_db()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ingest_upload(self, file_obj: BinaryIO | bytes, filename: str) -> DocumentRecord:
        """Persist the provided file object and capture its metadata.

        Parameters
        ----------
        file_obj:
            A binary file object positioned at the start of the upload payload
            or a raw ``bytes`` instance containing the entire payload.
        filename:
            The original filename supplied by the client.

        Returns
        -------
        DocumentRecord
            The record describing the ingested document as stored in the
            database.

        Raises
        ------
        ValueError
            If the upload's file extension indicates an unsupported type or if
            the payload is empty.
        """

        if isinstance(file_obj, (bytes, bytearray)):
            payload = bytes(file_obj)
        else:
            payload = file_obj.read()
        if not payload:
            raise ValueError("The uploaded file payload is empty.")

        detected_type = self._classify_payload(payload, filename)
        if detected_type == DocumentType.UNKNOWN:
            raise ValueError("Unsupported file type. Only PDF, DOCX, and XLSX are accepted.")

        file_hash = hashlib.sha256(payload).hexdigest()
        file_size = len(payload)

        destination = self._destination_path(file_hash, filename)
        if not destination.exists():
            destination.write_bytes(payload)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO documents (file_name, file_hash, file_size, detected_type, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(file_hash) DO UPDATE SET
                    file_name=excluded.file_name,
                    file_size=excluded.file_size,
                    detected_type=excluded.detected_type,
                    status=excluded.status
                """,
                (
                    filename,
                    file_hash,
                    file_size,
                    detected_type.value,
                    DocumentStatus.NEW.value,
                ),
            )
            conn.commit()
            cursor.execute(
                "SELECT id, file_name, file_hash, file_size, detected_type, status, created_at FROM documents WHERE file_hash = ?",
                (file_hash,),
            )
            row = cursor.fetchone()

        if row is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Failed to persist document metadata.")

        return self._row_to_record(row)

    def list_documents(self, statuses: Optional[Iterable[DocumentStatus]] = None) -> List[DocumentRecord]:
        """Retrieve documents filtered by status (or all documents)."""

        query = "SELECT id, file_name, file_hash, file_size, detected_type, status, created_at FROM documents"
        params: List[object] = []
        if statuses:
            status_list = [status for status in statuses]
            if status_list:
                placeholders = ",".join("?" for _ in status_list)
                query += f" WHERE status IN ({placeholders})"
                params.extend(status.value for status in status_list)
        query += " ORDER BY created_at DESC"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        return [self._row_to_record(row) for row in rows]

    def mark_status(self, document_id: int, status: DocumentStatus) -> None:
        """Update the status of a stored document."""

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE documents SET status = ? WHERE id = ?",
                (status.value, document_id),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # FastAPI integration helpers
    # ------------------------------------------------------------------
    def build_router(self):  # pragma: no cover - optional helper for API layers
        """Return a FastAPI router exposing ingestion endpoints.

        The import of FastAPI and Pydantic models is deferred to keep the
        dependency optional when the router integration is not needed.
        """

        from fastapi import APIRouter, File, HTTPException, UploadFile
        from pydantic import BaseModel

        service = self
        router = APIRouter(prefix="/ingestion", tags=["ingestion"])

        class DocumentResponse(BaseModel):
            id: int
            file_name: str
            file_hash: str
            file_size: int
            detected_type: str
            status: str
            created_at: str

            @classmethod
            def from_record(cls, record: DocumentRecord) -> "DocumentResponse":
                return cls(**record.dict())

        @router.post("/documents", response_model=DocumentResponse)
        async def upload_document(file: UploadFile = File(...)):
            try:
                record = service.ingest_upload(await file.read(), file.filename)
            except ValueError as exc:  # pragma: no cover - handled by FastAPI runtime
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return DocumentResponse.from_record(record)

        @router.get("/documents", response_model=List[DocumentResponse])
        def list_all_documents():
            records = service.list_documents()
            return [DocumentResponse.from_record(record) for record in records]

        return router

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _initialise_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    file_hash TEXT NOT NULL UNIQUE,
                    file_size INTEGER NOT NULL,
                    detected_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def _classify_payload(self, payload: bytes, filename: str) -> DocumentType:
        """Determine the document type using filename hints and payload."""

        suffix = Path(filename).suffix.lower()
        mime_type, _ = mimetypes.guess_type(filename)

        if suffix == ".pdf" or payload.startswith(b"%PDF") or mime_type == "application/pdf":
            return self._classify_pdf(payload)

        if suffix in {".docx"} or mime_type in {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",
        }:
            return DocumentType.DOCX

        if suffix in {".xlsx"} or mime_type in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        }:
            return DocumentType.XLSX

        return DocumentType.UNKNOWN

    def _classify_pdf(self, payload: bytes) -> DocumentType:
        """Differentiate between searchable and scanned PDFs."""

        if self._pdf_has_text_layer(payload):
            return DocumentType.PDF_SEARCHABLE
        return DocumentType.PDF_SCANNED

    def _pdf_has_text_layer(self, payload: bytes) -> bool:
        """Heuristic to detect whether a PDF contains a text layer."""

        stream = io.BytesIO(payload)

        # Try using PyPDF2 / pypdf when available for accurate detection.
        for library in ("pypdf", "PyPDF2"):
            try:  # pragma: no cover - third-party dependency optional
                module = __import__(library)
                reader = module.PdfReader(stream)
                stream.seek(0)
            except Exception:  # pragma: no cover - fallback heuristics
                stream.seek(0)
                continue
            else:
                for page in reader.pages:
                    text = page.extract_text() if hasattr(page, "extract_text") else page.extractText()
                    if text and text.strip():
                        return True
                return False

        # Lightweight heuristic fallback: searchable PDFs normally embed font
        # declarations and text operands. We look for common PDF operators.
        sample = payload[:4096].lower()
        if b"/font" in sample:
            return True
        if b"bt" in sample and b"et" in sample:
            return True
        return False

    def _destination_path(self, file_hash: str, filename: str) -> Path:
        suffix = Path(filename).suffix.lower()
        safe_suffix = suffix if suffix else ""
        return self.upload_dir / f"{file_hash}{safe_suffix}"

    def _row_to_record(self, row: sqlite3.Row) -> DocumentRecord:
        return DocumentRecord(
            id=row["id"],
            file_name=row["file_name"],
            file_hash=row["file_hash"],
            file_size=row["file_size"],
            detected_type=DocumentType(row["detected_type"]),
            status=DocumentStatus(row["status"]),
            created_at=row["created_at"],
        )


__all__ = [
    "DocumentRecord",
    "DocumentStatus",
    "DocumentType",
    "IngestionService",
]
