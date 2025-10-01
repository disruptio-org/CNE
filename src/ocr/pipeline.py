"""OCR pipeline for processing scanned PDF documents.

This module exposes :class:`OcrPipeline`, a lightweight job runner that
leverages an offline Tesseract installation to convert scanned PDFs into
searchable PDFs and accompanying plaintext transcriptions. OCR artefacts are
persisted under ``data/ocr`` and document metadata in the ingestion database is
updated to reflect processing progress.
"""

from __future__ import annotations

import logging
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

from ingestion import DocumentRecord, DocumentStatus, DocumentType

LOGGER = logging.getLogger(__name__)


class OcrPipeline:
    """Execute OCR jobs for scanned PDF documents."""

    def __init__(
        self,
        db_path: Path | str = Path("data/documents.db"),
        upload_dir: Path | str = Path("data/uploads"),
        ocr_output_dir: Path | str = Path("data/ocr"),
        tesseract_cmd: str = "tesseract",
    ) -> None:
        self.db_path = Path(db_path)
        self.upload_dir = Path(upload_dir)
        self.ocr_output_dir = Path(ocr_output_dir)
        self.tesseract_cmd = tesseract_cmd
        self.ocr_output_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> List[DocumentRecord]:
        """Process pending scanned PDFs and return updated records."""

        processed: List[DocumentRecord] = []
        for record in self._pending_pdf_documents():
            if record.detected_type == DocumentType.PDF_SEARCHABLE:
                LOGGER.info(
                    "Skipping OCR for %s (%s): already searchable",
                    record.file_name,
                    record.file_hash,
                )
                continue
            processed.append(self._process_scanned_pdf(record))
        return processed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _pending_pdf_documents(self) -> Iterable[DocumentRecord]:
        query = """
            SELECT
                id,
                file_name,
                file_hash,
                file_size,
                detected_type,
                status,
                created_at,
                ocr_pdf_path,
                ocr_text_path,
                ocr_started_at,
                ocr_completed_at
            FROM documents
            WHERE detected_type IN (?, ?)
              AND status != ?
            ORDER BY created_at ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                query,
                (
                    DocumentType.PDF_SCANNED.value,
                    DocumentType.PDF_SEARCHABLE.value,
                    DocumentStatus.OCR_DONE.value,
                ),
            )
            for row in cursor:
                yield self._row_to_record(row)

    def _process_scanned_pdf(self, record: DocumentRecord) -> DocumentRecord:
        if record.detected_type != DocumentType.PDF_SCANNED:
            LOGGER.debug(
                "Skipping OCR for %s (%s): not a scanned PDF",
                record.file_name,
                record.file_hash,
            )
            return record

        source = self._resolve_source_path(record)
        base_output = self.ocr_output_dir / record.file_hash
        text_output = base_output.with_suffix(".txt")
        pdf_output = base_output.with_suffix(".pdf")

        for artefact in (text_output, pdf_output):
            if artefact.exists():
                artefact.unlink()

        started_at = self._timestamp()
        self._run_tesseract(source, base_output)
        self._run_tesseract(source, base_output, ["pdf"])
        completed_at = self._timestamp()

        if not text_output.exists():
            raise RuntimeError(f"Tesseract did not produce expected text output for {record.file_name!r}.")
        if not pdf_output.exists():
            raise RuntimeError(f"Tesseract did not produce expected searchable PDF for {record.file_name!r}.")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                UPDATE documents
                SET
                    ocr_text_path = ?,
                    ocr_pdf_path = ?,
                    ocr_started_at = ?,
                    ocr_completed_at = ?,
                    status = ?
                WHERE id = ?
                """,
                (
                    str(text_output),
                    str(pdf_output),
                    started_at,
                    completed_at,
                    DocumentStatus.OCR_DONE.value,
                    record.id,
                ),
            )
            conn.commit()
            cursor = conn.execute(
                """
                SELECT
                    id,
                    file_name,
                    file_hash,
                    file_size,
                    detected_type,
                    status,
                    created_at,
                    ocr_pdf_path,
                    ocr_text_path,
                    ocr_started_at,
                    ocr_completed_at
                FROM documents
                WHERE id = ?
                """,
                (record.id,),
            )
            row = cursor.fetchone()

        if row is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Failed to refresh document metadata after OCR.")

        LOGGER.info("Completed OCR for %s (%s)", record.file_name, record.file_hash)
        return self._row_to_record(row)

    def _resolve_source_path(self, record: DocumentRecord) -> Path:
        suffix = Path(record.file_name).suffix.lower() or ".pdf"
        candidate = self.upload_dir / f"{record.file_hash}{suffix}"
        if candidate.exists():
            return candidate
        fallback = self.upload_dir / f"{record.file_hash}.pdf"
        if fallback.exists():
            return fallback
        raise FileNotFoundError(
            f"Original upload for document {record.id} ({record.file_name!r}) is missing from {self.upload_dir}."
        )

    def _run_tesseract(self, input_path: Path, output_base: Path, extra_args: Sequence[str] | None = None) -> None:
        command = [self.tesseract_cmd, str(input_path), str(output_base)]
        if extra_args:
            command.extend(extra_args)
        LOGGER.debug("Running tesseract command: %s", " ".join(command))
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("Tesseract binary not found. Ensure it is installed and on PATH.") from exc
        if result.returncode != 0:
            raise RuntimeError(
                f"Tesseract failed for {input_path.name}: {result.stderr.strip() or result.stdout.strip()}"
            )

    def _row_to_record(self, row: sqlite3.Row) -> DocumentRecord:
        return DocumentRecord(
            id=row["id"],
            file_name=row["file_name"],
            file_hash=row["file_hash"],
            file_size=row["file_size"],
            detected_type=DocumentType(row["detected_type"]),
            status=DocumentStatus(row["status"]),
            created_at=row["created_at"],
            ocr_pdf_path=row["ocr_pdf_path"],
            ocr_text_path=row["ocr_text_path"],
            ocr_started_at=row["ocr_started_at"],
            ocr_completed_at=row["ocr_completed_at"],
        )

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()


__all__ = ["OcrPipeline"]
