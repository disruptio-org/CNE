"""OCR pipeline for processing scanned PDF documents.

This module exposes :class:`OcrPipeline`, a lightweight job runner that
leverages an offline Tesseract installation to convert scanned PDFs into
searchable PDFs and accompanying plaintext transcriptions. OCR artefacts are
persisted under ``data/ocr`` and document metadata in the ingestion database is
updated to reflect processing progress.
"""

from __future__ import annotations

import importlib
import logging
import re
import shutil
import sqlite3
import subprocess
import tempfile
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
            if record.detected_type == DocumentType.PDF_SCANNED:
                processed.append(self._process_scanned_pdf(record))
            elif record.detected_type == DocumentType.PDF_SEARCHABLE:
                processed.append(self._process_searchable_pdf(record))
        return processed

    def run_for_document(self, document_id: int) -> DocumentRecord:
        """Run OCR for a specific document if applicable."""

        record = self._fetch_document(document_id)
        if record is None:
            raise ValueError(f"Document {document_id} not found in ingestion database.")
        if record.detected_type == DocumentType.PDF_SCANNED:
            return self._process_scanned_pdf(record)
        if record.detected_type == DocumentType.PDF_SEARCHABLE:
            return self._process_searchable_pdf(record)
        LOGGER.debug(
            "Skipping OCR for %s (%s): unsupported type %s",
            record.file_name,
            record.file_hash,
            record.detected_type,
        )
        return record

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
        with tempfile.TemporaryDirectory(prefix="ocr_pages_", dir=self.ocr_output_dir) as tmp_dir:
            page_dir = Path(tmp_dir)
            page_images = self._render_pdf_to_images(source, page_dir)
            if not page_images:
                raise RuntimeError(
                    f"No rasterised pages produced from scanned PDF {record.file_name!r}."
                )

            per_page_texts: List[Path] = []
            per_page_pdfs: List[Path] = []
            for index, page_image in enumerate(page_images, start=1):
                page_output_base = base_output.parent / f"{base_output.name}_page_{index:04d}"
                page_text = page_output_base.with_suffix(".txt")
                page_pdf = page_output_base.with_suffix(".pdf")
                for artefact in (page_text, page_pdf):
                    if artefact.exists():
                        artefact.unlink()

                self._run_tesseract(page_image, page_output_base)
                self._run_tesseract(page_image, page_output_base, ["pdf"])

                if not page_text.exists():
                    raise RuntimeError(
                        "Tesseract did not produce expected per-page text output for "
                        f"{record.file_name!r}."
                    )
                if not page_pdf.exists():
                    raise RuntimeError(
                        "Tesseract did not produce expected per-page PDF output for "
                        f"{record.file_name!r}."
                    )
                per_page_texts.append(page_text)
                per_page_pdfs.append(page_pdf)

        with text_output.open("w", encoding="utf-8") as combined_text:
            written = False
            for page_text in per_page_texts:
                text = page_text.read_text(encoding="utf-8")
                if written:
                    combined_text.write("\n")
                combined_text.write(text)
                if not text.endswith("\n"):
                    combined_text.write("\n")
                written = True

        self._merge_pdfs(pdf_output, per_page_pdfs)

        for artefact in (*per_page_texts, *per_page_pdfs):
            try:
                artefact.unlink()
            except FileNotFoundError:
                pass
        completed_at = self._timestamp()

        if not text_output.exists():
            raise RuntimeError(f"Tesseract did not produce expected text output for {record.file_name!r}.")
        if not pdf_output.exists():
            raise RuntimeError(f"Tesseract did not produce expected searchable PDF for {record.file_name!r}.")

        LOGGER.info("Completed OCR for %s (%s)", record.file_name, record.file_hash)
        return self._finalise_document(record.id, text_output, pdf_output, started_at, completed_at)

    def _process_searchable_pdf(self, record: DocumentRecord) -> DocumentRecord:
        if record.detected_type != DocumentType.PDF_SEARCHABLE:
            LOGGER.debug(
                "Skipping OCR for %s (%s): not a searchable PDF",
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
        text_content = self._extract_pdf_text(source)
        text_output.write_text(text_content, encoding="utf-8")
        shutil.copy2(source, pdf_output)
        completed_at = self._timestamp()

        LOGGER.info("Extracted text for %s (%s)", record.file_name, record.file_hash)
        return self._finalise_document(record.id, text_output, pdf_output, started_at, completed_at)

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

    def _normalise_artifact_path(self, artefact: Path) -> Path:
        """Return a stable representation for persisted OCR artefact paths."""

        base_dir = self.db_path.parent
        if base_dir:
            try:
                return artefact.relative_to(base_dir)
            except ValueError:
                try:
                    return artefact.resolve().relative_to(base_dir.resolve())
                except ValueError:
                    pass
        return artefact.resolve()

    def _fetch_document(self, document_id: int) -> DocumentRecord | None:
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
            WHERE id = ?
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, (document_id,))
            row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def _run_tesseract(
        self,
        input_paths: Sequence[Path] | Path,
        output_base: Path,
        extra_args: Sequence[str] | None = None,
    ) -> None:
        if isinstance(input_paths, Path):
            inputs = [input_paths]
        else:
            inputs = list(input_paths)
        if not inputs:
            raise RuntimeError("No input images provided to Tesseract.")

        command = [self.tesseract_cmd, *map(str, inputs), str(output_base)]
        if extra_args:
            command.extend(extra_args)
        LOGGER.debug("Running tesseract command: %s", " ".join(command))
        sources = ", ".join(Path(path).name for path in inputs)
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except FileNotFoundError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("Tesseract binary not found. Ensure it is installed and on PATH.") from exc
        if result.stdout:
            LOGGER.debug("Tesseract stdout for %s: %s", sources, result.stdout.strip())
        if result.stderr:
            LOGGER.debug("Tesseract stderr for %s: %s", sources, result.stderr.strip())
        if result.returncode != 0:
            raise RuntimeError(
                f"Tesseract failed for {sources}: {result.stderr.strip() or result.stdout.strip()}"
            )

    def _merge_pdfs(self, output: Path, inputs: Sequence[Path]) -> None:
        if not inputs:
            raise RuntimeError("No PDF pages provided for merge operation.")

        spec = importlib.util.find_spec("fitz")
        if spec is not None:
            fitz = importlib.import_module("fitz")
            merged = fitz.open()
            try:
                for pdf_path in inputs:
                    if not Path(pdf_path).exists():
                        raise RuntimeError(f"Missing PDF artefact {pdf_path} during merge.")
                    with fitz.open(pdf_path) as document:
                        merged.insert_pdf(document)
                merged.save(output)
            finally:
                merged.close()
            return

        pdfunite = shutil.which("pdfunite")
        if pdfunite:
            command = [pdfunite, *map(str, inputs), str(output)]
            LOGGER.debug("Running pdfunite command: %s", " ".join(command))
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                raise RuntimeError(
                    f"pdfunite failed: {result.stderr.strip() or result.stdout.strip()}"
                )
            return

        raise RuntimeError(
            "Unable to merge per-page PDFs: install PyMuPDF (`pip install pymupdf`) or "
            "provide the `pdfunite` binary."
        )

    def _render_pdf_to_images(self, source: Path, destination: Path) -> List[Path]:
        """Rasterise ``source`` into ``destination`` using the available backend."""

        renderers = (
            self._render_with_pymupdf,
            self._render_with_pdf2image,
        )

        for renderer in renderers:
            pages = renderer(source, destination)
            if pages is not None:
                return pages

        raise RuntimeError(
            "No PDF rasteriser is available. Install PyMuPDF (`pip install pymupdf`) "
            "or pdf2image along with Poppler binaries to process scanned PDFs."
        )

    def _render_with_pymupdf(self, source: Path, destination: Path) -> List[Path] | None:
        """Render ``source`` pages using PyMuPDF when installed."""

        spec = importlib.util.find_spec("fitz")
        if spec is None:
            LOGGER.debug("PyMuPDF not available, skipping rasterisation with fitz.")
            return None

        fitz = importlib.import_module("fitz")

        rendered_pages: List[Path] = []
        try:
            with fitz.open(source) as document:
                for index, page in enumerate(document, start=1):
                    pixmap = page.get_pixmap()
                    page_path = destination / f"{source.stem}_page_{index:04d}.png"
                    pixmap.save(page_path)
                    rendered_pages.append(page_path)
        except Exception as exc:  # pragma: no cover - depends on optional backend
            LOGGER.warning(
                "PyMuPDF failed to rasterise %s: %s", source.name, exc,
                exc_info=LOGGER.isEnabledFor(logging.DEBUG),
            )
            for artefact in rendered_pages:
                try:
                    artefact.unlink()
                except FileNotFoundError:
                    pass
            return None

        return rendered_pages

    def _render_with_pdf2image(self, source: Path, destination: Path) -> List[Path] | None:
        """Render ``source`` pages using pdf2image if it is available."""

        spec = importlib.util.find_spec("pdf2image")
        if spec is None:
            LOGGER.debug("pdf2image not available, skipping Poppler-backed rasterisation.")
            return None

        pdf2image = importlib.import_module("pdf2image")

        try:
            images = pdf2image.convert_from_path(str(source))
        except Exception as exc:  # pragma: no cover - depends on local tooling
            missing_poppler = False
            exceptions = getattr(pdf2image, "exceptions", None)
            if exceptions is not None:
                missing_cls = getattr(exceptions, "PDFInfoNotInstalledError", None)
                if missing_cls is not None and isinstance(exc, missing_cls):
                    missing_poppler = True

            if missing_poppler:
                raise RuntimeError(
                    "pdf2image is installed but Poppler binaries are missing. Install Poppler "
                    "or rely on PyMuPDF by adding the `pymupdf` package."
                ) from exc

            raise RuntimeError(
                f"Failed to rasterise scanned PDF {source.name} using pdf2image: {exc}"
            ) from exc

        rendered_pages: List[Path] = []
        for index, image in enumerate(images, start=1):
            page_path = destination / f"{source.stem}_page_{index:04d}.png"
            try:
                image.save(page_path, format="PNG")
            except Exception as exc:  # pragma: no cover - pillow optional
                raise RuntimeError(
                    f"Failed to write rasterised page {index} for {source.name}: {exc}"
                ) from exc
            rendered_pages.append(page_path)

        return rendered_pages

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

    def _finalise_document(
        self,
        document_id: int,
        text_output: Path,
        pdf_output: Path,
        started_at: str,
        completed_at: str,
    ) -> DocumentRecord:
        text_store_path = self._normalise_artifact_path(text_output)
        pdf_store_path = self._normalise_artifact_path(pdf_output)

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
                    str(text_store_path),
                    str(pdf_store_path),
                    started_at,
                    completed_at,
                    DocumentStatus.OCR_DONE.value,
                    document_id,
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
                (document_id,),
            )
            row = cursor.fetchone()

        if row is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Failed to refresh document metadata after OCR.")

        return self._row_to_record(row)

    def _extract_pdf_text(self, source: Path) -> str:
        """Extract the textual layer from a searchable PDF."""

        extractors = [
            self._extract_pdf_text_with_pypdf,
            self._extract_pdf_text_with_basic_parser,
        ]
        for extractor in extractors:
            try:
                text = extractor(source)
            except RuntimeError:
                continue
            if text:
                return text
        # If all strategies fail, still return an empty transcript file.
        return ""

    @staticmethod
    def _extract_pdf_text_with_pypdf(source: Path) -> str:
        try:  # pragma: no cover - optional dependency
            from pypdf import PdfReader  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("pypdf not available") from exc

        reader = PdfReader(str(source))
        chunks: List[str] = []
        for page in reader.pages:
            if hasattr(page, "extract_text"):
                text = page.extract_text()  # type: ignore[attr-defined]
            else:  # pragma: no cover - compatibility shim
                text = page.extractText()  # type: ignore[attr-defined]
            if text:
                chunks.append(text)
        return "\n".join(chunk.strip() for chunk in chunks if chunk).strip()

    def _extract_pdf_text_with_basic_parser(self, source: Path) -> str:
        raw = source.read_bytes()
        text_segments: List[str] = []

        # Handle "(text) Tj" operands
        for match in re.finditer(rb"\((.*?)\)\s*T[jJ]", raw, re.DOTALL):
            text_segments.append(self._decode_pdf_string(match.group(1)))

        # Handle "[(text1)(text2)] TJ" operands
        for match in re.finditer(rb"\[(.*?)\]\s*TJ", raw, re.DOTALL):
            parts = re.findall(rb"\((.*?)\)", match.group(1), re.DOTALL)
            for part in parts:
                text_segments.append(self._decode_pdf_string(part))

        cleaned = "\n".join(filter(None, (segment.strip() for segment in text_segments)))
        if not cleaned:
            raise RuntimeError("No text operands located in PDF stream.")
        return cleaned

    @staticmethod
    def _decode_pdf_string(raw: bytes) -> str:
        text = raw.replace(b"\\\\", b"\\").replace(b"\\(", b"(").replace(b"\\)", b")")
        return text.decode("latin-1", errors="ignore")


__all__ = ["OcrPipeline"]
