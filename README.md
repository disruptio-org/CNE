# CNE

CNE MVP for EAD

## Document ingestion service

The repository now ships with a lightweight ingestion module that can be used by
the dashboard (or other API layers) to accept PDF, DOCX, and XLSX uploads.

Key characteristics:

- Files are persisted under `data/uploads/` using their SHA-256 hash as the
  filename.
- Metadata is stored in `data/documents.db` within a `documents` table
  containing the file name, hash, size, detected document type, status, and
  creation timestamp.
- PDFs are classified as searchable vs scanned by checking whether they expose
  a text layer. DOCX and XLSX uploads are detected using MIME metadata and file
  extensions.
- New ingestions default to the `NEW` status, allowing background workers to
  pick them up for downstream processing.
- Scanned PDFs can be processed by the OCR pipeline to yield searchable PDFs
  and plaintext transcripts stored under `data/ocr/`.

## OCR pipeline

The `ocr.OcrPipeline` class inspects the ingestion database for scanned PDF
records, runs them through a local Tesseract binary, and writes both searchable
PDFs and extracted text files to `data/ocr/`. Document rows are updated with the
output locations, processing timestamps, and their status is set to
`OCR_DONE`. Searchable PDFs detected at ingestion time are skipped with a log
message.

To use the service in code:

```python
from ingestion.service import IngestionService

service = IngestionService()
with open("example.pdf", "rb") as handle:
    record = service.ingest_upload(handle, "example.pdf")
print(record)
```

If you are building a FastAPI application, you can expose an ingestion router
via `service.build_router()`.
