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
