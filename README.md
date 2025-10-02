# CNE

CNE MVP for EAD

## System overview

The repository provides an end-to-end processing pipeline that ingests election
documents, extracts candidate data via operator heuristics, reconciles the
results, and offers a review dashboard for human validation before export. All
state is persisted in `data/documents.db` and surfaced through a FastAPI
application that also serves a lightweight frontend console.

### Data locations

- **Uploads** – Source files and text fallbacks live under `data/uploads/`.
- **OCR artefacts** – Searchable PDFs and transcripts are written to
  `data/ocr/` by the OCR pipeline.【F:src/ocr/pipeline.py†L3-L52】
- **Database** – Document metadata, comparison rows, review decisions, and audit
  logs are stored in `data/documents.db`.
- **Exports** – Approved CSV bundles and QA reports are emitted to
  `data/exports/` by default.【F:src/exporter/csv_export.py†L25-L76】

## Processing pipeline

The dashboard API exposes the full workflow through
`POST /api/documents/{document_id}/stages/{stage}`. The
`PipelineOrchestrator` inside `src/dashboard/app.py` wires each stage to the
underlying services.【F:src/dashboard/app.py†L35-L121】 The table below summarises
the stages currently implemented and how they fit together.

| Stage | Purpose | Trigger |
| --- | --- | --- |
| `operator_a` / `operator_b` | Run the heuristic extractors defined in `operators.operator_a.OperatorA` and `operators.operator_b.OperatorB` to populate structured candidate rows for the selected document. | `POST /api/documents/{id}/stages/operator_a` or `/stages/operator_b`; alternatively instantiate `OperatorA`/`OperatorB` and call `.run(...)`.【F:src/operators/operator_a.py†L1-L151】【F:src/dashboard/app.py†L58-L156】 |
| `match` | Compare the stored operator outputs and persist dispute/agreement rows via `matching.CandidateComparator`, enabling downstream review. | `POST /api/documents/{id}/stages/match`; programmatically call `CandidateComparator.compare(a_rows, b_rows)` if running outside the API.【F:src/matching/comparator.py†L1-L120】【F:src/dashboard/app.py†L64-L163】 |
| `review` | Fetch the latest comparison rows and any reviewer decisions using `review.ReviewService`, returning payloads suitable for UI inspection. | `POST /api/documents/{id}/stages/review` with an optional `status` filter in the request body. Direct consumers can call `ReviewService.fetch_comparisons(...)`.【F:src/review/service.py†L28-L187】【F:src/dashboard/app.py†L70-L111】 |
| `approve` | Mark the document as approved once disputes are resolved, recording the action in the audit log. | `POST /api/documents/{id}/stages/approve` with an `approver_id`; programmatic usage calls `ReviewService.approve_document(...)`.【F:src/review/service.py†L249-L308】【F:src/dashboard/app.py†L82-L156】 |
| `export` | Generate the CSV and QA bundle for all approved documents using `exporter.CsvExporter`, logging the export event. | `POST /api/documents/{id}/stages/export` optionally supplying `output_dir`; or call `CsvExporter.export()` / `ReviewService.export_approved_data(...)`.【F:src/exporter/csv_export.py†L25-L155】【F:src/dashboard/app.py†L91-L158】 |
| Dashboard UI | Serve the HTML/JS console that orchestrates the pipeline, displays per-stage progress, and offers operator shortcuts. | Launch the FastAPI app and open the root (`/`) or `/app` endpoint in a browser. The UI issues the stage requests listed above.【F:frontend/index.html†L1-L82】【F:frontend/app.js†L1-L160】 |

The pipeline assumes that ingestion and OCR have already populated the document
metadata. You can refresh OCR results for a specific document via the `ocr`
stage, or rerun ingestion metadata checks via the `ingest` stage using the same
endpoint. Both stages rely on `IngestionService` and `OcrPipeline`
implementations bundled with the dashboard.【F:src/dashboard/app.py†L38-L82】

## Running the dashboard API

Install the project in editable mode to make the dashboard package available:

```bash
pip install -e .
```

The default installation pulls in [PyMuPDF](https://pymupdf.readthedocs.io/),
providing a pure-Python rasteriser for scanned PDFs. This avoids the external
Poppler dependency normally required by `pdf2image`, while still allowing that
backend to be used if both the library and Poppler binaries are installed.
`OcrPipeline` automatically prefers PyMuPDF and falls back to `pdf2image` when
available.【F:src/ocr/pipeline.py†L252-L343】

Then start the FastAPI application. The helper script adjusts `sys.path` and
runs Uvicorn with the correct factory entry point:

```bash
python scripts/run_dashboard.py
```

The API is also available via `uvicorn dashboard.app:create_app --factory`. Once
running, browse to <http://localhost:8000/> for the operator dashboard or
interact with the stage endpoints directly using tools such as `curl`:

```bash
curl -X POST \
  http://localhost:8000/api/documents/1/stages/operator_a \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Launching the frontend

The FastAPI application automatically mounts the static frontend at `/app`. When
the server is running, open <http://localhost:8000/app> to access the dashboard
UI built from the files in `frontend/`. During development you can also serve
the directory manually (for example `python -m http.server` inside `frontend/`),
but the API-powered experience requires the FastAPI backend for stage actions
and live status updates.【F:src/dashboard/app.py†L220-L264】

## Running tests

Execute the Python test suite with Pytest:

```bash
pytest
```

This command exercises the ingestion, OCR, operator, matching, review, and
export utilities to ensure the full workflow remains functional.

## Artefacts and audit trail

- OCR outputs (`*.pdf` and `*.txt`) are placed in `data/ocr/`, while searchable
  text fallbacks used by the operators are also stored alongside the original
  uploads.【F:src/ocr/pipeline.py†L31-L135】
- Export bundles are saved to `data/exports/` with timestamped filenames, and a
  JSON QA report accompanies every CSV run.【F:src/exporter/csv_export.py†L69-L155】
- Every approval or export action appends a row to the `audit_log` table in
  `data/documents.db`, recording the actor, action, summary, and timestamp for
  traceability.【F:src/review/service.py†L249-L308】【F:src/dashboard/app.py†L91-L158】

By following the stages listed above—either from the dashboard UI or via direct
API calls—you can ingest documents, rerun OCR, extract candidate rows, reconcile
operator differences, review disputes, approve the final data, and export the
resulting datasets end to end.

