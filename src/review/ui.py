"""FastAPI router exposing the review dashboard."""

from __future__ import annotations

import html
import zipfile
from pathlib import Path
from typing import List, Mapping, Sequence

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from .service import ReviewService

__all__ = ["build_review_router"]


def build_review_router(db_path: Path | str = Path("data/documents.db")) -> APIRouter:
    """Return an :class:`APIRouter` serving the review dashboard."""

    service = ReviewService(db_path=db_path)
    router = APIRouter()

    @router.get("/review", response_class=HTMLResponse)
    def review_dashboard(document_id: int | None = None, status: str | None = "dispute") -> HTMLResponse:
        documents = service.list_documents_with_disputes()
        active_status = status or None
        active_document_id = document_id
        if active_document_id is None and documents:
            active_document_id = documents[0]["document_id"]
        comparisons: List[Mapping[str, object]] = []
        active_document_status: str | None = None
        if active_document_id is not None:
            comparisons = service.fetch_comparisons(active_document_id, status=active_status)
            active_document = next(
                (entry for entry in documents if entry.get("document_id") == active_document_id),
                None,
            )
            if active_document:
                active_document_status = str(active_document.get("document_status") or "")
        content = _render_review_page(
            documents=documents,
            comparisons=comparisons,
            active_document_id=active_document_id,
            active_status=active_status,
            active_document_status=active_document_status,
        )
        return HTMLResponse(content=content)

    @router.post("/review/decision")
    def record_decision(
        comparison_id: int = Form(...),
        document_id: int = Form(...),
        selected_source: str = Form(...),
        final_value: str | None = Form(None),
        comment: str | None = Form(None),
        reviewer: str | None = Form(None),
        status: str | None = Form(None),
    ) -> RedirectResponse:
        try:
            service.save_decision(
                comparison_id=comparison_id,
                document_id=document_id,
                selected_source=selected_source,
                final_value=final_value,
                comment=comment,
                reviewer=reviewer,
            )
        except ValueError as exc:  # pragma: no cover - handled by FastAPI runtime
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        redirect_url = f"/review?document_id={document_id}"
        if status:
            redirect_url += f"&status={status}"
        return RedirectResponse(url=redirect_url, status_code=303)

    @router.post("/review/bulk_accept")
    def bulk_accept(
        document_id: int = Form(...),
        status: str | None = Form("dispute"),
    ) -> RedirectResponse:
        try:
            service.bulk_accept_agreements(document_id=document_id)
        except ValueError as exc:  # pragma: no cover - handled by FastAPI runtime
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        redirect_url = f"/review?document_id={document_id}"
        if status:
            redirect_url += f"&status={status}"
        return RedirectResponse(url=redirect_url, status_code=303)

    @router.post("/review/approve")
    def approve_document(
        document_id: int = Form(...),
        approver_id: str = Form(...),
        summary: str | None = Form(None),
        status: str | None = Form("dispute"),
    ) -> RedirectResponse:
        try:
            service.approve_document(
                document_id=document_id,
                approver_id=approver_id,
                summary=summary,
            )
        except ValueError as exc:  # pragma: no cover - handled by FastAPI runtime
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        redirect_url = f"/review?document_id={document_id}"
        if status:
            redirect_url += f"&status={status}"
        return RedirectResponse(url=redirect_url, status_code=303)

    @router.get("/review/export")
    def export_bundle() -> FileResponse:
        try:
            csv_path, qa_path, _ = service.export_approved_data()
        except ValueError as exc:  # pragma: no cover - handled via HTTPException
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        archive_path = csv_path.with_suffix(".zip")
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            bundle.write(csv_path, arcname=csv_path.name)
            bundle.write(qa_path, arcname=qa_path.name)

        return FileResponse(
            path=archive_path,
            media_type="application/zip",
            filename=archive_path.name,
        )

    @router.get("/review/snippet/{document_id}")
    def document_snippet(document_id: int) -> JSONResponse:
        payload = service.get_document_snippet(document_id)
        return JSONResponse(content=payload)

    return router


def _render_review_page(
    *,
    documents: Sequence[Mapping[str, object]],
    comparisons: Sequence[Mapping[str, object]],
    active_document_id: int | None,
    active_status: str | None,
    active_document_status: str | None,
) -> str:
    doc_options = _build_document_options(documents, active_document_id)
    status_options = _build_status_options(active_status)
    is_editable = (active_document_status or "").upper() != "APPROVED"
    rows = _build_comparison_rows(
        comparisons,
        active_document_id,
        active_status,
        editable=is_editable,
    )

    return f"""
    <!DOCTYPE html>
    <html lang=\"en\">
      <head>
        <meta charset=\"utf-8\" />
        <title>Document Review</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 2rem; background-color: #f7f7f7; }}
          h1 {{ margin-bottom: 1rem; }}
          .panel {{ background: #fff; padding: 1rem 1.5rem; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 1.5rem; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #e0e0e0; padding: 0.5rem; vertical-align: top; }}
          th {{ background: #fafafa; text-align: left; }}
          tr.status-dispute {{ background: #fff3e0; }}
          tr.status-agreement {{ background: #e8f5e9; }}
          tr.status-missing_operator_a, tr.status-missing_operator_b {{ background: #ede7f6; }}
          .filters form {{ display: flex; flex-wrap: wrap; gap: 1rem; align-items: center; }}
          .filters label {{ font-weight: bold; }}
          .decision-form {{ display: flex; flex-direction: column; gap: 0.3rem; }}
          .decision-form textarea {{ min-height: 3rem; }}
          .snippet-panel pre {{ background: #212121; color: #fafafa; padding: 1rem; border-radius: 4px; overflow-x: auto; max-height: 18rem; }}
          .actions {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; gap: 1rem; flex-wrap: wrap; }}
          .actions form {{ margin: 0; display: flex; gap: 0.5rem; align-items: flex-end; flex-wrap: wrap; }}
          .document-summary ul {{ margin: 0; padding-left: 1.2rem; }}
          .status-pill {{ display: inline-block; padding: 0.1rem 0.6rem; border-radius: 12px; background: #eceff1; font-size: 0.85rem; }}
          .status-pill.approved {{ background: #a5d6a7; }}
          .decision-readonly {{ background: #fafafa; border: 1px solid #e0e0e0; padding: 0.75rem; border-radius: 4px; }}
          .decision-readonly strong {{ display: block; margin-bottom: 0.3rem; }}
          .approval-notice {{ font-style: italic; color: #5d4037; }}
        </style>
      </head>
      <body>
        <div class=\"panel\">
          <h1>Review disputed records</h1>
          <div class=\"filters\">
            <form method=\"get\" action=\"/review\">{doc_options}
              <label for=\"status\">Row filter</label>
              <select name=\"status\" id=\"status\">{status_options}</select>
              <button type=\"submit\">Apply filters</button>
            </form>
          </div>
          <div class=\"actions\">
            <div>
              <strong>Documents with disputes:</strong>
              <span>{len(documents)}</span>
            </div>
            <div>
              <strong>Status:</strong>
              { _render_status_indicator(active_document_status) }
            </div>
            { _render_export_button() }
            { _render_bulk_accept(active_document_id, active_status, is_editable) }
            { _render_approval_controls(active_document_id, active_status, active_document_status, is_editable) }
          </div>
          <div class=\"document-summary\">
            {_render_document_list(documents, active_document_id)}
          </div>
        </div>
        <div class=\"panel\">
          <h2>Comparison results</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Operator A</th>
                <th>Operator B</th>
                <th>Decision</th>
              </tr>
            </thead>
            <tbody>
              {rows}
            </tbody>
          </table>
        </div>
        <div class=\"panel snippet-panel\">
          <div style=\"display:flex; align-items:center; gap:1rem;\">
            <h2 style=\"margin:0;\">Document snippet</h2>
            <button type=\"button\" onclick=\"loadSnippet({active_document_id or 'null'})\">Load snippet</button>
          </div>
          <pre id=\"snippet-content\">Select a document to preview OCR text.</pre>
        </div>
        <script>
          async function loadSnippet(documentId) {{
            const output = document.getElementById('snippet-content');
            if (!documentId) {{
              output.textContent = 'No document selected.';
              return;
            }}
            output.textContent = 'Loading snippet...';
            try {{
              const response = await fetch(`/review/snippet/${{documentId}}`);
              if (!response.ok) {{
                output.textContent = 'Failed to load snippet.';
                return;
              }}
              const data = await response.json();
              output.textContent = data.snippet || 'No snippet available.';
            }} catch (error) {{
              output.textContent = 'Failed to load snippet.';
            }}
          }}
        </script>
      </body>
    </html>
    """


def _build_document_options(documents: Sequence[Mapping[str, object]], active_document_id: int | None) -> str:
    options = ["<label for=\"document_id\">Document</label>"]
    select = ["<select name=\"document_id\" id=\"document_id\">"]
    for entry in documents:
        doc_id = entry.get("document_id")
        label = f"{entry.get('file_name')} ({entry.get('dispute_count')} disputes)"
        selected = " selected" if active_document_id == doc_id else ""
        value = "" if doc_id is None else str(doc_id)
        select.append(
            f"<option value=\"{html.escape(str(value))}\"{selected}>{html.escape(label)}</option>"
        )
    if not documents:
        select.append('<option value="">No disputes found</option>')
    select.append("</select>")
    options.extend(select)
    return "".join(options)


def _build_status_options(active_status: str | None) -> str:
    statuses = [
        ("", "All rows"),
        ("dispute", "Disputes only"),
        ("agreement", "Agreements only"),
        ("missing_operator_a", "Only missing from Operator B"),
        ("missing_operator_b", "Only missing from Operator A"),
    ]
    rendered = []
    for value, label in statuses:
        selected = " selected" if (active_status or "") == value else ""
        rendered.append(f"<option value=\"{value}\"{selected}>{html.escape(label)}</option>")
    return "".join(rendered)


def _build_comparison_rows(
    comparisons: Sequence[Mapping[str, object]],
    active_document_id: int | None,
    active_status: str | None,
    *,
    editable: bool,
) -> str:
    if not comparisons:
        return '<tr><td colspan="4">No comparison rows found for the selected filters.</td></tr>'

    rows: List[str] = []
    for entry in comparisons:
        comparison_id = entry.get("comparison_id")
        status = str(entry.get("status") or "unknown")
        nome_a = html.escape(entry.get("nome_a") or "–")
        nome_b = html.escape(entry.get("nome_b") or "–")
        partido_a = html.escape(entry.get("partido_a") or "")
        partido_b = html.escape(entry.get("partido_b") or "")
        final_value = entry.get("final_value") or entry.get("nome_a") or entry.get("nome_b") or ""
        selected_source = entry.get("selected_source") or ""
        comment = entry.get("comment") or ""
        reviewer = entry.get("reviewer") or ""
        status_badge = html.escape(status.replace("_", " ").title())
        decision_html = _render_decision_controls(
            entry,
            active_document_id,
            active_status,
            editable=editable,
        )
        rows.append(
            f"""
            <tr class=\"status-{status}\">
              <td>
                <strong>{comparison_id}</strong><br />
                <span>{status_badge}</span>
              </td>
              <td>
                <div><strong>{nome_a}</strong></div>
                <div>{partido_a}</div>
              </td>
              <td>
                <div><strong>{nome_b}</strong></div>
                <div>{partido_b}</div>
              </td>
              <td>{decision_html}</td>
            </tr>
            """
        )
    return "".join(rows)


def _render_decision_controls(
    entry: Mapping[str, object],
    active_document_id: int | None,
    active_status: str | None,
    *,
    editable: bool,
) -> str:
    comparison_id = entry.get("comparison_id")
    selected_source = entry.get("selected_source") or ""
    final_value = entry.get("final_value") or entry.get("nome_a") or entry.get("nome_b") or ""
    reviewer = entry.get("reviewer") or ""
    comment = entry.get("comment") or ""

    if not editable:
        summary_lines = [
            f"<strong>Selected source:</strong> {html.escape(str(selected_source) or '—')}",
            f"<strong>Final value:</strong> {html.escape(str(final_value) or '—')}",
        ]
        if reviewer:
            summary_lines.append(f"<strong>Reviewer:</strong> {html.escape(str(reviewer))}")
        if comment:
            summary_lines.append(f"<strong>Comment:</strong> {html.escape(str(comment))}")
        summary = "<br />".join(summary_lines) or "No decision recorded."
        return f"<div class=\"decision-readonly\">{summary}</div>"

    return (
        "<form method=\"post\" action=\"/review/decision\" class=\"decision-form\">"
        f"<input type=\"hidden\" name=\"comparison_id\" value=\"{comparison_id}\" />"
        f"<input type=\"hidden\" name=\"document_id\" value=\"{active_document_id or ''}\" />"
        f"<input type=\"hidden\" name=\"status\" value=\"{active_status or ''}\" />"
        "<label>Preferred source</label>"
        f"{_render_source_selector(selected_source)}"
        "<label>Final value</label>"
        f"<input type=\"text\" name=\"final_value\" value=\"{html.escape(str(final_value))}\" />"
        "<label>Reviewer</label>"
        f"<input type=\"text\" name=\"reviewer\" value=\"{html.escape(str(reviewer))}\" placeholder=\"Your name\" />"
        "<label>Comment</label>"
        f"<textarea name=\"comment\" placeholder=\"Notes for this decision\">{html.escape(str(comment))}</textarea>"
        "<button type=\"submit\">Save decision</button>"
        "</form>"
    )


def _render_source_selector(selected_source: str | None) -> str:
    options = [
        ("operator_a", "Use Operator A"),
        ("operator_b", "Use Operator B"),
        ("manual", "Manual override"),
        ("agreement", "Accepted agreement"),
    ]
    rendered = ["<select name=\"selected_source\" required>"]
    current = selected_source or ""
    for value, label in options:
        selected = " selected" if current == value else ""
        rendered.append(f"<option value=\"{value}\"{selected}>{html.escape(label)}</option>")
    rendered.append("</select>")
    return "".join(rendered)


def _render_export_button() -> str:
    return (
        "<form method=\"get\" action=\"/review/export\">"
        "<button type=\"submit\">Download CSV &amp; QA</button>"
        "</form>"
    )


def _render_bulk_accept(
    active_document_id: int | None,
    active_status: str | None,
    editable: bool,
) -> str:
    if active_document_id is None:
        return ""
    if not editable:
        return "<div class=\"approval-notice\">Approved documents cannot accept agreements.</div>"
    status_value = active_status or "dispute"
    return (
        "<form method=\"post\" action=\"/review/bulk_accept\">"
        f"<input type=\"hidden\" name=\"document_id\" value=\"{active_document_id}\" />"
        f"<input type=\"hidden\" name=\"status\" value=\"{status_value}\" />"
        "<button type=\"submit\">Accept all agreements</button>"
        "</form>"
    )


def _render_approval_controls(
    active_document_id: int | None,
    active_status: str | None,
    active_document_status: str | None,
    editable: bool,
) -> str:
    if active_document_id is None:
        return ""
    if not editable:
        return "<div class=\"approval-notice\">Document is approved. Edits are locked.</div>"
    status_value = active_status or "dispute"
    return (
        "<form method=\"post\" action=\"/review/approve\" class=\"decision-form\">"
        f"<input type=\"hidden\" name=\"document_id\" value=\"{active_document_id}\" />"
        f"<input type=\"hidden\" name=\"status\" value=\"{status_value}\" />"
        "<label>Approver</label>"
        "<input type=\"text\" name=\"approver_id\" placeholder=\"Approver name\" required />"
        "<label>Summary</label>"
        "<input type=\"text\" name=\"summary\" placeholder=\"Approval notes\" />"
        "<button type=\"submit\">Approve document</button>"
        "</form>"
    )


def _render_status_indicator(active_document_status: str | None) -> str:
    if not active_document_status:
        return "<span class=\"status-pill\">Unknown</span>"
    status_upper = active_document_status.upper()
    pill_class = "status-pill"
    if status_upper == "APPROVED":
        pill_class += " approved"
    label = html.escape(active_document_status.replace("_", " ").title())
    return f"<span class=\"{pill_class}\">{label}</span>"


def _render_document_list(
    documents: Sequence[Mapping[str, object]],
    active_document_id: int | None,
) -> str:
    if not documents:
        return '<p>No disputes pending review.</p>'

    items: List[str] = []
    for entry in documents:
        doc_id = entry.get("document_id")
        label = f"{entry.get('file_name')} — {entry.get('dispute_count')} disputes"
        if doc_id == active_document_id:
            items.append(f"<li><strong>{html.escape(label)}</strong></li>")
        else:
            items.append(f"<li>{html.escape(label)}</li>")
    return "<ul>" + "".join(items) + "</ul>"
