"""FastAPI router exposing the review dashboard."""

from __future__ import annotations

import html
from pathlib import Path
from typing import List, Mapping, Sequence

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
        if active_document_id is not None:
            comparisons = service.fetch_comparisons(active_document_id, status=active_status)
        content = _render_review_page(
            documents=documents,
            comparisons=comparisons,
            active_document_id=active_document_id,
            active_status=active_status,
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
        service.bulk_accept_agreements(document_id=document_id)
        redirect_url = f"/review?document_id={document_id}"
        if status:
            redirect_url += f"&status={status}"
        return RedirectResponse(url=redirect_url, status_code=303)

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
) -> str:
    doc_options = _build_document_options(documents, active_document_id)
    status_options = _build_status_options(active_status)
    rows = _build_comparison_rows(comparisons, active_document_id, active_status)

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
          .actions {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; gap: 1rem; }}
          .actions form {{ margin: 0; }}
          .document-summary ul {{ margin: 0; padding-left: 1.2rem; }}
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
            { _render_bulk_accept(active_document_id, active_status) }
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
              <td>
                <form method=\"post\" action=\"/review/decision\" class=\"decision-form\">
                  <input type=\"hidden\" name=\"comparison_id\" value=\"{comparison_id}\" />
                  <input type=\"hidden\" name=\"document_id\" value=\"{active_document_id or ''}\" />
                  <input type=\"hidden\" name=\"status\" value=\"{active_status or ''}\" />
                  <label>Preferred source</label>
                  {_render_source_selector(selected_source)}
                  <label>Final value</label>
                  <input type=\"text\" name=\"final_value\" value=\"{html.escape(str(final_value))}\" />
                  <label>Reviewer</label>
                  <input type=\"text\" name=\"reviewer\" value=\"{html.escape(str(reviewer))}\" placeholder=\"Your name\" />
                  <label>Comment</label>
                  <textarea name=\"comment\" placeholder=\"Notes for this decision\">{html.escape(str(comment))}</textarea>
                  <button type=\"submit\">Save decision</button>
                </form>
              </td>
            </tr>
            """
        )
    return "".join(rows)


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


def _render_bulk_accept(active_document_id: int | None, active_status: str | None) -> str:
    if active_document_id is None:
        return ""
    status_value = active_status or "dispute"
    return (
        "<form method=\"post\" action=\"/review/bulk_accept\">"
        f"<input type=\"hidden\" name=\"document_id\" value=\"{active_document_id}\" />"
        f"<input type=\"hidden\" name=\"status\" value=\"{status_value}\" />"
        "<button type=\"submit\">Accept all agreements</button>"
        "</form>"
    )


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
