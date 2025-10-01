import sqlite3
from pathlib import Path

import pytest

from matching import CandidateComparator
from review import ReviewService


def _prepare_document_table(db_path: Path, text_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL UNIQUE,
                file_size INTEGER NOT NULL,
                detected_type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                ocr_pdf_path TEXT,
                ocr_text_path TEXT,
                ocr_started_at TEXT,
                ocr_completed_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO documents (
                id,
                file_name,
                file_hash,
                file_size,
                detected_type,
                status,
                created_at,
                ocr_text_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "example.pdf",
                "hash-1",
                1024,
                "PDF_SEARCHABLE",
                "OCR_DONE",
                "2024-01-01T00:00:00Z",
                str(text_path),
            ),
        )
        conn.commit()


def _insert_comparisons(db_path: Path) -> None:
    comparator = CandidateComparator(db_path=db_path)
    row_agreement = {
        "document_id": 1,
        "ORGAO": "Org",
        "TIPO": 1,
        "NUM_ORDEM": 1,
        "NOME_CANDIDATO": "Ana",
        "PARTIDO_PROPONENTE": "Azul",
    }
    row_dispute_a = {
        "document_id": 1,
        "ORGAO": "Org",
        "TIPO": 1,
        "NUM_ORDEM": 2,
        "NOME_CANDIDATO": "Bruno",
        "PARTIDO_PROPONENTE": "Azul",
    }
    row_dispute_b = {
        "document_id": 1,
        "ORGAO": "Org",
        "TIPO": 1,
        "NUM_ORDEM": 2,
        "NOME_CANDIDATO": "Bruna",
        "PARTIDO_PROPONENTE": "Azul",
    }
    comparator.compare([row_agreement, row_dispute_a], [row_agreement, row_dispute_b])


def test_review_service_workflow(tmp_path: Path) -> None:
    db_path = tmp_path / "review.db"
    text_path = tmp_path / "snippet.txt"
    text_path.write_text("Primeira linha\nSegunda linha\nTerceira linha", encoding="utf-8")

    # Prepare schema and baseline comparison records
    _prepare_document_table(db_path, text_path)
    ReviewService(db_path=db_path)  # ensure review table exists
    _insert_comparisons(db_path)

    service = ReviewService(db_path=db_path)

    summary = service.list_documents_with_disputes()
    assert len(summary) == 1
    assert summary[0]["document_id"] == 1
    assert summary[0]["dispute_count"] == 1

    disputes = service.fetch_comparisons(1, status="dispute")
    assert len(disputes) == 1
    dispute = disputes[0]
    assert dispute["status"] == "dispute"

    agreements = service.fetch_comparisons(1, status="agreement")
    assert len(agreements) == 1

    accepted = service.bulk_accept_agreements(document_id=1)
    assert accepted == 1
    # Re-running bulk accept should be idempotent.
    assert service.bulk_accept_agreements(document_id=1) == 0

    service.save_decision(
        comparison_id=dispute["comparison_id"],
        document_id=1,
        selected_source="operator_a",
        final_value="Bruno",
        comment="Prefer operator A",
        reviewer="Reviewer 1",
    )

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT comparison_id, selected_source, final_value, comment, reviewer FROM review_decisions"
        )
        saved = cursor.fetchall()

    assert len(saved) == 2
    # Ensure the manual decision is stored as expected.
    manual_row = [row for row in saved if row[0] == dispute["comparison_id"]][0]
    assert manual_row[1:] == ("operator_a", "Bruno", "Prefer operator A", "Reviewer 1")

    service.approve_document(document_id=1, approver_id="lead-reviewer", summary="All disputes resolved")

    with sqlite3.connect(db_path) as conn:
        status_row = conn.execute("SELECT status FROM documents WHERE id = 1").fetchone()
        assert status_row == ("APPROVED",)

        audit_rows = conn.execute(
            "SELECT actor_id, action, summary FROM audit_log WHERE document_id = 1"
        ).fetchall()

    assert audit_rows
    actor_id, action, summary = audit_rows[-1]
    assert actor_id == "lead-reviewer"
    assert action == "approve_document"
    assert summary == "All disputes resolved"

    with pytest.raises(ValueError):
        service.save_decision(
            comparison_id=dispute["comparison_id"],
            document_id=1,
            selected_source="manual",
            final_value="Manual",  # Should be blocked because the document is approved.
        )

    with pytest.raises(ValueError):
        service.bulk_accept_agreements(document_id=1)

    snippet = service.get_document_snippet(1)
    assert "Primeira linha" in snippet["snippet"]

    csv_path, qa_path, stats = service.export_approved_data(output_dir=tmp_path / "exports")
    assert csv_path.exists()
    assert qa_path.exists()
    assert stats["documents"] == 1
    assert stats["rows"] >= 1
