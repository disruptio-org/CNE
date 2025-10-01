import csv
import json
import sqlite3
from pathlib import Path

from exporter import CsvExporter
from matching import CandidateComparator
from review import ReviewService


def _prepare_document(db_path: Path) -> None:
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
            INSERT OR REPLACE INTO documents (
                id,
                file_name,
                file_hash,
                file_size,
                detected_type,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "approved.pdf",
                "hash-1",
                2048,
                "PDF_SEARCHABLE",
                "IN_REVIEW",
                "2024-01-01T00:00:00Z",
            ),
        )
        conn.commit()


def test_csv_exporter_generates_files(tmp_path: Path) -> None:
    db_path = tmp_path / "review.db"
    _prepare_document(db_path)

    comparator = CandidateComparator(db_path=db_path)
    row_agreement = {
        "document_id": 1,
        "DTMNFR": "2024",
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 1,
        "NOME_CANDIDATO": "Ana Souza",
        "PARTIDO_PROPONENTE": "Partido Azul",
        "SIGLA": "CNE",
        "SIMBOLO": "*",
        "NOME_LISTA": "Lista Única",
        "INDEPENDENTE": 0,
    }
    row_dispute_a = {
        "document_id": 1,
        "DTMNFR": "2024",
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 2,
        "NOME_CANDIDATO": "Bruno Lima",
        "PARTIDO_PROPONENTE": "Partido Azul",
        "SIGLA": "CNE",
        "SIMBOLO": "*",
        "NOME_LISTA": "Lista Única",
        "INDEPENDENTE": 0,
    }
    row_dispute_b = {
        "document_id": 1,
        "DTMNFR": "2024",
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 2,
        "NOME_CANDIDATO": "Bruna Lima",
        "PARTIDO_PROPONENTE": "Partido Azul",
        "SIGLA": "CNE",
        "SIMBOLO": "*",
        "NOME_LISTA": "Lista Única",
        "INDEPENDENTE": 0,
    }

    comparator.compare([row_agreement, row_dispute_a], [row_agreement, row_dispute_b])

    service = ReviewService(db_path=db_path)
    service.bulk_accept_agreements(document_id=1)
    dispute = service.fetch_comparisons(1, status="dispute")[0]
    service.save_decision(
        comparison_id=dispute["comparison_id"],
        document_id=1,
        selected_source="manual",
        final_value="Bruno Henrique",
        comment="Manual adjustment",
        reviewer="Supervisor",
    )
    service.approve_document(document_id=1, approver_id="lead", summary="All set")

    exporter = CsvExporter(db_path=db_path, output_dir=tmp_path / "exports")
    result = exporter.export()

    assert result.csv_path.exists()
    assert result.qa_path.exists()

    with result.csv_path.open(encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        rows = list(reader)

    header, *data_rows = rows
    assert header == [
        "DTMNFR",
        "ORGAO",
        "SIGLA",
        "SIMBOLO",
        "NOME_LISTA",
        "TIPO",
        "NUM_ORDEM",
        "NOME_CANDIDATO",
        "PARTIDO_PROPONENTE",
        "INDEPENDENTE",
    ]
    assert len(data_rows) == 2

    manual_row = next(row for row in data_rows if row[6] == "2")
    assert manual_row[7] == "Bruno Henrique"

    qa_data = json.loads(result.qa_path.read_text(encoding="utf-8"))
    assert qa_data["documents"] == 1
    assert qa_data["rows"] == 2
    assert qa_data["disputes"] == 1
    assert qa_data["manual_edits"] == 1
    assert qa_data["disagreement_percentage"] == 50.0
    assert qa_data["reviewed_rows"] == 2
