import json
import sqlite3
from pathlib import Path

from matching import CandidateComparator


def read_records(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT status, confidence, similarity, distance, payload FROM candidate_comparisons"
        )
        return cursor.fetchall()


def test_agreement_records_are_persisted(tmp_path: Path):
    db_path = tmp_path / "comparisons.db"
    comparator = CandidateComparator(db_path=db_path)

    row = {
        "document_id": 1,
        "DTMNFR": "2024",
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 1,
        "NOME_CANDIDATO": "Ana Souza",
        "PARTIDO_PROPONENTE": "Partido Azul",
        "SIGLA": "CNE",
        "NOME_LISTA": "Lista Única",
        "INDEPENDENTE": 0,
    }

    results = comparator.compare([row], [row])
    assert len(results) == 1
    record = results[0]
    assert record.status == "agreement"
    assert record.confidence == 1.0
    assert record.similarity == 1.0
    assert record.distance == 0

    stored = read_records(db_path)
    assert len(stored) == 1
    status, confidence, similarity, distance, payload = stored[0]
    assert status == "agreement"
    assert confidence == 1.0
    assert similarity == 1.0
    assert distance == 0

    parsed = json.loads(payload)
    assert parsed["operator_a"]["nome_candidato"] == "Ana Souza"
    assert parsed["operator_a"] == parsed["operator_b"]


def test_disputes_include_similarity(tmp_path: Path):
    db_path = tmp_path / "comparisons.db"
    comparator = CandidateComparator(db_path=db_path)

    row_a = {
        "document_id": 7,
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 5,
        "NOME_CANDIDATO": "Bruna Lima",
        "PARTIDO_PROPONENTE": "Partido Azul",
    }
    row_b = {
        "document_id": 7,
        "ORGAO": "Conselho",
        "TIPO": 2,
        "NUM_ORDEM": 5,
        "NOME_CANDIDATO": "Bruna L. Lima",
        "PARTIDO_PROPONENTE": "Partido Azul",
    }

    results = comparator.compare([row_a], [row_b])
    assert len(results) == 1
    record = results[0]
    assert record.status == "dispute"
    assert 0 < record.similarity < 1
    assert record.confidence == record.similarity
    assert record.distance > 0


def test_missing_rows_are_flagged(tmp_path: Path):
    db_path = tmp_path / "comparisons.db"
    comparator = CandidateComparator(db_path=db_path)

    row_a = {
        "document_id": 9,
        "ORGAO": "Conselho",
        "TIPO": 3,
        "NUM_ORDEM": 2,
        "NOME_CANDIDATO": "Carlos Souza",
    }
    row_b = {
        "document_id": 9,
        "ORGAO": "Conselho",
        "TIPO": 3,
        "NUM_ORDEM": 3,
        "NOME_CANDIDATO": "Daniel Rocha",
    }

    results = comparator.compare([row_a], [row_b])
    assert len(results) == 2
    statuses = sorted(record.status for record in results)
    assert statuses == ["missing_operator_a", "missing_operator_b"]
