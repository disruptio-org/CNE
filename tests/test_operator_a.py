import sqlite3
import textwrap
from pathlib import Path

import pytest

from operators import OperatorA


def read_all_rows(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT document_id, tipo, num_ordem, nome_candidato, partido_proponente, independente "
            "FROM operator_a_results ORDER BY tipo, num_ordem"
        )
        return cursor.fetchall()


def test_extract_from_text(tmp_path: Path):
    db_path = tmp_path / "operator.db"
    operator = OperatorA(db_path=db_path)
    sample_text = textwrap.dedent(
        """
        Efetivos
        1 - Ana Souza - Partido Azul
        2 - Bruno Lima (Independente)
        Suplentes
        1 - Carla Dias - Partido Verde
        2 - Daniel Nogueira - Partido Cinza
        """
    )

    rows = operator.run(
        document_id=42,
        text=sample_text,
        dtmnfr="2024",
        orgao="Conselho X",
        sigla="CNE",
        simbolo="★",
        nome_lista="Lista Unica",
    )

    assert len(rows) == 4
    assert [row.tipo for row in rows] == [2, 2, 3, 3]
    assert [row.num_ordem for row in rows if row.tipo == 2] == [1, 2]
    assert rows[1].independente == 1

    stored = read_all_rows(db_path)
    assert (42, 2, 2, "Bruno Lima", None, 1) in stored


def test_structured_rows_override_metadata(tmp_path: Path):
    db_path = tmp_path / "operator.db"
    operator = OperatorA(db_path=db_path)
    structured = [
        {
            "TIPO": 3,
            "NUM_ORDEM": 7,
            "NOME_CANDIDATO": "Eva Gomes",
            "PARTIDO_PROPONENTE": "Partido Roxo",
            "INDEPENDENTE": 0,
        },
        {
            "NOME_CANDIDATO": "Fábio Campos (Independente)",
        },
    ]

    rows = operator.run(
        document_id=99,
        structured_rows=structured,
        dtmnfr="2020",
        orgao="Orgao Y",
        sigla="ORG",
        simbolo="◇",
        nome_lista="Lista B",
        default_tipo=2,
    )

    assert len(rows) == 2
    assert rows[0].num_ordem == 7
    assert rows[1].tipo == 2
    assert rows[1].independente == 1

    stored = read_all_rows(db_path)
    assert (99, rows[1].tipo, rows[1].num_ordem, rows[1].nome_candidato, None, 1) in stored
