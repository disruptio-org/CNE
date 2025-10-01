"""Comparison utilities between Operator A and Operator B outputs.

The module provides :class:`CandidateComparator`, a helper that inspects the
rows produced by the extraction operators and produces comparison records that
can be surfaced in review tooling. Rows are joined using a common key composed
of organisational metadata and ordinal fields. Exact matches are flagged as
agreements while mismatched values generate dispute records augmented with
Levenshtein based similarity metrics so reviewers can prioritise the most
critical differences.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence

__all__ = ["CandidateComparator", "ComparisonRecord"]


@dataclass(slots=True)
class ComparisonRecord:
    """Representation of a comparison outcome persisted to the database."""

    document_id: int | None
    orgao: str | None
    tipo: int | None
    num_ordem: int | None
    nome_a: str | None
    nome_b: str | None
    partido_a: str | None
    partido_b: str | None
    status: str
    confidence: float
    similarity: float
    distance: int
    payload: str

    def as_tuple(self) -> tuple:
        return (
            self.document_id,
            self.orgao,
            self.tipo,
            self.num_ordem,
            self.nome_a,
            self.nome_b,
            self.partido_a,
            self.partido_b,
            self.status,
            self.confidence,
            self.similarity,
            self.distance,
            self.payload,
            datetime.now(timezone.utc).isoformat(),
        )


class CandidateComparator:
    """Compare operator outputs and persist review friendly records."""

    KEY_FIELDS: Sequence[str] = (
        "document_id",
        "orgao",
        "tipo",
        "num_ordem",
        "dtmnfr",
        "sigla",
        "nome_lista",
    )
    EXACT_FIELDS: Sequence[str] = (
        "nome_candidato",
        "partido_proponente",
        "independente",
        "sigla",
        "nome_lista",
        "dtmnfr",
        "simbolo",
    )
    FUZZY_FIELDS: Sequence[str] = ("nome_candidato",)

    def __init__(self, db_path: Path | str = Path("data/documents.db")) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise_db()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def compare(
        self,
        operator_a_rows: Iterable[Mapping[str, object] | object],
        operator_b_rows: Iterable[Mapping[str, object] | object],
    ) -> list[ComparisonRecord]:
        """Compare rows produced by Operator A and B.

        Parameters
        ----------
        operator_a_rows, operator_b_rows:
            Iterable collections of rows produced by the operators. They can be
            dictionaries, dataclasses or any object exposing attributes that
            match the schema.
        """

        index_a = self._index_rows(operator_a_rows)
        index_b = self._index_rows(operator_b_rows)

        comparison_records: list[ComparisonRecord] = []
        for key in sorted(set(index_a) | set(index_b)):
            row_a = index_a.get(key)
            row_b = index_b.get(key)
            record = self._build_record(row_a, row_b)
            comparison_records.append(record)

        if comparison_records:
            self._persist_records(comparison_records)
        return comparison_records

    def fetch_records(
        self,
        *,
        document_id: int | None = None,
        limit: int | None = None,
    ) -> list[ComparisonRecord]:
        """Retrieve stored comparison records for UI consumption."""

        query = (
            "SELECT document_id, orgao, tipo, num_ordem, nome_a, nome_b, "
            "partido_a, partido_b, status, confidence, similarity, distance, payload "
            "FROM candidate_comparisons"
        )
        params: list[object] = []
        if document_id is not None:
            query += " WHERE document_id = ?"
            params.append(document_id)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _row_to_record(self, row: Sequence[object]) -> ComparisonRecord:
        return ComparisonRecord(
            document_id=row[0],
            orgao=row[1],
            tipo=row[2],
            num_ordem=row[3],
            nome_a=row[4],
            nome_b=row[5],
            partido_a=row[6],
            partido_b=row[7],
            status=row[8],
            confidence=row[9],
            similarity=row[10],
            distance=row[11],
            payload=row[12],
        )

    def _index_rows(
        self, rows: Iterable[Mapping[str, object] | object]
    ) -> MutableMapping[tuple, Mapping[str, object]]:
        indexed: MutableMapping[tuple, Mapping[str, object]] = {}
        for raw in rows:
            normalized = self._normalise_row(raw)
            key = tuple(normalized.get(field) for field in self.KEY_FIELDS)
            indexed[key] = normalized
        return indexed

    def _normalise_row(self, row: Mapping[str, object] | object) -> Mapping[str, object]:
        def pick(*names: str) -> object | None:
            if isinstance(row, Mapping):
                for name in names:
                    if name in row and row[name] is not None:
                        return row[name]
                    upper = name.upper()
                    if upper in row and row[upper] is not None:
                        return row[upper]
            for name in names:
                if hasattr(row, name):
                    value = getattr(row, name)
                    if value is not None:
                        return value
                snake = name.lower()
                if hasattr(row, snake):
                    value = getattr(row, snake)
                    if value is not None:
                        return value
            return None

        def normalise_string(value: object) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text if text else None

        def normalise_int(value: object) -> int | None:
            if value is None or value == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        normalized: dict[str, object] = {}
        normalized["document_id"] = pick("document_id", "DOCUMENT_ID")
        normalized["dtmnfr"] = normalise_string(pick("dtmnfr", "DTMNFR"))
        normalized["orgao"] = normalise_string(pick("orgao", "ORGAO"))
        normalized["tipo"] = normalise_int(pick("tipo", "TIPO"))
        normalized["sigla"] = normalise_string(pick("sigla", "SIGLA"))
        normalized["simbolo"] = normalise_string(pick("simbolo", "SIMBOLO"))
        normalized["nome_lista"] = normalise_string(pick("nome_lista", "NOME_LISTA"))
        normalized["num_ordem"] = normalise_int(pick("num_ordem", "NUM_ORDEM"))
        normalized["nome_candidato"] = normalise_string(
            pick("nome_candidato", "NOME_CANDIDATO")
        )
        normalized["partido_proponente"] = normalise_string(
            pick("partido_proponente", "PARTIDO_PROPONENTE")
        )
        normalized["independente"] = normalise_int(
            pick("independente", "INDEPENDENTE")
        )
        return normalized

    def _build_record(
        self,
        row_a: Mapping[str, object] | None,
        row_b: Mapping[str, object] | None,
    ) -> ComparisonRecord:
        nome_a = (row_a or {}).get("nome_candidato")
        nome_b = (row_b or {}).get("nome_candidato")
        partido_a = (row_a or {}).get("partido_proponente")
        partido_b = (row_b or {}).get("partido_proponente")

        similarity, distance = self._similarity(nome_a, nome_b)
        status: str
        confidence: float

        if row_a and row_b:
            if self._rows_match(row_a, row_b):
                status = "agreement"
                confidence = 1.0
                similarity = 1.0
                distance = 0
            else:
                status = "dispute"
                confidence = similarity
        elif row_a:
            status = "missing_operator_b"
            confidence = 0.0
        else:
            status = "missing_operator_a"
            confidence = 0.0

        payload = json.dumps({
            "operator_a": row_a,
            "operator_b": row_b,
        }, ensure_ascii=False)

        base = row_a or row_b or {}
        return ComparisonRecord(
            document_id=base.get("document_id"),
            orgao=base.get("orgao"),
            tipo=base.get("tipo"),
            num_ordem=base.get("num_ordem"),
            nome_a=nome_a,
            nome_b=nome_b,
            partido_a=partido_a,
            partido_b=partido_b,
            status=status,
            confidence=round(float(confidence), 4),
            similarity=round(float(similarity), 4),
            distance=int(distance),
            payload=payload,
        )

    def _rows_match(
        self,
        row_a: Mapping[str, object],
        row_b: Mapping[str, object],
    ) -> bool:
        for field in self.EXACT_FIELDS:
            value_a = row_a.get(field)
            value_b = row_b.get(field)
            if isinstance(value_a, str) or isinstance(value_b, str):
                if (value_a or "").casefold() != (value_b or "").casefold():
                    return False
            else:
                if value_a != value_b:
                    return False
        return True

    def _similarity(self, value_a: str | None, value_b: str | None) -> tuple[float, int]:
        if not value_a and not value_b:
            return 1.0, 0
        if value_a == value_b:
            return 1.0, 0
        if value_a is None or value_b is None:
            length = len(value_a or value_b or "")
            return 0.0, length

        distance = self._levenshtein(value_a.casefold(), value_b.casefold())
        max_len = max(len(value_a), len(value_b))
        similarity = 1 - (distance / max_len if max_len else 0)
        return similarity, distance

    def _levenshtein(self, source: str, target: str) -> int:
        if source == target:
            return 0
        if not source:
            return len(target)
        if not target:
            return len(source)

        previous = list(range(len(target) + 1))
        for i, char_s in enumerate(source, start=1):
            current = [i]
            for j, char_t in enumerate(target, start=1):
                insert_cost = current[j - 1] + 1
                delete_cost = previous[j] + 1
                replace_cost = previous[j - 1] + (char_s != char_t)
                current.append(min(insert_cost, delete_cost, replace_cost))
            previous = current
        return previous[-1]

    def _persist_records(self, records: Sequence[ComparisonRecord]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            for record in records:
                conn.execute(
                    """
                    DELETE FROM candidate_comparisons
                    WHERE document_id IS ?
                      AND orgao IS ?
                      AND tipo IS ?
                      AND num_ordem IS ?
                    """,
                    (
                        record.document_id,
                        record.orgao,
                        record.tipo,
                        record.num_ordem,
                    ),
                )
            conn.executemany(
                """
                INSERT INTO candidate_comparisons (
                    document_id,
                    orgao,
                    tipo,
                    num_ordem,
                    nome_a,
                    nome_b,
                    partido_a,
                    partido_b,
                    status,
                    confidence,
                    similarity,
                    distance,
                    payload,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [record.as_tuple() for record in records],
            )
            conn.commit()

    def _initialise_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_comparisons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER,
                    orgao TEXT,
                    tipo INTEGER,
                    num_ordem INTEGER,
                    nome_a TEXT,
                    nome_b TEXT,
                    partido_a TEXT,
                    partido_b TEXT,
                    status TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    similarity REAL NOT NULL,
                    distance INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
            )
            conn.commit()
