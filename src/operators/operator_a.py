"""Operator A: extract candidate rows from textual or structured sources.

This module provides :class:`OperatorA`, a lightweight parser that can read OCR
transcripts, plain text dumps, or structured data sources and produce rows that
match the downstream electoral schema. The operator keeps a persistent history
of the generated rows per document so later comparison steps can track deltas
between runs.

The extraction is intentionally heuristic-driven: electoral lists tend to follow
simple numbering patterns (``1. Name - PARTY``) and group candidates under
section headers such as ``Efetivos`` or ``Suplentes``. The implementation keeps
track of the active section, assigns the correct ``TIPO`` values, and detects
``(independente)`` annotations.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence

__all__ = ["CandidateRow", "OperatorA"]


@dataclass(slots=True)
class CandidateRow:
    """Representation of a candidate row produced by Operator A."""

    document_id: int
    dtmnfr: str
    orgao: str
    tipo: int
    sigla: str
    simbolo: str
    nome_lista: str
    num_ordem: int
    nome_candidato: str
    partido_proponente: Optional[str]
    independente: int

    def as_tuple(self) -> tuple:
        """Return the row as an ordered tuple for database insertion."""

        return (
            self.document_id,
            self.dtmnfr,
            self.orgao,
            self.tipo,
            self.sigla,
            self.simbolo,
            self.nome_lista,
            self.num_ordem,
            self.nome_candidato,
            self.partido_proponente,
            self.independente,
            datetime.now(timezone.utc).isoformat(),
        )


class OperatorA:
    """Extract candidates aligned with the schema from the provided sources."""

    SECTION_TIPOS: Mapping[str, int] = {
        "efetivo": 2,
        "efetivos": 2,
        "titular": 2,
        "titulares": 2,
        "suplente": 3,
        "suplentes": 3,
    }

    def __init__(self, db_path: Path | str = Path("data/documents.db")) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise_db()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(
        self,
        document_id: int,
        *,
        text: str | None = None,
        text_path: str | Path | None = None,
        structured_rows: Iterable[Mapping[str, object]] | None = None,
        dtmnfr: str | None = None,
        orgao: str | None = None,
        sigla: str | None = None,
        simbolo: str | None = None,
        nome_lista: str | None = None,
        default_tipo: int = 1,
    ) -> List[CandidateRow]:
        """Run extraction for the provided document.

        Parameters
        ----------
        document_id:
            Identifier of the document this extraction corresponds to.
        text:
            Raw text content to parse. When ``None`` the ``text_path`` or
            ``structured_rows`` arguments must provide the data source.
        text_path:
            Optional path pointing at a plaintext/OCR transcript. When provided
            it is read and treated as ``text``.
        structured_rows:
            Iterable of mappings that already contain structured data. Keys
            matching the schema are used directly and missing sequencing is
            filled in.
        dtmnfr, orgao, sigla, simbolo, nome_lista:
            Metadata that will be applied to each generated row when the source
            does not supply the fields directly.
        default_tipo:
            ``TIPO`` value to use when no section headers or structured values
            specify it.
        """

        if text_path and text:
            raise ValueError("Provide either text or text_path, not both.")
        if text_path:
            text = Path(text_path).read_text(encoding="utf-8")
        if text is None and structured_rows is None:
            raise ValueError("Either text/text_path or structured_rows must be provided.")

        metadata = self._normalise_metadata(
            document_id=document_id,
            dtmnfr=dtmnfr,
            orgao=orgao,
            sigla=sigla,
            simbolo=simbolo,
            nome_lista=nome_lista,
        )

        rows: List[CandidateRow] = []
        if structured_rows is not None:
            rows.extend(
                self._rows_from_structured(structured_rows, metadata, default_tipo)
            )
        if text:
            rows.extend(self._rows_from_text(text, metadata, default_tipo))

        if not rows:
            return []

        self._persist_rows(rows)
        return rows

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _rows_from_text(
        self,
        text: str,
        metadata: Mapping[str, str | int],
        default_tipo: int,
    ) -> List[CandidateRow]:
        counters: MutableMapping[int, int] = defaultdict(int)
        current_tipo = default_tipo
        rows: List[CandidateRow] = []

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            detected_tipo = self._detect_section_tipo(line, current_tipo)
            if detected_tipo != current_tipo:
                current_tipo = detected_tipo
            candidate = self._parse_candidate_line(
                line,
                current_tipo,
                counters,
                metadata,
            )
            if candidate:
                rows.append(candidate)
        return rows

    def _rows_from_structured(
        self,
        structured_rows: Iterable[Mapping[str, object]],
        metadata: Mapping[str, str | int],
        default_tipo: int,
    ) -> List[CandidateRow]:
        counters: MutableMapping[int, int] = defaultdict(int)
        rows: List[CandidateRow] = []
        for item in structured_rows:
            tipo = self._coerce_int(item.get("TIPO"), default_tipo)
            counters[tipo] += 1
            num_ordem = self._coerce_int(item.get("NUM_ORDEM"), counters[tipo])
            counters[tipo] = num_ordem
            raw_partido = self._normalise_string(item.get("PARTIDO_PROPONENTE"))
            raw_nome = self._choose_value(item.get("NOME_CANDIDATO"), "")
            partido = self._strip_independent_token(raw_partido) or None
            indep = self._detect_independent(
                partido,
                raw_nome,
                self._normalise_string(item.get("SIGLA")),
            )
            rows.append(
                CandidateRow(
                    document_id=int(metadata["document_id"]),
                    dtmnfr=self._choose_value(item.get("DTMNFR"), metadata["dtmnfr"]),
                    orgao=self._choose_value(item.get("ORGAO"), metadata["orgao"]),
                    tipo=tipo,
                    sigla=self._choose_value(item.get("SIGLA"), metadata["sigla"]),
                    simbolo=self._choose_value(item.get("SIMBOLO"), metadata["simbolo"]),
                    nome_lista=self._choose_value(item.get("NOME_LISTA"), metadata["nome_lista"]),
                    num_ordem=num_ordem,
                    nome_candidato=self._strip_independent_token(raw_nome),
                    partido_proponente=partido,
                    independente=int(item.get("INDEPENDENTE", indep) or indep),
                )
            )
        return rows

    def _detect_section_tipo(self, line: str, current_tipo: int) -> int:
        lowered = line.lower()
        for token, tipo in self.SECTION_TIPOS.items():
            if token in lowered:
                return tipo
        return current_tipo

    def _parse_candidate_line(
        self,
        line: str,
        current_tipo: int,
        counters: MutableMapping[int, int],
        metadata: Mapping[str, str | int],
    ) -> Optional[CandidateRow]:
        match = re.match(r"^(?P<num>\d{1,3})[\).\-\s]+(?P<body>.+)$", line)
        if not match:
            return None
        num_ordem = int(match.group("num"))
        counters[current_tipo] = num_ordem
        body = match.group("body").strip()
        name, partido, indep = self._split_candidate_body(body)

        return CandidateRow(
            document_id=int(metadata["document_id"]),
            dtmnfr=str(metadata["dtmnfr"]),
            orgao=str(metadata["orgao"]),
            tipo=current_tipo,
            sigla=str(metadata["sigla"]),
            simbolo=str(metadata["simbolo"]),
            nome_lista=str(metadata["nome_lista"]),
            num_ordem=num_ordem,
            nome_candidato=name,
            partido_proponente=partido,
            independente=int(indep),
        )

    def _split_candidate_body(self, body: str) -> tuple[str, Optional[str], int]:
        candidato = body.strip()
        partido: Optional[str] = None
        indep = 0

        if re.search(r"\(\s*independente\s*\)", candidato, flags=re.IGNORECASE):
            indep = 1
        candidato = self._strip_independent_token(candidato)
        # Parenthesised party information.
        paren_match = re.search(r"\(([^)]+)\)", candidato)
        if paren_match:
            raw = paren_match.group(1).strip()
            candidato = re.sub(r"\(([^)]+)\)", "", candidato).strip()
            if "independente" in raw.lower():
                indep = 1
            elif raw:
                partido = raw
        # Split on dash separators for party names.
        if partido is None:
            for sep in (" - ", " – ", " — "):
                if sep in candidato:
                    name_part, party_part = candidato.split(sep, 1)
                    partido = party_part.strip() or None
                    candidato = name_part.strip()
                    break
        if partido and "independente" in partido.lower():
            indep = 1
            partido = self._strip_independent_token(partido) or None
        candidato = re.sub(r"\s{2,}", " ", candidato).strip("-–— ")
        return candidato, partido, indep

    def _detect_independent(
        self,
        partido: Optional[str],
        nome_candidato: Optional[str],
        sigla: Optional[str],
    ) -> int:
        tokens = " ".join(filter(None, [partido, nome_candidato, sigla])).lower()
        return 1 if "independente" in tokens else 0

    def _strip_independent_token(self, value: str) -> str:
        cleaned = re.sub(
            r"\(\s*independente\s*\)",
            "",
            value,
            flags=re.IGNORECASE,
        )
        cleaned = cleaned.replace("independente", "")
        return cleaned.strip(" -–—\t ")

    def _coerce_int(self, value: object, fallback: int) -> int:
        try:
            if value is None:
                raise ValueError
            return int(value)
        except (TypeError, ValueError):
            return int(fallback)

    def _choose_value(self, value: object, fallback: object) -> str:
        if value is None:
            return str(fallback or "")
        return str(value)

    def _normalise_string(self, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _normalise_metadata(
        self,
        *,
        document_id: int,
        dtmnfr: str | None,
        orgao: str | None,
        sigla: str | None,
        simbolo: str | None,
        nome_lista: str | None,
    ) -> Mapping[str, str | int]:
        return {
            "document_id": document_id,
            "dtmnfr": dtmnfr or "",
            "orgao": orgao or "",
            "sigla": sigla or "",
            "simbolo": simbolo or "",
            "nome_lista": nome_lista or "",
        }

    def _persist_rows(self, rows: Sequence[CandidateRow]) -> None:
        if not rows:
            return
        document_id = rows[0].document_id
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM operator_a_results WHERE document_id = ?",
                (document_id,),
            )
            conn.executemany(
                """
                INSERT INTO operator_a_results (
                    document_id,
                    dtmnfr,
                    orgao,
                    tipo,
                    sigla,
                    simbolo,
                    nome_lista,
                    num_ordem,
                    nome_candidato,
                    partido_proponente,
                    independente,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [row.as_tuple() for row in rows],
            )
            conn.commit()

    def _initialise_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_a_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER NOT NULL,
                    dtmnfr TEXT,
                    orgao TEXT,
                    tipo INTEGER NOT NULL,
                    sigla TEXT,
                    simbolo TEXT,
                    nome_lista TEXT,
                    num_ordem INTEGER NOT NULL,
                    nome_candidato TEXT NOT NULL,
                    partido_proponente TEXT,
                    independente INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(document_id, tipo, num_ordem, nome_candidato)
                )
                """
            )
            conn.commit()
