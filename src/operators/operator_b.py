"""Operator B: alternative heuristics for extracting candidate rows.

Unlike :mod:`operator_a`, this implementation focuses on parsing table-like
layouts frequently produced by OCR tools that preserve column alignment.  The
heuristics operate in two stages: first, rows are segmented using column
splitting (pipes, tabulation or wide spacing), and then residual inline
patterns are captured with regular expressions.  The resulting rows mimic the
schema emitted by Operator A so downstream comparison jobs can audit
consistency between both approaches.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence

from .operator_a import CandidateRow

__all__ = ["OperatorB"]


class OperatorB:
    """Extract candidates using table-oriented heuristics."""

    SECTION_PATTERNS = (
        (re.compile(r"\bEfetiv(?:o|a)s?\b", re.IGNORECASE), 2),
        (re.compile(r"\bTitular(?:es)?\b", re.IGNORECASE), 2),
        (re.compile(r"\bSuplent(?:e|es)\b", re.IGNORECASE), 3),
    )

    ROW_PATTERN = re.compile(r"^\s*(?P<num>\d{1,3})[\s\).:;-]+(?P<body>.+)$")
    COLUMN_SPLITTER = re.compile(r"\s{2,}|\t|\s?\|\s?|;")
    PAREN_CONTENT = re.compile(r"\(([^)]+)\)")
    INDEPENDENT_TOKEN = re.compile(r"\(\s*independente\s*\)", re.IGNORECASE)

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
    # Text extraction heuristics
    # ------------------------------------------------------------------
    def _rows_from_text(
        self,
        text: str,
        metadata: Mapping[str, str | int],
        default_tipo: int,
    ) -> List[CandidateRow]:
        current_tipo = default_tipo
        counters: MutableMapping[int, int] = defaultdict(int)
        rows: List[CandidateRow] = []

        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                continue

            detected = self._detect_section(line)
            if detected is not None:
                current_tipo = detected
                continue

            candidate = self._parse_table_line(
                line=line,
                current_tipo=current_tipo,
                counters=counters,
                metadata=metadata,
            )
            if candidate:
                rows.append(candidate)
                continue

            candidate = self._parse_inline_line(
                line=line,
                current_tipo=current_tipo,
                counters=counters,
                metadata=metadata,
            )
            if candidate:
                rows.append(candidate)
        return rows

    def _detect_section(self, line: str) -> Optional[int]:
        lowered = line.lower()
        for pattern, tipo in self.SECTION_PATTERNS:
            if pattern.search(lowered):
                return tipo
        return None

    def _parse_table_line(
        self,
        *,
        line: str,
        current_tipo: int,
        counters: MutableMapping[int, int],
        metadata: Mapping[str, str | int],
    ) -> Optional[CandidateRow]:
        columns = [cell.strip() for cell in self.COLUMN_SPLITTER.split(line) if cell.strip()]
        if len(columns) < 2:
            return None

        number = self._extract_number(columns[0])
        if number is None:
            return None

        candidate_blob = columns[1]
        party_blob = " ".join(columns[2:]) if len(columns) > 2 else ""

        name, partido, indep = self._normalise_candidate(candidate_blob, party_blob)
        num_ordem = self._ensure_continuity(counters, current_tipo, number)

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

    def _parse_inline_line(
        self,
        *,
        line: str,
        current_tipo: int,
        counters: MutableMapping[int, int],
        metadata: Mapping[str, str | int],
    ) -> Optional[CandidateRow]:
        match = self.ROW_PATTERN.match(line)
        if not match:
            return None

        number = self._extract_number(match.group("num"))
        if number is None:
            return None

        body = match.group("body").strip()
        name, partido, indep = self._normalise_candidate(body, "")
        num_ordem = self._ensure_continuity(counters, current_tipo, number)

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

    # ------------------------------------------------------------------
    # Structured extraction
    # ------------------------------------------------------------------
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
            number = self._extract_number(item.get("NUM_ORDEM"))
            num_ordem = self._ensure_continuity(counters, tipo, number)

            raw_name = self._choose_value(item.get("NOME_CANDIDATO"), "")
            raw_party = self._choose_value(item.get("PARTIDO_PROPONENTE"), "")

            name, partido, indep = self._normalise_candidate(raw_name, raw_party)
            indep_flag = self._coerce_int(item.get("INDEPENDENTE"), indep)

            rows.append(
                CandidateRow(
                    document_id=int(metadata["document_id"]),
                    dtmnfr=self._choose_value(item.get("DTMNFR"), metadata["dtmnfr"]),
                    orgao=self._choose_value(item.get("ORGAO"), metadata["orgao"]),
                    tipo=tipo,
                    sigla=self._choose_value(item.get("SIGLA"), metadata["sigla"]),
                    simbolo=self._choose_value(item.get("SIMBOLO"), metadata["simbolo"]),
                    nome_lista=self._choose_value(
                        item.get("NOME_LISTA"), metadata["nome_lista"]
                    ),
                    num_ordem=num_ordem,
                    nome_candidato=name,
                    partido_proponente=partido,
                    independente=int(indep_flag),
                )
            )
        return rows

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _normalise_candidate(
        self, candidate_blob: str, party_blob: str
    ) -> tuple[str, Optional[str], int]:
        indep = 0

        candidate_text, indep_candidate = self._strip_independent(candidate_blob)
        indep = max(indep, indep_candidate)

        party_text, indep_party = self._strip_independent(party_blob)
        indep = max(indep, indep_party)

        party: Optional[str] = None

        paren_matches = list(self.PAREN_CONTENT.finditer(candidate_text))
        if paren_matches:
            last = paren_matches[-1]
            extracted = last.group(1).strip()
            candidate_text = self.PAREN_CONTENT.sub("", candidate_text).strip()
            if extracted:
                party = extracted

        if party_text:
            party = party_text if party_text else party

        if party and "independente" in party.lower():
            party, flag = self._strip_independent(party)
            indep = max(indep, flag)
            party = party or None

        candidate_text = re.sub(r"\s{2,}", " ", candidate_text).strip(" -–—")
        if party:
            party = re.sub(r"\s{2,}", " ", party).strip(" -–—") or None

        return candidate_text, party, indep

    def _strip_independent(self, value: str) -> tuple[str, int]:
        if not value:
            return "", 0
        flag = 0

        def mark_flag(match: re.Match[str]) -> str:
            nonlocal flag
            flag = 1
            return ""

        cleaned = self.INDEPENDENT_TOKEN.sub(mark_flag, value)
        if "independente" in cleaned.lower():
            cleaned = re.sub(r"independente", "", cleaned, flags=re.IGNORECASE)
            flag = 1
        cleaned = cleaned.strip()
        return cleaned, flag

    def _extract_number(self, value: object) -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip()
        match = re.search(r"\d{1,3}", text)
        if not match:
            return None
        try:
            return int(match.group(0))
        except ValueError:
            return None

    def _ensure_continuity(
        self,
        counters: MutableMapping[int, int],
        tipo: int,
        number: Optional[int],
    ) -> int:
        current = counters.get(tipo, 0)
        if number is None:
            current += 1
            counters[tipo] = current
            return current
        if number <= current:
            current += 1
            counters[tipo] = current
            return current
        counters[tipo] = number
        return number

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

    # ------------------------------------------------------------------
    # Persistence layer
    # ------------------------------------------------------------------
    def _persist_rows(self, rows: Sequence[CandidateRow]) -> None:
        if not rows:
            return
        document_id = rows[0].document_id
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM operator_b_results WHERE document_id = ?",
                (document_id,),
            )
            conn.executemany(
                """
                INSERT INTO operator_b_results (
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
                [
                    (
                        row.document_id,
                        row.dtmnfr,
                        row.orgao,
                        row.tipo,
                        row.sigla,
                        row.simbolo,
                        row.nome_lista,
                        row.num_ordem,
                        row.nome_candidato,
                        row.partido_proponente,
                        row.independente,
                        datetime.now(timezone.utc).isoformat(),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def _initialise_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_b_results (
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
                """,
            )
            conn.commit()
