"""Database helpers for the review dashboard."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_ALLOWED_SOURCES = {"operator_a", "operator_b", "manual", "agreement"}


class ReviewService:
    """High level helper exposing review-friendly database queries."""

    def __init__(self, db_path: Path | str = Path("data/documents.db")) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise_db()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def list_documents_with_disputes(self) -> List[Dict[str, Any]]:
        """Return a summary of documents that still contain disputes."""

        query = (
            """
            SELECT
                c.document_id,
                COALESCE(d.file_name, 'Unknown document') AS file_name,
                COALESCE(d.status, 'MISSING') AS document_status,
                COUNT(*) AS dispute_count,
                MAX(c.created_at) AS latest_activity
            FROM candidate_comparisons AS c
            LEFT JOIN documents AS d ON d.id = c.document_id
            WHERE c.status = 'dispute'
            GROUP BY c.document_id, file_name, document_status
            ORDER BY latest_activity DESC
            """
        )

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query)
            rows = cursor.fetchall()

        return [
            {
                "document_id": row[0],
                "file_name": row[1],
                "document_status": row[2],
                "dispute_count": row[3],
                "latest_activity": row[4],
            }
            for row in rows
        ]

    def fetch_comparisons(
        self,
        document_id: int,
        *,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return comparison rows for a given document.

        Parameters
        ----------
        document_id:
            Identifier of the document whose comparison rows should be
            retrieved.
        status:
            Optional filter to return only rows with a specific status (e.g.
            ``"dispute"`` or ``"agreement"``).
        """

        base_query = (
            """
            SELECT
                c.id,
                c.document_id,
                c.status,
                c.nome_a,
                c.nome_b,
                c.partido_a,
                c.partido_b,
                c.payload,
                d.selected_source,
                d.final_value,
                d.comment,
                d.reviewer,
                d.decided_at
            FROM candidate_comparisons AS c
            LEFT JOIN review_decisions AS d ON d.comparison_id = c.id
            WHERE c.document_id = ?
            """
        )
        params: List[Any] = [document_id]
        if status:
            base_query += " AND c.status = ?"
            params.append(status)
        base_query += " ORDER BY c.num_ordem IS NULL, c.num_ordem, c.id"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(base_query, params)
            rows = cursor.fetchall()

        comparisons: List[Dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row[7]) if row[7] else {}
            comparisons.append(
                {
                    "comparison_id": row[0],
                    "document_id": row[1],
                    "status": row[2],
                    "nome_a": row[3],
                    "nome_b": row[4],
                    "partido_a": row[5],
                    "partido_b": row[6],
                    "payload": payload,
                    "selected_source": row[8],
                    "final_value": row[9],
                    "comment": row[10],
                    "reviewer": row[11],
                    "decided_at": row[12],
                }
            )
        return comparisons

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------
    def save_decision(
        self,
        *,
        comparison_id: int,
        document_id: int,
        selected_source: str,
        final_value: Optional[str],
        comment: Optional[str] = None,
        reviewer: Optional[str] = None,
    ) -> None:
        """Persist a reviewer decision for a specific comparison row."""

        self._ensure_document_editable(document_id)

        if selected_source not in _ALLOWED_SOURCES:
            raise ValueError(
                f"Unsupported source {selected_source!r}. Allowed values: {sorted(_ALLOWED_SOURCES)}"
            )

        decided_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO review_decisions (
                    comparison_id,
                    document_id,
                    reviewer,
                    selected_source,
                    final_value,
                    comment,
                    decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(comparison_id) DO UPDATE SET
                    document_id = excluded.document_id,
                    reviewer = excluded.reviewer,
                    selected_source = excluded.selected_source,
                    final_value = excluded.final_value,
                    comment = excluded.comment,
                    decided_at = excluded.decided_at
                """,
                (
                    comparison_id,
                    document_id,
                    reviewer,
                    selected_source,
                    final_value,
                    comment,
                    decided_at,
                ),
            )
            conn.commit()

    def bulk_accept_agreements(
        self,
        *,
        document_id: int,
    ) -> int:
        """Automatically accept agreement rows for the provided document.

        Returns
        -------
        int
            Number of comparison rows that were marked as accepted.
        """

        self._ensure_document_editable(document_id)

        fetch_query = (
            """
            SELECT id, payload
            FROM candidate_comparisons
            WHERE document_id = ? AND status = 'agreement'
              AND id NOT IN (SELECT comparison_id FROM review_decisions)
            """
        )

        decisions: List[tuple[int, Optional[str]]] = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(fetch_query, (document_id,))
            for comparison_id, payload in cursor.fetchall():
                try:
                    parsed = json.loads(payload or "{}")
                except json.JSONDecodeError:
                    parsed = {}
                candidate_a = (parsed.get("operator_a") or {}).get("nome_candidato")
                candidate_b = (parsed.get("operator_b") or {}).get("nome_candidato")
                final_value = candidate_a or candidate_b
                decisions.append((comparison_id, final_value))

            for comparison_id, final_value in decisions:
                conn.execute(
                    """
                    INSERT INTO review_decisions (
                        comparison_id,
                        document_id,
                        reviewer,
                        selected_source,
                        final_value,
                        comment,
                        decided_at
                    ) VALUES (?, ?, NULL, 'agreement', ?, NULL, ?)
                    ON CONFLICT(comparison_id) DO NOTHING
                    """,
                    (
                        comparison_id,
                        document_id,
                        final_value,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            conn.commit()
        return len(decisions)

    def approve_document(
        self,
        *,
        document_id: int,
        approver_id: str,
        summary: Optional[str] = None,
    ) -> None:
        """Mark a document as approved and capture an audit log entry."""

        if not approver_id or not approver_id.strip():
            raise ValueError("An approver identifier is required to approve a document.")

        approval_time = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT status FROM documents WHERE id = ?",
                (document_id,),
            )
            row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Document {document_id} not found.")

            current_status = row[0]
            if str(current_status or "").upper() == "APPROVED":
                raise ValueError(f"Document {document_id} is already approved.")

            conn.execute(
                "UPDATE documents SET status = 'APPROVED' WHERE id = ?",
                (document_id,),
            )
            conn.execute(
                """
                INSERT INTO audit_log (
                    document_id,
                    actor_id,
                    action,
                    summary,
                    created_at
                ) VALUES (?, ?, 'approve_document', ?, ?)
                """,
                (document_id, approver_id.strip(), summary, approval_time),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Document helpers
    # ------------------------------------------------------------------
    def get_document_snippet(self, document_id: int, *, max_length: int = 600) -> Dict[str, Any]:
        """Return a small textual snippet for the requested document."""

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT file_name, ocr_text_path FROM documents WHERE id = ?",
                (document_id,),
            )
            row = cursor.fetchone()

        if row is None:
            return {
                "document_id": document_id,
                "file_name": None,
                "snippet": "Document metadata not found.",
            }

        file_name, text_path = row
        snippet = "No OCR text available for this document."
        if text_path:
            path = Path(text_path)
            if not path.is_absolute():
                path = (self.db_path.parent / path).resolve()
            if path.exists():
                try:
                    text = path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    text = ""
                snippet = text.strip()[:max_length] or "OCR text file is empty."
            else:
                snippet = f"OCR text not found at {path}."

        return {
            "document_id": document_id,
            "file_name": file_name,
            "snippet": snippet,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _initialise_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS review_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comparison_id INTEGER NOT NULL UNIQUE,
                    document_id INTEGER,
                    reviewer TEXT,
                    selected_source TEXT NOT NULL,
                    final_value TEXT,
                    comment TEXT,
                    decided_at TEXT NOT NULL,
                    FOREIGN KEY (comparison_id) REFERENCES candidate_comparisons(id)
                )
                """,
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER NOT NULL,
                    actor_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    summary TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (document_id) REFERENCES documents(id)
                )
                """,
            )
            conn.commit()

    def _ensure_document_editable(self, document_id: int) -> None:
        status = self._get_document_status(document_id)
        if status is None:
            raise ValueError(f"Document {document_id} not found.")
        if str(status).upper() == "APPROVED":
            raise ValueError(
                f"Document {document_id} has already been approved and can no longer be edited."
            )

    def _get_document_status(self, document_id: int) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT status FROM documents WHERE id = ?",
                (document_id,),
            )
            row = cursor.fetchone()
        return row[0] if row else None
