
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

CSV_FIELDS = [
    "DTMNFR","ORGAO","TIPO","SIGLA","SIMBOLO","NOME_LISTA",
    "NUM_ORDEM","NOME_CANDIDATO","PARTIDO_PROPONENTE","INDEPENDENTE"
]

class CandidateRecord(BaseModel):
    DTMNFR: str
    ORGAO: Literal["AM","CM","AF"]
    TIPO: Literal["2","3"]
    SIGLA: str
    SIMBOLO: Optional[str] = ""
    NOME_LISTA: Optional[str] = ""
    NUM_ORDEM: int
    NOME_CANDIDATO: str
    PARTIDO_PROPONENTE: Optional[str] = ""
    INDEPENDENTE: Optional[bool] = False

class FieldDiff(BaseModel):
    field: str
    A: Optional[str] = None
    B: Optional[str] = None
    suggestion: Optional[str] = None
    reason: Optional[str] = None

class RecordDiff(BaseModel):
    record_key: str
    status: Literal["ok","needs_review"]
    field_diffs: List[FieldDiff] = Field(default_factory=list)
    confidence: float = 0.0

class ReconciliationResult(BaseModel):
    file_id: str
    diffs: List[RecordDiff]
    auto_accept_count: int = 0
    needs_review_count: int = 0

class ValidationIssue(BaseModel):
    level: Literal["ERROR","WARN","INFO"]
    record_key: Optional[str] = None
    message: str

class ValidationReport(BaseModel):
    file_id: str
    issues: List[ValidationIssue]
    valid: bool

class ResolveDecision(BaseModel):
    record_key: str
    decision: Dict[str, Any]

class ResolvePayload(BaseModel):
    decisions: List[ResolveDecision]

class SummaryReport(BaseModel):
    total_files: int
    per_file: Dict[str, Dict[str, int]]
    globals: Dict[str, int]
