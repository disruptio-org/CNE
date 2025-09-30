
from typing import List, Dict, Any
from ..models.schemas import CandidateRecord, ValidationIssue, ValidationReport
def validate_schema(recs: List[CandidateRecord]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    for r in recs:
        key = f"{r.DTMNFR}|{r.ORGAO}|{r.NUM_ORDEM}"
        if r.ORGAO not in {"AM","CM","AF"}:
            issues.append(ValidationIssue(level="ERROR", record_key=key, message=f"ORGAO inválido: {r.ORGAO}"))
        if r.TIPO not in {"2","3"}:
            issues.append(ValidationIssue(level="ERROR", record_key=key, message=f"TIPO inválido: {r.TIPO}"))
        if not r.NOME_CANDIDATO:
            issues.append(ValidationIssue(level="ERROR", record_key=key, message="NOME_CANDIDATO vazio"))
        if r.NUM_ORDEM is None or int(r.NUM_ORDEM) <= 0:
            issues.append(ValidationIssue(level="ERROR", record_key=key, message="NUM_ORDEM inválido"))
        if not r.PARTIDO_PROPONENTE:
            issues.append(ValidationIssue(level="WARN", record_key=key, message="PARTIDO_PROPONENTE vazio"))
    return issues
def build_validation_report(file_id: str, recs: List[CandidateRecord], meta: Dict[str, Any]) -> ValidationReport:
    issues = validate_schema(recs)
    valid = not any(i.level == "ERROR" for i in issues)
    return ValidationReport(file_id=file_id, issues=issues, valid=valid)
