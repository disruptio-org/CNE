
from typing import List, Tuple
from ..models.schemas import CandidateRecord, RecordDiff, FieldDiff, ReconciliationResult
from .utils import similarity
FIELDS = ["DTMNFR","ORGAO","TIPO","SIGLA","SIMBOLO","NOME_LISTA","NUM_ORDEM","NOME_CANDIDATO","PARTIDO_PROPONENTE","INDEPENDENTE"]
def record_key(r: CandidateRecord) -> str:
    return f"{r.DTMNFR}|{r.ORGAO}|{r.NUM_ORDEM}" if r.DTMNFR else f"{r.ORGAO}|{r.NUM_ORDEM}"
def compare_values(field: str, a, b) -> Tuple[bool, float]:
    if a == b: return True, 1.0
    if field == "INDEPENDENTE":
        return (bool(a) == bool(b)), 1.0 if bool(a) == bool(b) else 0.0
    sa, sb = str(a or ""), str(b or ""); sim = similarity(sa, sb)
    return sim >= 0.97, sim
def reconcile(a: List[CandidateRecord], b: List[CandidateRecord]) -> ReconciliationResult:
    index_a = {record_key(x): x for x in a}; index_b = {record_key(x): x for x in b}
    all_keys = sorted(set(index_a.keys()) | set(index_b.keys()))
    diffs: List[RecordDiff] = []; auto_accept = 0; needs_review = 0
    for k in all_keys:
        ra = index_a.get(k); rb = index_b.get(k)
        if (ra is None) or (rb is None):
            rd = RecordDiff(record_key=k, status="needs_review", field_diffs=[
                FieldDiff(field="__ROW__", A=("present" if ra else "missing"), B=("present" if rb else "missing"),
                          suggestion=None, reason="row presence mismatch")
            ], confidence=0.0)
            needs_review += 1; diffs.append(rd); continue
        row_diffs: List[FieldDiff] = []; all_ok = True; conf_acc = 0.0
        for f in FIELDS:
            va = getattr(ra, f); vb = getattr(rb, f)
            ok, sim = compare_values(f, va, vb); conf_acc += sim
            if not ok:
                all_ok = False
                suggestion = va if (va not in [None, "", 0]) else vb
                row_diffs.append(FieldDiff(field=f, A=str(va), B=str(vb), suggestion=str(suggestion) if suggestion is not None else None, reason=f"similarity={sim:.2f}"))
        conf = conf_acc / len(FIELDS); status = "ok" if all_ok else "needs_review"
        if status == "ok": auto_accept += 1
        else: needs_review += 1
        diffs.append(RecordDiff(record_key=k, status=status, field_diffs=row_diffs, confidence=conf))
    return ReconciliationResult(file_id="", diffs=diffs, auto_accept_count=auto_accept, needs_review_count=needs_review)
