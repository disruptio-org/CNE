
from typing import Dict, List
from ..models.schemas import CandidateRecord, SummaryReport
def k(r: CandidateRecord): return f"{r.DTMNFR}|{r.ORGAO}"
def summary_for_file(file_id: str, records: List[CandidateRecord]) -> Dict[str, int]:
    total=len(records); efetivos=sum(1 for r in records if r.TIPO=="2"); suplentes=sum(1 for r in records if r.TIPO=="3")
    orgaos=len(set(k(r) for r in records)); return {"total_registos":total,"efetivos":efetivos,"suplentes":suplentes,"orgaos_unicos":orgaos}
def build_global_summary(per_file: Dict[str, Dict[str, int]]) -> SummaryReport:
    globals={"total_registos":0,"efetivos":0,"suplentes":0,"orgaos_unicos":0}
    for stats in per_file.values():
        for key in globals.keys(): globals[key]+=stats.get(key,0)
    return SummaryReport(total_files=len(per_file), per_file=per_file, globals=globals)
