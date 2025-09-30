
import os
from fastapi import APIRouter, HTTPException
from ..services import storage, validator, dtmnfr_mapper
from ..models.schemas import CandidateRecord
from ..services.storage import read_json, save_json, load_metadata
router = APIRouter(prefix="/validate", tags=["validate"])
@router.post("/{file_id}")
def validate_file(file_id: str, use_final: bool = True, metadata_name: str = "metadata.json", autofill_dtmnfr: bool = True):
    work_dir = storage.file_dir(file_id, "working")
    path = os.path.join(work_dir, "final_records.json" if use_final else "B_records.json")
    if not os.path.exists(path): raise HTTPException(status_code=400, detail="Sem registos (corra /resolve ou /process).")
    recs = [CandidateRecord(**x) for x in read_json(path)]
    meta = load_metadata(metadata_name) or {}
    file_meta_path = os.path.join(work_dir, "file_meta.json")
    file_meta = read_json(file_meta_path) if os.path.exists(file_meta_path) else {}
    if autofill_dtmnfr: recs, autofill_issues = dtmnfr_mapper.autofill_dtmnfr(recs, file_meta, meta)
    else: autofill_issues = []
    report = validator.build_validation_report(file_id, recs, meta); report.issues.extend(autofill_issues)
    save_json(os.path.join(work_dir, "validation.json"), report.model_dump())
    save_json(os.path.join(work_dir, "validated_records.json"), [r.model_dump() for r in recs])
    return report
