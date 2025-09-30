
import os
from fastapi import APIRouter
from ..services import storage, reporter
from ..models.schemas import CandidateRecord
from ..services.storage import read_json
router = APIRouter(prefix="/report", tags=["report"])
@router.get("/summary")
def summary():
    per_file = {}
    for f in storage.list_files_status():
        fid = f["file_id"]; work_dir = storage.file_dir(fid, "working")
        path = os.path.join(work_dir, "validated_records.json")
        if not os.path.exists(path): path = os.path.join(work_dir, "final_records.json")
        if not os.path.exists(path): continue
        recs = [CandidateRecord(**x) for x in read_json(path)]
        per_file[fid] = reporter.summary_for_file(fid, recs)
    return reporter.build_global_summary(per_file)
