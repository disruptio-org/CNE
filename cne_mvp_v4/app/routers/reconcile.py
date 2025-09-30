
import os
from fastapi import APIRouter, HTTPException
from ..services import storage, reconciler
from ..models.schemas import CandidateRecord
from ..services.storage import read_json, save_json
router = APIRouter(prefix="/reconcile", tags=["reconcile"])
@router.post("/{file_id}")
def reconcile_file(file_id: str):
    work_dir = storage.file_dir(file_id, "working")
    A_path = os.path.join(work_dir, "A_records.json")
    B_path = os.path.join(work_dir, "B_records.json")
    if not (os.path.exists(A_path) and os.path.exists(B_path)):
        raise HTTPException(status_code=400, detail="Execute /process primeiro (A e B).")
    A = [CandidateRecord(**x) for x in read_json(A_path)]
    B = [CandidateRecord(**x) for x in read_json(B_path)]
    res = reconciler.reconcile(A, B); res.file_id = file_id
    save_json(os.path.join(work_dir, "diffs.json"), res.model_dump()); return res
