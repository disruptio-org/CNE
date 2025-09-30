
import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from ..services import storage, exporter
from ..models.schemas import CandidateRecord
from ..services.storage import read_json
router = APIRouter(prefix="/export", tags=["export"])
@router.get("/{file_id}/csv")
def export_csv(file_id: str, source: str = "final"):
    work_dir = storage.file_dir(file_id, "working")
    if source == "final": path = os.path.join(work_dir, "final_records.json")
    elif source.upper() == "A": path = os.path.join(work_dir, "A_records.json")
    elif source.upper() == "B": path = os.path.join(work_dir, "B_records.json")
    elif source.lower() == "validated": path = os.path.join(work_dir, "validated_records.json")
    else: raise HTTPException(status_code=400, detail="source inválido")
    if not os.path.exists(path): raise HTTPException(status_code=400, detail=f"Não existem registos em {source}.")
    recs = [CandidateRecord(**x) for x in read_json(path)]
    out_dir = storage.file_dir(file_id, "outputs")
    filename = f"{file_id}_{source}.csv"; out_path = os.path.join(out_dir, filename)
    exporter.export_csv(out_path, recs); return FileResponse(out_path, media_type="text/csv", filename=filename)
