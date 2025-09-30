
import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from ..services import storage
router = APIRouter(prefix="/artifacts", tags=["artifacts"])
def _dir_for(kind: str, file_id: str):
    if kind not in {"input","working","outputs"}: raise HTTPException(status_code=400, detail="kind inválido")
    return storage.file_dir(file_id, kind)
@router.get("/{file_id}/list")
def list_artifacts(file_id: str):
    items = {}; 
    for kind in ["input","working","outputs"]:
        d = _dir_for(kind, file_id); items[kind] = sorted(os.listdir(d)) if os.path.isdir(d) else []
    return {"file_id": file_id, "artifacts": items}
@router.get("/{file_id}/get")
def get_artifact(file_id: str, name: str, kind: str = "working"):
    d = _dir_for(kind, file_id); path = os.path.join(d, name)
    if not os.path.exists(path): raise HTTPException(status_code=404, detail="artifact não encontrado")
    media = "application/octet-stream"
    if name.endswith(".json"): media = "application/json"
    if name.endswith(".csv"): media = "text/csv"
    return FileResponse(path, media_type=media, filename=name)
