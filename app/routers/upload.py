
import os
from fastapi import APIRouter, UploadFile, File
from typing import List
from ..services import storage
router = APIRouter(prefix="/upload", tags=["upload"])
@router.post("")
async def upload_files(files: List[UploadFile] = File(...)):
    storage.ensure_dirs(); file_id = storage.new_file_id()
    input_dir = storage.file_dir(file_id, "input")
    for f in files:
        dest = os.path.join(input_dir, f.filename)
        with open(dest, "wb") as out: out.write(await f.read())
    return {"file_id": file_id, "files": [f.filename for f in files]}
@router.get("/list")
def list_files(): return storage.list_files_status()
