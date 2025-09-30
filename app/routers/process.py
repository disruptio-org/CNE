
import os
from fastapi import APIRouter, HTTPException
from typing import List
from ..services import storage, extractor_a, extractor_b, normalizer, context_extractor
router = APIRouter(prefix="/process", tags=["process"])
def load_input_files(file_id: str) -> List[str]:
    d = storage.file_dir(file_id, "input"); files = [os.path.join(d, x) for x in os.listdir(d)]
    if not files: raise HTTPException(status_code=404, detail="Nenhum ficheiro para este file_id.")
    return files
@router.post("/{file_id}")
def process_file(file_id: str, models: str = "AB"):
    files = load_input_files(file_id); work_dir = storage.file_dir(file_id, "working")
    all_records_A = []; all_records_B = []
    for path in files:
        if "A" in models:
            rawA = extractor_a.extract(path); all_records_A.extend(normalizer.normalize_records(rawA))
        if "B" in models:
            rawB = extractor_b.extract(path); all_records_B.extend(normalizer.normalize_records(rawB))
    from ..services.storage import save_json
    save_json(os.path.join(work_dir, "A_records.json"), [r.model_dump() for r in all_records_A])
    save_json(os.path.join(work_dir, "B_records.json"), [r.model_dump() for r in all_records_B])
    # meta contexto
    meta_candidates = []
    for path in files:
        meta = context_extractor.detect_context(path)
        if meta: meta_candidates.append(meta)
    file_meta = {}
    if meta_candidates:
        for k in ["ORGAO","MUNICIPIO","FREGUESIA"]:
            vals = [m.get(k) for m in meta_candidates if m.get(k)]
            file_meta[k] = vals[0] if vals else None
    save_json(os.path.join(work_dir, "file_meta.json"), file_meta)
    return {"file_id": file_id, "A_count": len(all_records_A), "B_count": len(all_records_B), "file_meta": file_meta}
