
import os, re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException

from ..models.schemas import CandidateRecord, ResolvePayload
from ..services import storage
from ..services.storage import read_json, save_json

router = APIRouter(prefix="/resolve", tags=["resolve"])


def _key(r: CandidateRecord) -> str:
    return f"{r.DTMNFR}|{r.ORGAO}|{r.NUM_ORDEM}" if r.DTMNFR else f"{r.ORGAO}|{r.NUM_ORDEM}"


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y", "sim"}:
            return True
        if lowered in {"false", "f", "0", "no", "n", "nao", "não", "não"}:
            return False
        if not lowered:
            return False
    return bool(value)


def _coerce_field_value(field: str, value: Any) -> Any:
    if field == "NUM_ORDEM":
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"Valor inválido para {field}: {value!r}")
    if field == "INDEPENDENTE":
        normalized = _normalize_bool(value)
        if normalized is None:
            return None
        return normalized
    return "" if value is None else str(value)


def _apply_change(rec: CandidateRecord, field: str, value: Any) -> bool:
    if not hasattr(rec, field):
        return False
    coerced = _coerce_field_value(field, value)
    if coerced is None and field == "NUM_ORDEM":
        return False
    setattr(rec, field, coerced)
    return True


@router.post("/{file_id}")
def resolve_diffs(file_id: str, payload: ResolvePayload):
    work_dir = storage.file_dir(file_id, "working")
    A_path = os.path.join(work_dir, "A_records.json")
    B_path = os.path.join(work_dir, "B_records.json")
    if not (os.path.exists(A_path) and os.path.exists(B_path)):
        raise HTTPException(status_code=400, detail="Execute /process primeiro (A e B).")
    A = [CandidateRecord(**x) for x in read_json(A_path)]
    B = [CandidateRecord(**x) for x in read_json(B_path)]
    final_map = {}; [final_map.setdefault(_key(r), r) for r in B]
    for r in A:
        k = _key(r)
        if k not in final_map: final_map[k] = r
    diffs_path = os.path.join(work_dir, "diffs.json")
    diffs = read_json(diffs_path) if os.path.exists(diffs_path) else {"diffs":[]}
    suggestions = {}
    for rec in diffs.get("diffs", []):
        rk = rec.get("record_key"); sug_fields = {}
        for fd in rec.get("field_diffs", []):
            if fd.get("suggestion") is not None: sug_fields[fd["field"]] = fd["suggestion"]
        if sug_fields: suggestions[rk] = sug_fields
    save_json(os.path.join(work_dir, "decisions.json"), payload.model_dump()); audit = {}
    for d in payload.decisions:
        rk = d.record_key; rec = final_map.get(rk) or final_map.get(re.sub(r"^\|","", rk))
        if rec is None: continue
        for field, value in d.decision.items():
            if _apply_change(rec, field, value):
                audit.setdefault(rk, {})[field] = "manual"
    for rk, fdict in suggestions.items():
        rec = final_map.get(rk) or final_map.get(re.sub(r"^\|","", rk))
        if rec is None: continue
        manual_fields = set(audit.get(rk, {}).keys())
        for field, sugval in fdict.items():
            if field in manual_fields: continue
            if _apply_change(rec, field, sugval):
                audit.setdefault(rk, {})[field] = "auto_suggestion"
    final_records = [r.model_dump() for r in final_map.values()]
    save_json(os.path.join(work_dir, "final_records.json"), final_records)
    save_json(os.path.join(work_dir, "final_audit.json"), audit)
    return {"file_id": file_id, "final_count": len(final_records)}
