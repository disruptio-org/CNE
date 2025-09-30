
import os, csv, unicodedata, re
from typing import List, Dict, Any, Tuple
from ..models.schemas import CandidateRecord, ValidationIssue
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    return re.sub(r"\s+", " ", s).strip().lower()
def _same(a: str, b: str) -> bool: return _norm(a) == _norm(b)
def _build_code(org, dist_code, muni_code, freg_code):
    dc = (dist_code or "").zfill(2); mc = (muni_code or "").zfill(2); fc = (freg_code or "").zfill(2)
    if org == "AF": return f"{dc}{mc}{fc}" if dc and mc and fc else ""
    else: return f"{dc}{mc}" if dc and mc else ""
def _load_codigos_csv():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "metadata"))
    path = os.path.join(base_dir, "codigos_ine.csv"); rows = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f, delimiter=';')
            for r in rdr: rows.append(r)
    return rows
def _match_from_csv(org, municipio, freguesia, rows):
    for r in rows:
        muni = r.get("MUNICIPIO",""); freg = r.get("FREGUESIA","")
        if org == "AF":
            if _same(muni, municipio) and _same(freg, freguesia):
                return _build_code(org, r.get("COD_DISTRITO"), r.get("COD_MUNICIPIO"), r.get("COD_FREGUESIA"))
        else:
            if _same(muni, municipio):
                return _build_code(org, r.get("COD_DISTRITO"), r.get("COD_MUNICIPIO"), None)
    return ""
def _match_from_metadata(org, municipio, freguesia, map_items: List[Dict[str, Any]]):
    for it in map_items or []:
        if _same(it.get("ORGAO",""), org) and _same(it.get("MUNICIPIO",""), municipio) and _same(it.get("FREGUESIA","") or "", freguesia or ""):
            return str(it.get("DTMNFR") or "")
    if org in {"CM","AM"}:
        for it in map_items or []:
            if _same(it.get("ORGAO",""), org) and _same(it.get("MUNICIPIO",""), municipio) and not it.get("FREGUESIA"):
                return str(it.get("DTMNFR") or "")
    return ""
def autofill_dtmnfr(records: List[CandidateRecord], file_meta: Dict[str, Any], metadata: Dict[str, Any]) -> Tuple[List[CandidateRecord], List[ValidationIssue]]:
    issues: List[ValidationIssue] = []
    rows = _load_codigos_csv()
    map_items = metadata.get("dtmnfr_map") or []
    muni = (file_meta or {}).get("MUNICIPIO"); freg = (file_meta or {}).get("FREGUESIA")
    changed = 0
    for r in records:
        if not r.DTMNFR:
            code = _match_from_csv(r.ORGAO, muni, freg, rows)
            if not code: code = _match_from_metadata(r.ORGAO, muni, freg, map_items)
            if code: r.DTMNFR = code; changed += 1
            else:
                issues.append(ValidationIssue(level="WARN", record_key=f"{r.DTMNFR}|{r.ORGAO}|{r.NUM_ORDEM}", message="DTMNFR não encontrado (CSV/metadata)"))
    if changed:
        issues.append(ValidationIssue(level="INFO", record_key=None, message=f"DTMNFR preenchido automaticamente em {changed} registos"))
    return records, issues
