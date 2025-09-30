
import re, unicodedata, os, pandas as pd
from typing import Dict, List
from . import edital_parser
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    return re.sub(r"\s+", " ", s).strip()
def detect_from_docx(path: str) -> Dict[str, str]:
    try:
        _, meta = edital_parser.parse_docx(path)
        return meta or {}
    except Exception:
        return {}
def detect_from_pdf(path: str) -> Dict[str, str]:
    try:
        _, meta = edital_parser.parse_pdf(path)
        return meta or {}
    except Exception:
        return {}
def detect_from_xlsx(path: str) -> Dict[str, str]:
    try:
        df = pd.read_excel(path, header=None, dtype=str)
    except Exception:
        return {}
    vals: List[str] = []
    for row in df.fillna("").values.tolist():
        for cell in row:
            s = _norm(str(cell))
            if s: vals.append(s)
    text = " \\n ".join(vals).upper()
    orgao = None
    if "CÂMARA MUNICIPAL" in text or "CAMARA MUNICIPAL" in text: orgao = "CM"
    if "ASSEMBLEIA MUNICIPAL" in text: orgao = "AM"
    if "ASSEMBLEIA DE UNI" in text or "ASSEMBLEIA DE FREGUESIA" in text: orgao = "AF"
    municipio = None; freguesia = None
    for v in vals:
        m = re.search(r"C[ÂA]MARA MUNICIPAL DE\\s+(.+)", v, re.IGNORECASE)
        if m: municipio = _norm(m.group(1)); break
        m = re.search(r"MUNIC[IÍ]PIO DE\\s+(.+)", v, re.IGNORECASE)
        if m: municipio = _norm(m.group(1)); break
    for v in vals:
        m = re.search(r"ASSEMBLEIA DE UNI[ÃA]O DE FREGUESIAS DE\\s+(.+)", v, re.IGNORECASE)
        if m: freguesia = _norm(m.group(1)); break
        m = re.search(r"ASSEMBLEIA DE FREGUESIA(?:S)? DE\\s+(.+)", v, re.IGNORECASE)
        if m: freguesia = _norm(m.group(1)); break
    return {"ORGAO": orgao, "MUNICIPIO": municipio, "FREGUESIA": freguesia}
def detect_context(path: str) -> Dict[str, str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".docx": return detect_from_docx(path)
    if ext == ".pdf": return detect_from_pdf(path)
    if ext in [".xlsx",".xls"]: return detect_from_xlsx(path)
    return {}
