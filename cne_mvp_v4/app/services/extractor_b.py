
import os, pandas as pd
from typing import List, Dict, Any
from docx import Document
import pdfplumber
from . import edital_parser
CANON = {
    "dtmnfr":"DTMNFR","orgao":"ORGAO","tipo":"TIPO","sigla":"SIGLA","simbolo":"SIMBOLO",
    "nome_lista":"NOME_LISTA","num_ordem":"NUM_ORDEM","numero_ordem":"NUM_ORDEM","n_ordem":"NUM_ORDEM",
    "nome_candidato":"NOME_CANDIDATO","partido_proponente":"PARTIDO_PROPONENTE","independente":"INDEPENDENTE",
    "nome":"NOME_CANDIDATO","ord":"NUM_ORDEM",
}
def normalize_headers(cols):
    out = []
    for c in cols:
        key = str(c).strip().lower().replace(" ", "_")
        out.append(CANON.get(key, None) or str(c).strip())
    return out
def extract_from_xlsx(path: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(path, dtype=str); df.columns = normalize_headers(df.columns)
    return df.to_dict(orient="records")
def extract_from_docx(path: str) -> List[Dict[str, Any]]:
    try:
        recs, _ = edital_parser.parse_docx(path)
        if recs: return recs
    except Exception: pass
    rows = []; doc = Document(path)
    for tbl in doc.tables:
        headers = [cell.text.strip() for cell in tbl.rows[0].cells]
        headers = normalize_headers(headers)
        for row in tbl.rows[1:]:
            r = {headers[i]: row.cells[i].text.strip() for i in range(min(len(headers), len(row.cells)))}
            rows.append(r)
    return rows
def extract_from_pdf(path: str) -> List[Dict[str, Any]]:
    rows = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables() or []
            for tbl in tables:
                if len(tbl) > 1:
                    headers = normalize_headers([str(x).strip() for x in tbl[0]])
                    for row in tbl[1:]:
                        r = {headers[i]: (str(row[i]).strip() if i < len(row) else "") for i in range(len(headers))}
                        rows.append(r)
    return rows
def extract(path: str) -> List[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in [".xlsx", ".xls"]: return extract_from_xlsx(path)
        if ext == ".csv":
            df = pd.read_csv(path, dtype=str, sep=None, engine="python"); return df.to_dict(orient="records")
        if ext == ".docx": return extract_from_docx(path)
        if ext == ".pdf": return extract_from_pdf(path)
    except Exception as e:
        print(f"[Extractor B] Falha a processar {path}: {e}")
    return []
