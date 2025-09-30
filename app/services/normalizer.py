
from typing import List, Dict, Any
import pandas as pd
from ..models.schemas import CandidateRecord, CSV_FIELDS
def to_bool(v) -> bool:
    if isinstance(v, bool): return v
    s = str(v).strip().lower(); return s in {"1","true","t","yes","sim","y"}
def normalize_records(records: List[Dict[str, Any]]) -> List[CandidateRecord]:
    norm = []
    for r in records:
        sigla = str(r.get("SIGLA","")).strip()
        if sigla in {"O","A","OS","AS"}: sigla = ""
        prop = str(r.get("PARTIDO_PROPONENTE") or "").strip() or sigla
        item = {
            "DTMNFR": str(r.get("DTMNFR","")).strip(),
            "ORGAO": str(r.get("ORGAO","")).strip().upper() or "AF",
            "TIPO": str(r.get("TIPO","")).strip() or "2",
            "SIGLA": sigla,
            "SIMBOLO": (str(r.get("SIMBOLO") or "").strip()),
            "NOME_LISTA": (str(r.get("NOME_LISTA") or "").strip()),
            "NUM_ORDEM": int(r.get("NUM_ORDEM") or r.get("NUM") or r.get("ORD") or 0),
            "NOME_CANDIDATO": str(r.get("NOME_CANDIDATO") or r.get("NOME") or "").strip(),
            "PARTIDO_PROPONENTE": prop,
            "INDEPENDENTE": to_bool(r.get("INDEPENDENTE", False)),
        }
        norm.append(CandidateRecord(**item))
    norm.sort(key=lambda x: (x.DTMNFR, x.ORGAO, x.NUM_ORDEM))
    return norm
def dataframe_from_records(records: List[CandidateRecord]) -> pd.DataFrame:
    rows = [{k: getattr(rec, k) for k in CSV_FIELDS} for rec in records]
    df = pd.DataFrame(rows, columns=CSV_FIELDS)
    df["NUM_ORDEM"] = pd.to_numeric(df["NUM_ORDEM"], errors="coerce").fillna(0).astype(int)
    df["INDEPENDENTE"] = df["INDEPENDENTE"].astype(bool)
    for col in ["DTMNFR","SIGLA","SIMBOLO","NOME_LISTA","NOME_CANDIDATO","PARTIDO_PROPONENTE","ORGAO","TIPO"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"None":"", "nan":"", "NaN":"", "NoneType":""})
    return df
