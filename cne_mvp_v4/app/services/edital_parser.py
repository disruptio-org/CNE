
import re, unicodedata
from typing import List, Dict, Optional
from docx import Document
import pdfplumber
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    return re.sub(r"\s+", " ", s).strip()
def _detect_orgao(lines: List[str]) -> str:
    t = " ".join(lines).upper()
    if "CÂMARA MUNICIPAL" in t or "CAMARA MUNICIPAL" in t: return "CM"
    if "ASSEMBLEIA MUNICIPAL" in t: return "AM"
    if "ASSEMBLEIA DE UNI" in t or "ASSEMBLEIA DE FREGUESIA" in t: return "AF"
    return "AF"
def _detect_municipio(lines: List[str]) -> Optional[str]:
    for ln in lines:
        m = re.search(r"MUNIC[IÍ]PIO DE\s+(.+)", ln, re.IGNORECASE)
        if m: return _norm(m.group(1))
        m = re.search(r"C[ÂA]MARA MUNICIPAL DE\s+(.+)", ln, re.IGNORECASE)
        if m: return _norm(m.group(1))
    return None
def _detect_freguesia(lines: List[str]) -> Optional[str]:
    for ln in lines:
        m = re.search(r"ASSEMBLEIA DE UNI[ÃA]O DE FREGUESIAS DE\s+(.+)", ln, re.IGNORECASE)
        if m: return _norm(m.group(1))
        m = re.search(r"ASSEMBLEIA DE FREGUESIA(?:S)? DE\s+(.+)", ln, re.IGNORECASE)
        if m: return _norm(m.group(1))
    return None
def _looks_like_list_header(ln: str) -> bool:
    s = _norm(ln)
    if not s: return False
    if s.lower() in {"efetivo","efectivo","suplente","efetivos","suplentes"}: return False
    if re.search(r"assinatura\s+e\s+autentica", s, re.IGNORECASE): return False
    return s.isupper() or "(" in s or " - " in s or s.split(" ")[0].isupper()
def _extract_sigla_nome(header: str):
    h = _norm(header)
    m = re.search(r"\((I|II|III|IV|V|VI|VII|VIII|IX|X)\)", h)
    simbolo = m.group(1) if m else ""
    h_wo = re.sub(r"\s*\((I|II|III|IV|V|VI|VII|VIII|IX|X)\)\s*", "", h)
    ARTICLES = {"O","A","OS","AS"}
    if " - " in h_wo:
        left, right = h_wo.split(" - ", 1)
        left_clean = left.strip().upper()
        right_clean = right.strip()
        left_is_sigla = bool(re.fullmatch(r"[A-ZÀ-Ü0-9/.\-]{2,20}", left_clean)) and left_clean not in ARTICLES
        if left_is_sigla:
            return left_clean, right_clean, simbolo
        right_as_sigla = re.sub(r"\s+", "", right_clean.upper())
        if re.fullmatch(r"[A-ZÀ-Ü0-9/.\-]{2,40}", right_as_sigla) and right_as_sigla not in ARTICLES:
            return right_as_sigla, left.strip(), simbolo
        return "", h_wo.strip(), simbolo
    parts = h_wo.split(" ", 1)
    first = parts[0].strip().upper() if parts else ""
    rest = parts[1].strip() if len(parts) > 1 else ""
    if first and first not in ARTICLES and re.fullmatch(r"[A-ZÀ-Ü0-9/.\-]{2,20}", first):
        return first, rest, simbolo
    return "", h_wo, simbolo
def _is_name_line(s: str) -> bool:
    s = _norm(s)
    if not s or len(s) < 3: return False
    if s.lower() in {"efetivo","efectivo","suplente","efetivos","suplentes"}: return False
    return bool(re.search(r"[A-Za-zÁÀÂÃÉÊÍÓÔÕÚÇáàâãéêíóôõúç]", s))
def _strip_independente(name: str):
    s = _norm(name); indep = False
    if re.search(r"\(independente\)", s, re.IGNORECASE):
        indep = True; s = re.sub(r"\(independente\)", "", s, flags=re.IGNORECASE).strip()
    return s, indep
def parse_lines_to_records(lines: List[str]):
    orgao = _detect_orgao(lines)
    municipio = _detect_municipio(lines)
    freguesia = _detect_freguesia(lines)
    records: List[Dict] = []
    i = 0; current_header = None
    while i < len(lines):
        ln = _norm(lines[i])
        if not ln: i += 1; continue
        if ln.lower() in {"efetivo","efectivo","efetivos"}:
            nomes_ef = []; i += 1
            while i < len(lines):
                s = _norm(lines[i])
                if s.lower() in {"suplente","suplentes","efetivo","efectivo"}: break
                if _looks_like_list_header(s) and not _is_name_line(s): break
                if _is_name_line(s): nomes_ef.append(s)
                i += 1
            nomes_su = []
            if i < len(lines) and _norm(lines[i]).lower() in {"suplente","suplentes"}:
                i += 1
                while i < len(lines):
                    s = _norm(lines[i])
                    if _looks_like_list_header(s) and not _is_name_line(s): break
                    if _is_name_line(s): nomes_su.append(s)
                    i += 1
            if current_header is None:
                back = max(0, i-30)
                for j in range(i-1, back, -1):
                    if _looks_like_list_header(lines[j]):
                        current_header = _norm(lines[j]); break
            sigla, nome_lista, simbolo = _extract_sigla_nome(current_header or "")
            seq = 1
            for nm in nomes_ef + nomes_su:
                clean, indep = _strip_independente(nm)
                rec = {
                    "DTMNFR":"", "ORGAO": orgao,
                    "TIPO": "2" if seq <= len(nomes_ef) else "3",
                    "SIGLA": sigla or "", "SIMBOLO": simbolo or "",
                    "NOME_LISTA": nome_lista, "NUM_ORDEM": seq,
                    "NOME_CANDIDATO": clean, "PARTIDO_PROPONENTE": (sigla or ""),
                    "INDEPENDENTE": indep
                }
                records.append(rec); seq += 1
            current_header = None; continue
        else:
            if _looks_like_list_header(ln) and not _is_name_line(ln):
                current_header = ln
            i += 1
    meta = {"ORGAO": orgao, "MUNICIPIO": municipio, "FREGUESIA": freguesia}
    return records, meta
def parse_docx(path: str):
    doc = Document(path)
    lines = [p.text for p in doc.paragraphs]
    return parse_lines_to_records(lines)
def parse_pdf(path: str):
    lines = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                lines.append(ln)
    return parse_lines_to_records(lines)
