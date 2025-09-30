
import re, unicodedata
from rapidfuzz import fuzz
def normalize_string(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s
def similarity(a: str, b: str) -> float:
    a_n = normalize_string(a); b_n = normalize_string(b)
    if not a_n and not b_n: return 1.0
    return fuzz.ratio(a_n, b_n) / 100.0
