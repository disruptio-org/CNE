
import os, json, uuid
from typing import Any, Dict, List
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_INPUT = os.path.join(BASE, "data", "input")
DATA_WORK = os.path.join(BASE, "data", "working")
DATA_OUT = os.path.join(BASE, "data", "outputs")
META_DIR = os.path.join(BASE, "metadata")
def ensure_dirs():
    for d in [DATA_INPUT, DATA_WORK, DATA_OUT, META_DIR]:
        os.makedirs(d, exist_ok=True)
def new_file_id() -> str:
    return str(uuid.uuid4())[:8]
def file_dir(file_id: str, kind: str) -> str:
    base = {"input": DATA_INPUT, "working": DATA_WORK, "outputs": DATA_OUT}[kind]
    d = os.path.join(base, file_id); os.makedirs(d, exist_ok=True); return d
def save_json(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
def list_files_status() -> List[Dict[str, Any]]:
    ensure_dirs(); files = []
    if not os.path.isdir(DATA_INPUT): return files
    for fid in os.listdir(DATA_INPUT):
        working = os.path.join(DATA_WORK, fid)
        outputs = os.path.join(DATA_OUT, fid)
        files.append({
            "file_id": fid,
            "has_working": os.path.isdir(working),
            "has_outputs": os.path.isdir(outputs),
            "input_files": os.listdir(os.path.join(DATA_INPUT, fid))
        })
    return files
def save_metadata(name: str, obj: Any):
    ensure_dirs(); path = os.path.join(META_DIR, name); save_json(path, obj); return path
def load_metadata(name: str):
    path = os.path.join(META_DIR, name)
    if os.path.exists(path): return read_json(path)
    return None
