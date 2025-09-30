
import os, json, random
from typing import List, Dict, Any
from spacy.tokens import DocBin
import spacy
LABELS = ["SIGLA","NOME_LISTA","NUM_ORDEM","NOME_CANDIDATO","PARTIDO_PROPONENTE","SIMBOLO"]
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODELS_DIR = os.path.join(BASE_DIR, "models", "model_B_spacy")
INDEX_PATH = os.path.join(MODELS_DIR, "index.json")
def ensure_dirs():
    os.makedirs(MODELS_DIR, exist_ok=True)
    if not os.path.exists(INDEX_PATH):
        with open(INDEX_PATH,"w",encoding="utf-8") as f:
            json.dump({"current": None, "runs": {}}, f, indent=2)
def _load_index():
    ensure_dirs()
    with open(INDEX_PATH,"r",encoding="utf-8") as f: return json.load(f)
def _save_index(idx): 
    with open(INDEX_PATH,"w",encoding="utf-8") as f: json.dump(idx,f,indent=2)
def make_spacy_docs(nlp, examples: List[Dict[str, Any]]):
    db = DocBin()
    for ex in examples:
        doc = nlp.make_doc(ex["text"])
        ents = []
        for sp in ex.get("spans", []):
            start, end, label = sp["start"], sp["end"], sp["label"]
            ents.append(doc.char_span(start, end, label=label))
        doc.ents = [e for e in ents if e is not None]
        db.add(doc)
    return db
def train(run_id: str, train_examples: List[Dict[str,Any]], dev_examples: List[Dict[str,Any]], base_model: str = "pt_core_news_sm"):
    ensure_dirs()
    try:
        nlp = spacy.blank("pt")  # totalmente offline, sem puxar modelo
    except Exception as e:
        raise RuntimeError(f"spaCy indisponível: {e}")
    ner = nlp.add_pipe("ner")
    for lb in LABELS: ner.add_label(lb)
    train_db = make_spacy_docs(nlp, train_examples)
    dev_db = make_spacy_docs(nlp, dev_examples or train_examples[:max(1, int(0.1*len(train_examples)))])
    train_path = os.path.join(MODELS_DIR, run_id, "train.spacy")
    dev_path = os.path.join(MODELS_DIR, run_id, "dev.spacy")
    os.makedirs(os.path.dirname(train_path), exist_ok=True)
    train_db.to_disk(train_path); dev_db.to_disk(dev_path)
    # treino simples
    optimizer = nlp.begin_training()
    TRAIN_DOCS = list(train_db.get_docs(nlp.vocab))
    DEV_DOCS = list(dev_db.get_docs(nlp.vocab))
    for epoch in range(20):
        random.shuffle(TRAIN_DOCS)
        losses = {}
        for doc in TRAIN_DOCS:
            nlp.update([doc], losses=losses)
        # avaliação simples
        _ = sum(len(d.ents) for d in DEV_DOCS)
    out_dir = os.path.join(MODELS_DIR, run_id, "model")
    nlp.to_disk(out_dir)
    idx = _load_index(); idx["runs"][run_id] = {"path": out_dir}; _save_index(idx)
    return {"run_id": run_id, "path": out_dir}
def evaluate(run_id: str, dev_examples: List[Dict[str,Any]]):
    ensure_dirs(); idx = _load_index()
    if run_id not in idx["runs"]: return {"error":"run_id desconhecido"}
    nlp = spacy.load(idx["runs"][run_id]["path"])
    total = 0; hit = 0
    for ex in dev_examples:
        doc = nlp(ex["text"]); gold = {(sp["start"], sp["end"], sp["label"]) for sp in ex.get("spans",[])}
        pred = set((ent.start_char, ent.end_char, ent.label_) for ent in doc.ents)
        total += len(gold); hit += len(gold & pred)
    prec = hit / (sum(len(sp["spans"]) for sp in dev_examples) or 1)
    rec = hit / (total or 1)
    f1 = 2*prec*rec/(prec+rec) if (prec+rec)>0 else 0.0
    return {"precision": round(prec,3), "recall": round(rec,3), "f1": round(f1,3)}
def promote(run_id: str):
    idx = _load_index()
    if run_id not in idx["runs"]: return {"error":"run_id desconhecido"}
    idx["current"] = run_id; _save_index(idx); return {"current": run_id}
def current():
    idx = _load_index(); return {"current": idx.get("current"), "runs": list(idx.get("runs",{}).keys())}
def infer(text: str, model_run: str = None):
    idx = _load_index()
    run = model_run or idx.get("current")
    if not run or run not in idx["runs"]:
        return {"error":"modelo não promovido/indisponível"}
    nlp = spacy.load(idx["runs"][run]["path"])
    doc = nlp(text)
    return [{"text": ent.text, "label": ent.label_, "start": ent.start_char, "end": ent.end_char} for ent in doc.ents]
