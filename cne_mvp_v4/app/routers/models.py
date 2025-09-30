
import os, json
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Dict, Any
from ..services import model_b_spacy
router = APIRouter(prefix="/models", tags=["models"])
class LabeledExample(BaseModel):
    text: str
    spans: List[Dict[str, Any]] = []
class TrainPayload(BaseModel):
    run_id: str
    train: List[LabeledExample]
    dev: List[LabeledExample] = []
@router.post("/train")
def train(payload: TrainPayload):
    return model_b_spacy.train(payload.run_id, [x.model_dump() for x in payload.train], [x.model_dump() for x in payload.dev])
class EvalPayload(BaseModel):
    run_id: str
    dev: List[LabeledExample]
@router.post("/evaluate")
def evaluate(payload: EvalPayload):
    return model_b_spacy.evaluate(payload.run_id, [x.model_dump() for x in payload.dev])
class PromotePayload(BaseModel):
    run_id: str
@router.post("/promote")
def promote(payload: PromotePayload):
    return model_b_spacy.promote(payload.run_id)
@router.get("/current")
def current():
    return model_b_spacy.current()
class InferPayload(BaseModel):
    text: str
    run_id: str | None = None
@router.post("/infer")
def infer(payload: InferPayload):
    return model_b_spacy.infer(payload.text, payload.run_id)
