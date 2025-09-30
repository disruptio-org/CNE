
import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from .routers import upload, process, reconcile, resolve, validate, export, report, artifacts, models
from .services import storage
app = FastAPI(title="CNE MVP - Extração e Validação (On-Prem + spaCy)")
app.include_router(upload.router)
app.include_router(process.router)
app.include_router(reconcile.router)
app.include_router(resolve.router)
app.include_router(validate.router)
app.include_router(export.router)
app.include_router(report.router)
app.include_router(artifacts.router)
app.include_router(models.router)
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(BASE, "app", "templates"))
STATIC_DIR = os.path.join(BASE, "app", "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
@app.get("/", response_class=HTMLResponse, tags=["ui"])
def index(request: Request):
    files = storage.list_files_status()
    return templates.TemplateResponse("index.html", {"request": request, "files": files})
@app.get("/ui/{file_id}", response_class=HTMLResponse, tags=["ui"])
def file_ui(request: Request, file_id: str):
    return templates.TemplateResponse("file_detail.html", {"request": request, "file_id": file_id})
