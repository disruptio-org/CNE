"""Web dashboard application for orchestrating the processing pipeline."""

from .app import PipelineOrchestrator, create_app
from .progress import fetch_document_progress

__all__ = ["create_app", "fetch_document_progress", "PipelineOrchestrator"]
