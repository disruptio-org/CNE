"""Review tooling for reconciling operator outputs."""

from .service import ReviewService

try:  # pragma: no cover - optional dependency guard
    from .ui import build_review_router
except ModuleNotFoundError as exc:  # pragma: no cover - triggered when FastAPI is absent
    if exc.name != "fastapi":
        raise

    def build_review_router(*_args, **_kwargs):  # type: ignore[override]
        raise RuntimeError(
            "FastAPI is required to build the review router. Install 'fastapi' to enable the UI components."
        )

__all__ = ["ReviewService", "build_review_router"]
