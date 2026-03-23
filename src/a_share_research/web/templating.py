from __future__ import annotations

from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = Jinja2Templates(directory=str(PACKAGE_ROOT / "templates"))
STATIC_ROOT = PACKAGE_ROOT / "static"


def is_htmx_request(request: Request) -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"

