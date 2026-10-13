"""
app/api/routes/pipeline_templates.py
-----------------------------------
API route for listing available pipeline templates.
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.routes.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline-templates", tags=["pipeline-templates"])


class PipelineTemplate(BaseModel):
    id: str
    name: str
    description: str
    yaml: str


def _find_templates_dir() -> pathlib.Path:
    candidates = [
        pathlib.Path("/app/sample-configs/templates"),
        pathlib.Path(__file__).parent.parent.parent / "sample-configs" / "templates",
        pathlib.Path(__file__).resolve().parents[3] / "backend" / "sample-configs" / "templates",
        pathlib.Path.cwd() / "backend" / "sample-configs" / "templates",
    ]
    for p in candidates:
        if (p / "templates.json").exists():
            return p
    return candidates[0]


@router.get(
    "",
    response_model=list[PipelineTemplate],
    summary="List available pipeline templates",
    dependencies=[Depends(get_current_user)],
)
async def list_pipeline_templates() -> list[PipelineTemplate]:
    """
    Return predefined, validated pipeline templates for quick start.
    Only reads files explicitly declared in templates.json manifest.
    """
    templates_dir = _find_templates_dir()
    manifest_path = templates_dir / "templates.json"

    if not manifest_path.exists():
        logger.error("Templates manifest not found at %s", manifest_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Templates manifest not found",
        )

    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest: list[dict[str, Any]] = json.load(fh)
    except Exception as exc:
        logger.error("Failed to read templates manifest: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to parse templates manifest",
        )

    results: list[PipelineTemplate] = []
    for item in manifest:
        tid = item.get("id")
        filename = item.get("file")
        if not tid or not filename:
            continue
        # Only allow filenames without path separators (no path traversal)
        clean_filename = pathlib.Path(filename).name
        template_file = templates_dir / clean_filename
        if not template_file.exists():
            logger.error("Template file missing: %s", template_file)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Template file '{clean_filename}' missing",
            )
        try:
            with open(template_file, "r", encoding="utf-8") as tf:
                yaml_content = tf.read()
        except Exception as exc:
            logger.error("Failed to read template %s: %s", clean_filename, exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to read template '{clean_filename}'",
            )

        results.append(
            PipelineTemplate(
                id=tid,
                name=item.get("name", tid),
                description=item.get("description", ""),
                yaml=yaml_content,
            )
        )

    return results
