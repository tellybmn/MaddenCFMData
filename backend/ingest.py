"""
Ingest router — receives exports from the Madden Companion App.

The Companion App POSTs JSON to the URL you configure.
Enter this URL in the Companion App:
    https://[your-railway-domain]/api/export

Optional: set EXPORT_TOKEN in .env to validate exports.
The app can send it as ?token=xxx or in an Authorization header.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from database import get_db
from models import ExportLog, Season, Week
from etl.loader import run_loader
from etl.aggregator import run_aggregator

log = logging.getLogger(__name__)
router = APIRouter()

RAW_DIR = Path(os.getenv("RAW_EXPORT_DIR", "./data/raw"))
EXPORT_TOKEN = os.getenv("EXPORT_TOKEN", "")


# ---------------------------------------------------------------------------
# Helper: auth check
# ---------------------------------------------------------------------------

def _check_token(request: Request):
    if not EXPORT_TOKEN:
        return   # no token configured → open
    token = (
        request.query_params.get("token")
        or request.headers.get("X-Export-Token")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    if token != EXPORT_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid export token")


# ---------------------------------------------------------------------------
# Background processing
# ---------------------------------------------------------------------------

def _process_export(raw: dict, file_path: Path, file_hash: str, db_url: str):
    """
    Run in a background thread so the Companion App gets an immediate 200.
    Creates its own DB session (background tasks run outside the request scope).
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(db_url, connect_args={"check_same_thread": False}
                           if db_url.startswith("sqlite") else {})
    Sess = sessionmaker(bind=engine)
    db: Session = Sess()

    try:
        # Run loader
        summary = run_loader(db, raw)

        # Find the season/week we just loaded to pass to aggregator
        from models import Season as S, Week as W
        season = db.query(S).filter_by(season_index=summary["season_index"]).first()
        week = db.query(W).filter_by(
            season_id=season.id, week_index=summary["week_index"]
        ).first() if season else None

        if season and week:
            run_aggregator(db, season, week)

        total = sum(v for v in summary.values() if isinstance(v, int))
        _log_export(db, file_hash, str(file_path), "success",
                    season_index=summary.get("season_index"),
                    week_index=summary.get("week_index"),
                    records=total)

    except Exception as exc:
        log.exception("Export processing failed")
        db.rollback()
        _log_export(db, file_hash, str(file_path), "error", error=str(exc))
    finally:
        db.close()


def _log_export(db: Session, file_hash: str, path: str, status: str,
                season_index: int = None, week_index: int = None,
                records: int = 0, error: str = None):
    entry = ExportLog(
        file_hash=file_hash,
        raw_file_path=path,
        status=status,
        season_index=season_index,
        week_index=week_index,
        records_processed=records,
        error_message=error,
    )
    db.add(entry)
    db.commit()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/api/export")
async def receive_export(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Primary endpoint — the Companion App POSTs here.
    Saves raw JSON, checks for duplicates, queues processing, returns 200 immediately.
    """
    _check_token(request)

    # Read body
    try:
        body = await request.body()
        raw: dict = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Hash for deduplication
    file_hash = hashlib.sha256(body).hexdigest()[:16]

    # Check duplicate
    existing = db.query(ExportLog).filter_by(file_hash=file_hash, status="success").first()
    if existing:
        log.info("Duplicate export received (hash %s), skipping", file_hash)
        return {"status": "duplicate", "hash": file_hash}

    # Archive raw file
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    file_path = RAW_DIR / f"export_{ts}_{file_hash}.json"
    file_path.write_text(json.dumps(raw, indent=2))

    # Queue background processing (returns 200 before processing finishes)
    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/madden_cfm.db")
    background_tasks.add_task(_process_export, raw, file_path, file_hash, db_url)

    log.info("Export received, queued for processing (hash %s)", file_hash)
    return {"status": "queued", "hash": file_hash}


@router.post("/api/export/upload")
async def upload_export_file(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Manual upload endpoint — accepts a JSON file via multipart form upload.
    Useful when the Companion App isn't available or for testing.

    POST with form field `file` containing the JSON export.
    """
    _check_token(request)

    form = await request.form()
    upload = form.get("file")
    if upload is None:
        raise HTTPException(status_code=400, detail="No file field in form")

    body = await upload.read()
    try:
        raw: dict = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Uploaded file is not valid JSON")

    file_hash = hashlib.sha256(body).hexdigest()[:16]
    existing = db.query(ExportLog).filter_by(file_hash=file_hash, status="success").first()
    if existing:
        return {"status": "duplicate", "hash": file_hash}

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    file_path = RAW_DIR / f"upload_{ts}_{file_hash}.json"
    file_path.write_text(json.dumps(raw, indent=2))

    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/madden_cfm.db")
    background_tasks.add_task(_process_export, raw, file_path, file_hash, db_url)

    return {"status": "queued", "hash": file_hash}


@router.get("/api/export/log")
def export_log(limit: int = 20, db: Session = Depends(get_db)):
    """Return the 20 most recent export log entries."""
    rows = (db.query(ExportLog)
            .order_by(ExportLog.received_at.desc())
            .limit(limit)
            .all())
    return [
        {
            "id": r.id,
            "received_at": r.received_at.isoformat() if r.received_at else None,
            "season_index": r.season_index,
            "week_index": r.week_index,
            "status": r.status,
            "records": r.records_processed,
            "error": r.error_message,
            "hash": r.file_hash,
        }
        for r in rows
    ]


@router.post("/api/export/reprocess/{file_hash}")
def reprocess_export(
    file_hash: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Re-run ETL on a previously archived raw file. Useful after schema changes."""
    log_entry = db.query(ExportLog).filter_by(file_hash=file_hash).first()
    if log_entry is None:
        raise HTTPException(status_code=404, detail="No export with that hash")

    path = Path(log_entry.raw_file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Raw file not found on disk")

    raw = json.loads(path.read_text())
    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/madden_cfm.db")
    background_tasks.add_task(_process_export, raw, path, file_hash + "_reprocess", db_url)
    return {"status": "requeued", "hash": file_hash}
