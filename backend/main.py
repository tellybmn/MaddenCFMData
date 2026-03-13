"""
Madden CFM Data System — FastAPI application entry point.

Routes:
  /api/export          POST — Companion App receiver
  /api/export/upload   POST — manual JSON file upload
  /api/export/log      GET  — audit log
  /api/standings       GET  — league standings
  /api/players         GET  — player list
  /api/players/{id}    GET  — player profile + full history
  /api/teams           GET  — team list
  /api/teams/{id}      GET  — team detail
  /api/scouting/{id}   GET  — opponent scouting report

  /                    HTML — standings dashboard
  /players             HTML — player browser
  /players/{id}        HTML — player detail page
  /teams/{id}          HTML — team page
  /metrics             HTML — advanced metrics leaderboards
  /scouting/{id}       HTML — scouting report page
  /admin               HTML — export log / admin panel
"""

import logging
import os
from pathlib import Path

from fastapi import FastAPI, Depends, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from database import get_db, init_db
from ingest import router as ingest_router
from api.standings import router as standings_router
from api.players import router as players_router
from api.teams import router as teams_router
from api.scouting import router as scouting_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App init
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Madden CFM Data System",
    description="Full-stack database and dashboard for Madden NFL 26 franchise leagues",
    version="1.0.0",
)

# Static files
STATIC_DIR = Path(__file__).parent.parent / "static"
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# API routers
app.include_router(ingest_router)
app.include_router(standings_router)
app.include_router(players_router)
app.include_router(teams_router)
app.include_router(scouting_router)


@app.on_event("startup")
def startup():
    # Ensure data directories exist
    Path("./data/raw").mkdir(parents=True, exist_ok=True)
    # Create all DB tables
    init_db()
    log.info("Madden CFM app started.")


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

def _base_context(request: Request, db: Session, title: str = "Madden CFM") -> dict:
    """Common context injected into every template."""
    from models import Season, Team
    seasons = db.query(Season).order_by(Season.season_index.desc()).all()
    teams = db.query(Team).order_by(Team.conference, Team.division, Team.name).all()
    current_season = seasons[0] if seasons else None
    return {
        "request": request,
        "page_title": title,
        "seasons": [{"season_index": s.season_index, "year": s.year or s.season_index} for s in seasons],
        "current_season_index": current_season.season_index if current_season else None,
        "nav_teams": [
            {
                "id": t.team_id,
                "abbr": t.abbreviation,
                "full": f"{t.city} {t.name}",
                "conference": t.conference,
                "division": t.division,
            }
            for t in teams
        ],
    }


# ---------------------------------------------------------------------------
# HTML Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    ctx = _base_context(request, db, "Standings — Madden CFM")
    ctx["selected_season"] = season_index or ctx["current_season_index"]
    return templates.TemplateResponse("standings.html", ctx)


@app.get("/players", response_class=HTMLResponse)
def players_page(
    request: Request,
    position: str = Query(None),
    team_id: int = Query(None),
    search: str = Query(None),
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    ctx = _base_context(request, db, "Players — Madden CFM")
    ctx["filter_position"] = position or ""
    ctx["filter_team_id"] = team_id or ""
    ctx["filter_search"] = search or ""
    ctx["selected_season"] = season_index or ctx["current_season_index"]
    positions = ["QB", "HB", "FB", "WR", "TE", "LT", "LG", "C", "RG", "RT",
                 "LE", "RE", "DT", "LOLB", "MLB", "ROLB", "CB", "SS", "FS",
                 "K", "P", "KR", "PR"]
    ctx["positions"] = positions
    return templates.TemplateResponse("players.html", ctx)


@app.get("/players/{player_id}", response_class=HTMLResponse)
def player_detail_page(
    player_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    from models import Player
    player = db.query(Player).get(player_id)
    if not player:
        return HTMLResponse("<h1>Player not found</h1>", status_code=404)
    ctx = _base_context(request, db, f"{player.full_name} — Madden CFM")
    ctx["player_id"] = player_id
    ctx["player_name"] = player.full_name
    ctx["player_position"] = player.position
    return templates.TemplateResponse("player_detail.html", ctx)


@app.get("/teams/{team_id}", response_class=HTMLResponse)
def team_detail_page(
    team_id: int,
    request: Request,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    from models import Team
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        return HTMLResponse("<h1>Team not found</h1>", status_code=404)
    ctx = _base_context(request, db, f"{team.city} {team.name} — Madden CFM")
    ctx["team_id"] = team_id
    ctx["team_name"] = f"{team.city} {team.name}"
    ctx["selected_season"] = season_index or ctx["current_season_index"]
    return templates.TemplateResponse("team_detail.html", ctx)


@app.get("/metrics", response_class=HTMLResponse)
def metrics_page(
    request: Request,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    ctx = _base_context(request, db, "Advanced Metrics — Madden CFM")
    ctx["selected_season"] = season_index or ctx["current_season_index"]
    return templates.TemplateResponse("metrics.html", ctx)


@app.get("/scouting/{team_id}", response_class=HTMLResponse)
def scouting_page(
    team_id: int,
    request: Request,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    from models import Team
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        return HTMLResponse("<h1>Team not found</h1>", status_code=404)
    ctx = _base_context(request, db, f"Scouting: {team.city} {team.name} — Madden CFM")
    ctx["team_id"] = team_id
    ctx["team_name"] = f"{team.city} {team.name}"
    ctx["selected_season"] = season_index or ctx["current_season_index"]
    return templates.TemplateResponse("scouting_report.html", ctx)


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request, db: Session = Depends(get_db)):
    ctx = _base_context(request, db, "Admin — Madden CFM")
    return templates.TemplateResponse("admin.html", ctx)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}
