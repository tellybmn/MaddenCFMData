from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db
from models import Standing, Team, Season, Week, AdvancedTeamMetric, SeasonTeamStat

router = APIRouter()


def _latest_season(db: Session):
    return db.query(Season).order_by(Season.season_index.desc()).first()


def _latest_week(db: Session, season: Season):
    return db.query(Week).filter_by(season_id=season.id).order_by(Week.week_index.desc()).first()


@router.get("/api/standings")
def get_standings(
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    season = (db.query(Season).filter_by(season_index=season_index).first()
              if season_index is not None else _latest_season(db))
    if not season:
        return []

    week = _latest_week(db, season)
    if not week:
        return []

    rows = db.query(Standing).filter_by(season_id=season.id, week_id=week.id).all()
    teams = {t.id: t for t in db.query(Team).all()}
    metrics = {
        m.team_id: m
        for m in db.query(AdvancedTeamMetric).filter_by(
            season_id=season.id, week_id=None, metric_scope="season"
        ).all()
    }

    result = []
    for r in rows:
        t = teams.get(r.team_id)
        m = metrics.get(r.team_id)
        total = (r.wins + r.losses + r.ties) or 1
        pct = round(r.wins / total, 3)
        result.append({
            "team_id":        r.team_id,
            "team_name":      f"{t.city} {t.name}" if t else "Unknown",
            "abbreviation":   t.abbreviation if t else "???",
            "division":       t.division if t else "",
            "conference":     t.conference if t else "",
            "wins":           r.wins,
            "losses":         r.losses,
            "ties":           r.ties,
            "win_pct":        pct,
            "points_for":     r.points_for,
            "points_against": r.points_against,
            "point_diff":     r.points_for - r.points_against,
            "div_record":     f"{r.division_wins}-{r.division_losses}",
            "conf_record":    f"{r.conf_wins}-{r.conf_losses}",
            "home_record":    f"{r.home_wins}-{r.home_losses}",
            "away_record":    f"{r.away_wins}-{r.away_losses}",
            "streak":         r.streak,
            # advanced
            "pythagorean_win_pct": m.pythagorean_win_pct if m else None,
            "srs":            m.srs if m else None,
            "sos":            m.strength_of_schedule if m else None,
            "ppg":            m.points_per_game if m else None,
            "papg":           m.points_allowed_per_game if m else None,
            "ypp":            m.yards_per_play if m else None,
            "third_pct":      m.third_down_conv_rate if m else None,
            "turnover_diff":  m.turnover_differential if m else None,
        })

    # Sort: conference → division → win_pct desc → point_diff desc
    result.sort(key=lambda x: (-x["win_pct"], -x["point_diff"]))
    return result


@router.get("/api/standings/division")
def get_standings_by_division(
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    """Returns standings grouped by conference then division."""
    flat = get_standings(season_index=season_index, db=db)
    grouped: dict = {}
    for row in flat:
        conf = row["conference"] or "Unknown"
        div = row["division"] or "Unknown"
        grouped.setdefault(conf, {}).setdefault(div, []).append(row)

    # Sort each division group
    for conf in grouped:
        for div in grouped[conf]:
            grouped[conf][div].sort(key=lambda x: (-x["win_pct"], -x["point_diff"]))

    return grouped
