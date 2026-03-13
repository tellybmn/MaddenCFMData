from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from models import (
    Team, Season, Week, Standing, SeasonTeamStat, TeamStat,
    AdvancedTeamMetric, ScheduleGame, PlayerSnapshot, Player,
)

router = APIRouter()


def _latest_season(db: Session):
    return db.query(Season).order_by(Season.season_index.desc()).first()


def _latest_week(db: Session, season: Season):
    return (db.query(Week)
            .filter_by(season_id=season.id)
            .order_by(Week.week_index.desc())
            .first())


@router.get("/api/teams")
def list_teams(db: Session = Depends(get_db)):
    teams = db.query(Team).order_by(Team.conference, Team.division, Team.name).all()
    return [
        {
            "id": t.id,
            "team_id": t.team_id,
            "city": t.city,
            "name": t.name,
            "full_name": f"{t.city} {t.name}",
            "abbreviation": t.abbreviation,
            "division": t.division,
            "conference": t.conference,
        }
        for t in teams
    ]


@router.get("/api/teams/{team_id}")
def get_team(
    team_id: int,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    """Full team profile: season stats, advanced metrics, schedule, roster."""
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        raise HTTPException(404, "Team not found")

    season = (db.query(Season).filter_by(season_index=season_index).first()
              if season_index else _latest_season(db))
    if not season:
        return {"team": {"id": team.id, "name": f"{team.city} {team.name}"}}

    week = _latest_week(db, season)
    standing = db.query(Standing).filter_by(
        team_id=team.id, season_id=season.id, week_id=week.id
    ).first() if week else None

    ts = db.query(SeasonTeamStat).filter_by(team_id=team.id, season_id=season.id).first()
    metrics = db.query(AdvancedTeamMetric).filter_by(
        team_id=team.id, season_id=season.id, week_id=None, metric_scope="season"
    ).first()

    # Weekly game log
    games = _team_game_log(db, team.id, season)

    # Current roster
    roster = _team_roster(db, team.id, week)

    # Week-by-week team stats
    weekly = _weekly_team_stats(db, team.id, season)

    return {
        "team": {
            "id": team.id,
            "team_id": team.team_id,
            "full_name": f"{team.city} {team.name}",
            "abbreviation": team.abbreviation,
            "division": team.division,
            "conference": team.conference,
        },
        "record": {
            "wins": standing.wins if standing else 0,
            "losses": standing.losses if standing else 0,
            "ties": standing.ties if standing else 0,
            "streak": standing.streak if standing else None,
            "points_for": standing.points_for if standing else 0,
            "points_against": standing.points_against if standing else 0,
        },
        "season_offense": _fmt_offense(ts),
        "season_defense": _fmt_defense(ts),
        "advanced_metrics": _fmt_metrics(metrics),
        "schedule": games,
        "roster": roster,
        "weekly_stats": weekly,
    }


def _fmt_offense(ts: SeasonTeamStat) -> dict:
    if not ts:
        return {}
    g = max(ts.games or 1, 1)
    return {
        "games":            ts.games,
        "points":           ts.points,
        "points_per_game":  round((ts.points or 0) / g, 1),
        "total_yards":      ts.total_yards,
        "pass_yards":       ts.pass_yards,
        "rush_yards":       ts.rush_yards,
        "pass_att":         ts.pass_att,
        "pass_cmp":         ts.pass_cmp,
        "pass_tds":         ts.pass_tds,
        "pass_ints":        ts.pass_ints,
        "sacks_allowed":    ts.sacks_allowed,
        "rush_att":         ts.rush_att,
        "rush_tds":         ts.rush_tds,
        "first_downs":      ts.first_downs,
        "third_att":        ts.third_att,
        "third_conv":       ts.third_conv,
        "fourth_att":       ts.fourth_att,
        "fourth_conv":      ts.fourth_conv,
        "rz_att":           ts.rz_att,
        "rz_tds":           ts.rz_tds,
        "rz_fgs":           ts.rz_fgs,
        "turnovers":        ts.turnovers,
        "penalties":        ts.penalties,
        "penalty_yards":    ts.penalty_yards,
    }


def _fmt_defense(ts: SeasonTeamStat) -> dict:
    if not ts:
        return {}
    return {
        "points_allowed":       ts.points_allowed,
        "def_sacks":            ts.def_sacks,
        "def_ints":             ts.def_ints,
        "def_forced_fumbles":   ts.def_forced_fumbles,
        "def_fumble_recoveries": ts.def_fumble_recoveries,
        "def_total_tackles":    ts.def_total_tackles,
        "def_tfl":              ts.def_tfl,
        "def_safeties":         ts.def_safeties,
    }


def _fmt_metrics(m: AdvancedTeamMetric) -> dict:
    if not m:
        return {}
    result = {}
    skip = {"id", "team_id", "season_id", "week_id", "metric_scope"}
    for col in AdvancedTeamMetric.__table__.columns.keys():
        if col not in skip:
            val = getattr(m, col)
            if val is not None:
                result[col] = val
    return result


def _team_game_log(db: Session, team_db_id: int, season: Season) -> list:
    games = (db.query(ScheduleGame, Week)
             .join(Week, ScheduleGame.week_id == Week.id)
             .filter(
                 ScheduleGame.season_id == season.id,
                 (ScheduleGame.home_team_id == team_db_id) |
                 (ScheduleGame.away_team_id == team_db_id),
             )
             .order_by(Week.week_index)
             .all())

    all_teams = {t.id: t for t in db.query(Team).all()}
    result = []
    for g, w in games:
        is_home = g.home_team_id == team_db_id
        opp_id = g.away_team_id if is_home else g.home_team_id
        opp = all_teams.get(opp_id)
        team_score = g.home_score if is_home else g.away_score
        opp_score = g.away_score if is_home else g.home_score

        outcome = None
        if g.is_completed and team_score is not None and opp_score is not None:
            if team_score > opp_score:
                outcome = "W"
            elif team_score < opp_score:
                outcome = "L"
            else:
                outcome = "T"

        result.append({
            "week": w.week_number,
            "home_away": "H" if is_home else "A",
            "opponent": opp.abbreviation if opp else "?",
            "opponent_full": f"{opp.city} {opp.name}" if opp else "Unknown",
            "team_score": team_score,
            "opp_score": opp_score,
            "outcome": outcome,
            "is_completed": g.is_completed,
        })
    return result


def _team_roster(db: Session, team_db_id: int, week: Week) -> list:
    if not week:
        return []
    snaps = (db.query(PlayerSnapshot, Player)
             .join(Player, PlayerSnapshot.player_id == Player.id)
             .filter(PlayerSnapshot.team_id == team_db_id, PlayerSnapshot.week_id == week.id)
             .order_by(PlayerSnapshot.depth_position, PlayerSnapshot.depth_order)
             .all())
    dev_labels = {0: "Normal", 1: "Impact", 2: "Star", 3: "X-Factor"}
    return [
        {
            "player_id":     p.id,
            "name":          p.full_name,
            "position":      p.position,
            "depth_position": s.depth_position,
            "depth_order":   s.depth_order,
            "overall":       s.overall_rating,
            "age":           s.age,
            "dev_trait":     dev_labels.get(s.dev_trait, "Normal"),
            "injury_status": s.injury_status,
            "jersey":        p.jersey_number,
        }
        for s, p in snaps
    ]


def _weekly_team_stats(db: Session, team_db_id: int, season: Season) -> list:
    rows = (db.query(TeamStat, Week)
            .join(Week, TeamStat.week_id == Week.id)
            .filter(TeamStat.team_id == team_db_id, TeamStat.season_id == season.id)
            .order_by(Week.week_index)
            .all())
    return [
        {
            "week": w.week_number,
            "points": r.points,
            "total_yards": r.total_yards,
            "pass_yards": r.pass_yards,
            "rush_yards": r.rush_yards,
            "pass_att": r.pass_att,
            "pass_cmp": r.pass_cmp,
            "pass_tds": r.pass_tds,
            "pass_ints": r.pass_ints,
            "sacks_allowed": r.sacks_allowed,
            "rush_att": r.rush_att,
            "rush_tds": r.rush_tds,
            "first_downs": r.first_downs,
            "third_att": r.third_att,
            "third_conv": r.third_conv,
            "rz_att": r.rz_att,
            "rz_tds": r.rz_tds,
            "turnovers": r.turnovers,
            "def_sacks": r.def_sacks,
            "def_ints": r.def_ints,
            "top_seconds": r.top_seconds,
        }
        for r, w in rows
    ]


@router.get("/api/teams/{team_id}/offense")
def team_offense(
    team_id: int,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        raise HTTPException(404, "Team not found")
    season = (db.query(Season).filter_by(season_index=season_index).first()
              if season_index else _latest_season(db))
    ts = db.query(SeasonTeamStat).filter_by(team_id=team.id, season_id=season.id).first()
    m = db.query(AdvancedTeamMetric).filter_by(
        team_id=team.id, season_id=season.id, week_id=None, metric_scope="season"
    ).first()
    return {
        "raw": _fmt_offense(ts),
        "advanced": {k: v for k, v in _fmt_metrics(m).items()
                     if k in (
                         "points_per_game", "yards_per_play", "pass_yards_per_game",
                         "rush_yards_per_game", "third_down_conv_rate", "fourth_down_conv_rate",
                         "rz_td_rate", "rz_scoring_rate", "any_a", "ny_a", "completion_pct",
                         "passer_rating", "rush_ypc", "explosive_play_rate", "turnover_rate",
                         "scoring_drive_rate", "pass_ratio", "pythagorean_win_pct",
                     )},
    }


@router.get("/api/teams/{team_id}/defense")
def team_defense(
    team_id: int,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        raise HTTPException(404, "Team not found")
    season = (db.query(Season).filter_by(season_index=season_index).first()
              if season_index else _latest_season(db))
    ts = db.query(SeasonTeamStat).filter_by(team_id=team.id, season_id=season.id).first()
    m = db.query(AdvancedTeamMetric).filter_by(
        team_id=team.id, season_id=season.id, week_id=None, metric_scope="season"
    ).first()
    return {
        "raw": _fmt_defense(ts),
        "advanced": {k: v for k, v in _fmt_metrics(m).items()
                     if k in (
                         "points_allowed_per_game", "yards_allowed_per_play",
                         "pass_yards_allowed_per_game", "rush_yards_allowed_per_game",
                         "third_down_stop_rate", "rz_stop_rate", "sack_rate",
                         "opp_passer_rating", "opp_ypc", "turnover_forced_rate",
                         "takeaways_per_game",
                     )},
    }
