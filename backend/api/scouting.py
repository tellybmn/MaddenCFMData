"""
Scouting Report API — opponent breakdown for gameplan purposes.

GET /api/scouting/{opponent_team_id}
Returns a full opponent breakdown:
  - Offensive tendencies (pass/run ratio, 3rd down plays, RZ behavior)
  - Defensive tendencies (blitz rate proxy, coverage style proxy)
  - Top statistical threats at each position
  - Key weaknesses (bottom-ranked metrics vs league average)
  - Advanced metrics comparison vs league averages
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from database import get_db
from models import (
    Team, Season, Week, AdvancedTeamMetric, SeasonTeamStat,
    PlayerSnapshot, Player, AdvancedPlayerMetric,
    SeasonPlayerPassingStat, SeasonPlayerRushingStat,
    SeasonPlayerReceivingStat, SeasonPlayerDefenseStat,
)

router = APIRouter()


def _latest_season(db):
    return db.query(Season).order_by(Season.season_index.desc()).first()


def _latest_week(db, season):
    return (db.query(Week)
            .filter_by(season_id=season.id)
            .order_by(Week.week_index.desc())
            .first())


@router.get("/api/scouting/{team_id}")
def scouting_report(
    team_id: int,
    season_index: int = Query(None),
    db: Session = Depends(get_db),
):
    team = db.query(Team).filter_by(team_id=team_id).first()
    if not team:
        raise HTTPException(404, "Team not found")

    season = (db.query(Season).filter_by(season_index=season_index).first()
              if season_index else _latest_season(db))
    if not season:
        raise HTTPException(404, "No season data")

    week = _latest_week(db, season)

    ts = db.query(SeasonTeamStat).filter_by(team_id=team.id, season_id=season.id).first()
    metrics = db.query(AdvancedTeamMetric).filter_by(
        team_id=team.id, season_id=season.id, week_id=None, metric_scope="season"
    ).first()

    # League averages for comparison
    league_avgs = _league_averages(db, season)

    return {
        "opponent": {
            "id": team.id,
            "team_id": team.team_id,
            "full_name": f"{team.city} {team.name}",
            "abbreviation": team.abbreviation,
            "division": team.division,
        },
        "offensive_tendencies": _offense_tendencies(ts, metrics, league_avgs),
        "defensive_tendencies": _defense_tendencies(ts, metrics, league_avgs),
        "key_threats": _key_threats(db, team.id, season, week),
        "down_and_distance": _down_and_distance(ts, metrics),
        "red_zone": _red_zone_breakdown(ts, metrics, league_avgs),
        "turnover_profile": _turnover_profile(ts, metrics, league_avgs),
        "weaknesses": _identify_weaknesses(metrics, league_avgs),
        "strengths": _identify_strengths(metrics, league_avgs),
        "metric_comparisons": _metric_comparisons(metrics, league_avgs),
    }


def _league_averages(db: Session, season: Season) -> dict:
    """Compute league-wide averages for key metrics."""
    all_metrics = db.query(AdvancedTeamMetric).filter_by(
        season_id=season.id, week_id=None, metric_scope="season"
    ).all()

    if not all_metrics:
        return {}

    def avg(attr):
        vals = [getattr(m, attr) for m in all_metrics if getattr(m, attr) is not None]
        return round(sum(vals) / len(vals), 2) if vals else 0

    return {
        "points_per_game":          avg("points_per_game"),
        "points_allowed_per_game":  avg("points_allowed_per_game"),
        "yards_per_play":           avg("yards_per_play"),
        "yards_allowed_per_play":   avg("yards_allowed_per_play"),
        "third_down_conv_rate":     avg("third_down_conv_rate"),
        "third_down_stop_rate":     avg("third_down_stop_rate"),
        "rz_td_rate":               avg("rz_td_rate"),
        "rz_stop_rate":             avg("rz_stop_rate"),
        "pass_ratio":               avg("pass_ratio"),
        "turnover_rate":            avg("turnover_rate"),
        "sack_rate":                avg("sack_rate"),
        "opp_passer_rating":        avg("opp_passer_rating"),
        "any_a":                    avg("any_a"),
        "rush_ypc":                 avg("rush_ypc"),
        "opp_ypc":                  avg("opp_ypc"),
        "completion_pct":           avg("completion_pct"),
        "explosive_play_rate":      avg("explosive_play_rate"),
        "turnover_differential":    avg("turnover_differential"),
        "pythagorean_win_pct":      avg("pythagorean_win_pct"),
        "srs":                      avg("srs"),
    }


def _offense_tendencies(ts: SeasonTeamStat, m: AdvancedTeamMetric, avgs: dict) -> dict:
    if not ts:
        return {}
    g = max(ts.games or 1, 1)
    pass_ratio = m.pass_ratio if m else None
    tendency = "Balanced"
    if pass_ratio:
        if pass_ratio > 60:
            tendency = "Pass-heavy"
        elif pass_ratio < 40:
            tendency = "Run-heavy"

    return {
        "playcall_tendency": tendency,
        "pass_ratio": pass_ratio,
        "yards_per_play": m.yards_per_play if m else None,
        "yards_per_play_vs_avg": _vs_avg(m, "yards_per_play", avgs, higher_is_better=True),
        "any_a": m.any_a if m else None,
        "any_a_vs_avg": _vs_avg(m, "any_a", avgs, higher_is_better=True),
        "rush_ypc": m.rush_ypc if m else None,
        "rush_ypc_vs_avg": _vs_avg(m, "rush_ypc", avgs, higher_is_better=True),
        "explosive_play_rate": m.explosive_play_rate if m else None,
        "scoring_drive_rate": m.scoring_drive_rate if m else None,
        "completion_pct": m.completion_pct if m else None,
        "passer_rating": m.passer_rating if m else None,
        "points_per_game": m.points_per_game if m else None,
        "points_per_game_vs_avg": _vs_avg(m, "points_per_game", avgs, higher_is_better=True),
    }


def _defense_tendencies(ts: SeasonTeamStat, m: AdvancedTeamMetric, avgs: dict) -> dict:
    if not m:
        return {}
    sack_rate = m.sack_rate
    blitz_tendency = "Unknown"
    avg_sack = avgs.get("sack_rate", 5)
    if sack_rate is not None:
        if sack_rate > avg_sack * 1.3:
            blitz_tendency = "Aggressive pass rush"
        elif sack_rate < avg_sack * 0.7:
            blitz_tendency = "Conservative / coverage-first"
        else:
            blitz_tendency = "Balanced"

    return {
        "pass_rush_tendency": blitz_tendency,
        "points_allowed_per_game": m.points_allowed_per_game,
        "points_allowed_vs_avg": _vs_avg(m, "points_allowed_per_game", avgs, higher_is_better=False),
        "yards_allowed_per_play": m.yards_allowed_per_play,
        "yards_allowed_vs_avg": _vs_avg(m, "yards_allowed_per_play", avgs, higher_is_better=False),
        "sack_rate": sack_rate,
        "sack_rate_vs_avg": _vs_avg(m, "sack_rate", avgs, higher_is_better=True),
        "opp_passer_rating": m.opp_passer_rating,
        "opp_passer_rating_vs_avg": _vs_avg(m, "opp_passer_rating", avgs, higher_is_better=False),
        "opp_ypc": m.opp_ypc,
        "opp_ypc_vs_avg": _vs_avg(m, "opp_ypc", avgs, higher_is_better=False),
        "third_down_stop_rate": m.third_down_stop_rate,
        "third_down_stop_vs_avg": _vs_avg(m, "third_down_stop_rate", avgs, higher_is_better=True),
        "rz_stop_rate": m.rz_stop_rate,
        "takeaways_per_game": m.takeaways_per_game,
        "turnover_forced_rate": m.turnover_forced_rate,
    }


def _key_threats(db: Session, team_db_id: int, season: Season, week: Week) -> dict:
    """Return top statistical threats by position group."""
    # Get current roster
    snaps = (db.query(PlayerSnapshot, Player)
             .join(Player, PlayerSnapshot.player_id == Player.id)
             .filter(PlayerSnapshot.team_id == team_db_id, PlayerSnapshot.week_id == week.id)
             .all()) if week else []

    roster_ids = {p.id: p for _, p in snaps}
    snap_map = {s.player_id: s for s, _ in snaps}

    def top_by_metric(positions, metric_col, limit=3):
        q = (db.query(AdvancedPlayerMetric, Player)
             .join(Player, AdvancedPlayerMetric.player_id == Player.id)
             .filter(
                 AdvancedPlayerMetric.season_id == season.id,
                 AdvancedPlayerMetric.metric_scope == "season",
                 Player.id.in_(list(roster_ids.keys())),
                 Player.position.in_(positions),
                 metric_col.isnot(None),
             )
             .order_by(metric_col.desc())
             .limit(limit)
             .all())
        return [
            {
                "player_id": p.id,
                "name": p.full_name,
                "position": p.position,
                "overall": snap_map[p.id].overall_rating if p.id in snap_map else None,
                metric_col.key: getattr(m, metric_col.key),
            }
            for m, p in q
        ]

    return {
        "qbs": top_by_metric(["QB"], AdvancedPlayerMetric.any_a),
        "rbs": top_by_metric(["HB", "FB", "RB"], AdvancedPlayerMetric.ypc),
        "receivers": top_by_metric(["WR", "TE"], AdvancedPlayerMetric.y_tgt),
        "pass_rushers": top_by_metric(["RE", "LE", "DT", "LOLB", "ROLB"], AdvancedPlayerMetric.sacks_per_game),
        "coverage": top_by_metric(["CB", "SS", "FS"], AdvancedPlayerMetric.passer_rating_allowed),
        "top_overall": sorted(
            [
                {
                    "player_id": p.id,
                    "name": p.full_name,
                    "position": p.position,
                    "overall": snap_map[p.id].overall_rating if p.id in snap_map else 0,
                }
                for p in roster_ids.values()
            ],
            key=lambda x: -(x["overall"] or 0)
        )[:5],
    }


def _down_and_distance(ts: SeasonTeamStat, m: AdvancedTeamMetric) -> dict:
    """3rd down breakdown."""
    if not ts:
        return {}
    third_rate = m.third_down_conv_rate if m else None
    return {
        "third_down_att": ts.third_att,
        "third_down_conv": ts.third_conv,
        "third_down_conv_rate": third_rate,
        "fourth_down_att": ts.fourth_att,
        "fourth_down_conv": ts.fourth_conv,
        "fourth_down_conv_rate": m.fourth_down_conv_rate if m else None,
        "note": "Down-and-distance splits by distance (short/medium/long) require play-by-play data not available in Madden exports.",
    }


def _red_zone_breakdown(ts: SeasonTeamStat, m: AdvancedTeamMetric, avgs: dict) -> dict:
    if not ts:
        return {}
    rz_att = ts.rz_att or 0
    rz_tds = ts.rz_tds or 0
    rz_fgs = ts.rz_fgs or 0
    scoring = rz_tds + rz_fgs
    rz_td_rate = round((rz_tds / rz_att * 100), 1) if rz_att else 0
    rz_score_rate = round((scoring / rz_att * 100), 1) if rz_att else 0
    avg_td = avgs.get("rz_td_rate", 50)
    return {
        "rz_trips": rz_att,
        "rz_tds": rz_tds,
        "rz_fgs": rz_fgs,
        "rz_td_rate": rz_td_rate,
        "rz_score_rate": rz_score_rate,
        "rz_td_rate_vs_avg": round(rz_td_rate - avg_td, 1),
        "grade": "Elite" if rz_td_rate > avg_td + 10
                 else "Above Average" if rz_td_rate > avg_td
                 else "Below Average" if rz_td_rate < avg_td - 10
                 else "Average",
    }


def _turnover_profile(ts: SeasonTeamStat, m: AdvancedTeamMetric, avgs: dict) -> dict:
    if not ts:
        return {}
    giveaways = ts.turnovers or 0
    takeaways = (ts.def_ints or 0) + (ts.def_fumble_recoveries or 0)
    diff = takeaways - giveaways
    return {
        "giveaways": giveaways,
        "takeaways": takeaways,
        "turnover_differential": diff,
        "diff_grade": "+" if diff > 0 else ("=" if diff == 0 else "-"),
        "ints_thrown": ts.pass_ints,
        "fumbles_lost": ts.fumbles_lost,
        "def_ints": ts.def_ints,
        "def_fumble_recoveries": ts.def_fumble_recoveries,
    }


def _identify_weaknesses(m: AdvancedTeamMetric, avgs: dict) -> list[dict]:
    """Return metrics where this team is significantly below league average."""
    if not m:
        return []
    checks = [
        ("Offense: Yards per Play", "yards_per_play", True, 0.85),
        ("Offense: 3rd Down Rate", "third_down_conv_rate", True, 0.85),
        ("Offense: Red Zone TD Rate", "rz_td_rate", True, 0.85),
        ("Offense: ANY/A", "any_a", True, 0.85),
        ("Defense: Points Allowed", "points_allowed_per_game", False, 1.15),
        ("Defense: Yards Allowed/Play", "yards_allowed_per_play", False, 1.15),
        ("Defense: 3rd Down Stop Rate", "third_down_stop_rate", True, 0.85),
        ("Defense: Opponent Passer Rating", "opp_passer_rating", False, 1.15),
        ("Defense: Sack Rate", "sack_rate", True, 0.7),
        ("Turnovers: Differential", "turnover_differential", True, None),
    ]
    weaknesses = []
    for label, attr, higher_better, threshold in checks:
        val = getattr(m, attr, None)
        avg = avgs.get(attr)
        if val is None or avg is None or avg == 0:
            continue
        if threshold:
            bad = (val < avg * threshold) if higher_better else (val > avg * threshold)
        else:
            bad = val < 0
        if bad:
            weaknesses.append({
                "category": label,
                "value": val,
                "league_avg": avg,
                "vs_avg": round(val - avg, 2),
            })
    weaknesses.sort(key=lambda x: abs(x["vs_avg"]), reverse=True)
    return weaknesses[:5]


def _identify_strengths(m: AdvancedTeamMetric, avgs: dict) -> list[dict]:
    """Return metrics where this team significantly exceeds league average."""
    if not m:
        return []
    checks = [
        ("Offense: Yards per Play", "yards_per_play", True, 1.1),
        ("Offense: Scoring Drive Rate", "scoring_drive_rate", True, 1.1),
        ("Offense: 3rd Down Rate", "third_down_conv_rate", True, 1.1),
        ("Offense: ANY/A", "any_a", True, 1.1),
        ("Offense: Explosive Plays", "explosive_play_rate", True, 1.1),
        ("Defense: Sack Rate", "sack_rate", True, 1.3),
        ("Defense: 3rd Down Stop Rate", "third_down_stop_rate", True, 1.1),
        ("Defense: Turnover Forced Rate", "turnover_forced_rate", True, 1.2),
        ("Composite: SRS", "srs", True, None),
    ]
    strengths = []
    for label, attr, higher_better, threshold in checks:
        val = getattr(m, attr, None)
        avg = avgs.get(attr)
        if val is None or avg is None or avg == 0:
            continue
        if threshold:
            good = (val > avg * threshold) if higher_better else (val < avg * threshold)
        else:
            good = val > 2  # SRS > +2 is solid
        if good:
            strengths.append({
                "category": label,
                "value": val,
                "league_avg": avg,
                "vs_avg": round(val - avg, 2),
            })
    strengths.sort(key=lambda x: abs(x["vs_avg"]), reverse=True)
    return strengths[:5]


def _metric_comparisons(m: AdvancedTeamMetric, avgs: dict) -> list[dict]:
    """Full side-by-side comparison of all key metrics vs league average."""
    if not m:
        return []
    metrics = [
        ("Points/Game", "points_per_game", True),
        ("Points Allowed/Game", "points_allowed_per_game", False),
        ("Yards/Play (Off)", "yards_per_play", True),
        ("Yards Allowed/Play (Def)", "yards_allowed_per_play", False),
        ("3rd Down Conv %", "third_down_conv_rate", True),
        ("3rd Down Stop %", "third_down_stop_rate", True),
        ("RZ TD Rate", "rz_td_rate", True),
        ("RZ Stop Rate", "rz_stop_rate", True),
        ("ANY/A", "any_a", True),
        ("Rush YPC (Off)", "rush_ypc", True),
        ("Opp Rush YPC (Def)", "opp_ypc", False),
        ("Completion %", "completion_pct", True),
        ("Passer Rating (Off)", "passer_rating", True),
        ("Opp Passer Rating (Def)", "opp_passer_rating", False),
        ("Sack Rate (Def)", "sack_rate", True),
        ("Turnover Rate (Off)", "turnover_rate", False),
        ("Turnover Forced Rate", "turnover_forced_rate", True),
        ("Explosive Play Rate", "explosive_play_rate", True),
        ("Pythagorean Win%", "pythagorean_win_pct", True),
        ("SRS", "srs", True),
    ]
    result = []
    for label, attr, higher_better in metrics:
        val = getattr(m, attr, None)
        avg = avgs.get(attr)
        if val is None:
            continue
        better = None
        if avg is not None:
            diff = val - avg
            better = (diff > 0) if higher_better else (diff < 0)
        result.append({
            "metric": label,
            "value": val,
            "league_avg": avg,
            "vs_avg": round(val - avg, 2) if avg is not None else None,
            "is_better": better,
        })
    return result


def _vs_avg(m: AdvancedTeamMetric, attr: str, avgs: dict, higher_is_better: bool) -> dict | None:
    if not m:
        return None
    val = getattr(m, attr, None)
    avg = avgs.get(attr)
    if val is None or avg is None:
        return None
    diff = val - avg
    return {
        "diff": round(diff, 2),
        "better": (diff > 0) if higher_is_better else (diff < 0),
    }
