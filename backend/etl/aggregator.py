"""
Aggregator — runs after each successful import to:
  1. Rebuild season-to-date totals for all players and teams
  2. Recompute all advanced metrics (player + team level, weekly + season)
  3. Recompute standings from the schedule table
  4. Recompute SRS (requires all teams, iterative)

Called by ingest.py after loader.run_loader() completes.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import (
    Season, Week, Team, Player,
    PlayerPassingStat, PlayerRushingStat, PlayerReceivingStat,
    PlayerDefenseStat, PlayerKickingStat, PlayerPuntingStat,
    TeamStat, ScheduleGame, Standing,
    SeasonPlayerPassingStat, SeasonPlayerRushingStat,
    SeasonPlayerReceivingStat, SeasonPlayerDefenseStat,
    SeasonPlayerKickingStat, SeasonPlayerPuntingStat,
    SeasonTeamStat, AdvancedPlayerMetric, AdvancedTeamMetric,
)
from etl.metrics import (
    build_player_metrics, build_team_metrics, calc_srs,
    calc_strength_of_schedule, calc_pythagorean_win_pct,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sum_col(rows, attr: str, default=0):
    return sum(getattr(r, attr, default) or default for r in rows)


def _upsert(db: Session, model_class, lookup: dict, data: dict):
    obj = db.query(model_class).filter_by(**lookup).first()
    if obj is None:
        obj = model_class(**data)
        db.add(obj)
    else:
        for k, v in data.items():
            setattr(obj, k, v)
    return obj


# ---------------------------------------------------------------------------
# 1. Season totals — Players
# ---------------------------------------------------------------------------

def _rebuild_season_player_stats(db: Session, season: Season):
    """Accumulate all weekly rows for this season into season_player_* tables."""

    # --- Passing ---
    passing = (db.query(PlayerPassingStat)
               .filter_by(season_id=season.id).all())
    by_player: dict[int, list] = {}
    for row in passing:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        team_id = rows[-1].team_id   # most recent team
        _upsert(db, SeasonPlayerPassingStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":   player_id,
                    "team_id":     team_id,
                    "season_id":   season.id,
                    "games":       len(rows),
                    "completions": _sum_col(rows, "completions"),
                    "attempts":    _sum_col(rows, "attempts"),
                    "yards":       _sum_col(rows, "yards"),
                    "tds":         _sum_col(rows, "tds"),
                    "ints":        _sum_col(rows, "ints"),
                    "sacks":       _sum_col(rows, "sacks"),
                    "sack_yards":  _sum_col(rows, "sack_yards"),
                    "yac":         _sum_col(rows, "yac"),
                    "longest":     max(r.longest or 0 for r in rows),
                    "first_downs": _sum_col(rows, "first_downs"),
                })

    # --- Rushing ---
    rushing = db.query(PlayerRushingStat).filter_by(season_id=season.id).all()
    by_player = {}
    for row in rushing:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        _upsert(db, SeasonPlayerRushingStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":      player_id,
                    "team_id":        rows[-1].team_id,
                    "season_id":      season.id,
                    "games":          len(rows),
                    "attempts":       _sum_col(rows, "attempts"),
                    "yards":          _sum_col(rows, "yards"),
                    "tds":            _sum_col(rows, "tds"),
                    "fumbles":        _sum_col(rows, "fumbles"),
                    "fumbles_lost":   _sum_col(rows, "fumbles_lost"),
                    "yac":            _sum_col(rows, "yac"),
                    "longest":        max(r.longest or 0 for r in rows),
                    "first_downs":    _sum_col(rows, "first_downs"),
                    "broken_tackles": _sum_col(rows, "broken_tackles"),
                })

    # --- Receiving ---
    receiving = db.query(PlayerReceivingStat).filter_by(season_id=season.id).all()
    by_player = {}
    for row in receiving:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        _upsert(db, SeasonPlayerReceivingStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":   player_id,
                    "team_id":     rows[-1].team_id,
                    "season_id":   season.id,
                    "games":       len(rows),
                    "targets":     _sum_col(rows, "targets"),
                    "receptions":  _sum_col(rows, "receptions"),
                    "yards":       _sum_col(rows, "yards"),
                    "tds":         _sum_col(rows, "tds"),
                    "drops":       _sum_col(rows, "drops"),
                    "yac":         _sum_col(rows, "yac"),
                    "longest":     max(r.longest or 0 for r in rows),
                    "first_downs": _sum_col(rows, "first_downs"),
                })

    # --- Defense ---
    defense = db.query(PlayerDefenseStat).filter_by(season_id=season.id).all()
    by_player = {}
    for row in defense:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        _upsert(db, SeasonPlayerDefenseStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":          player_id,
                    "team_id":            rows[-1].team_id,
                    "season_id":          season.id,
                    "games":              len(rows),
                    "tackles_solo":       _sum_col(rows, "tackles_solo"),
                    "tackles_assist":     _sum_col(rows, "tackles_assist"),
                    "tackles_total":      _sum_col(rows, "tackles_total"),
                    "tackles_for_loss":   _sum_col(rows, "tackles_for_loss"),
                    "sacks":              _sum_col(rows, "sacks"),
                    "sack_yards":         _sum_col(rows, "sack_yards"),
                    "ints":               _sum_col(rows, "ints"),
                    "int_yards":          _sum_col(rows, "int_yards"),
                    "int_tds":            _sum_col(rows, "int_tds"),
                    "pass_breakups":      _sum_col(rows, "pass_breakups"),
                    "forced_fumbles":     _sum_col(rows, "forced_fumbles"),
                    "fumble_recoveries":  _sum_col(rows, "fumble_recoveries"),
                    "safeties":           _sum_col(rows, "safeties"),
                    "kick_blocks":        _sum_col(rows, "kick_blocks"),
                    "missed_tackles":     _sum_col(rows, "missed_tackles"),
                })

    # --- Kicking ---
    kicking = db.query(PlayerKickingStat).filter_by(season_id=season.id).all()
    by_player = {}
    for row in kicking:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        _upsert(db, SeasonPlayerKickingStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":     player_id,
                    "team_id":       rows[-1].team_id,
                    "season_id":     season.id,
                    "games":         len(rows),
                    "fg_att":        _sum_col(rows, "fg_att"),
                    "fg_made":       _sum_col(rows, "fg_made"),
                    "fg_att_19":     _sum_col(rows, "fg_att_19"),
                    "fg_made_19":    _sum_col(rows, "fg_made_19"),
                    "fg_att_29":     _sum_col(rows, "fg_att_29"),
                    "fg_made_29":    _sum_col(rows, "fg_made_29"),
                    "fg_att_39":     _sum_col(rows, "fg_att_39"),
                    "fg_made_39":    _sum_col(rows, "fg_made_39"),
                    "fg_att_49":     _sum_col(rows, "fg_att_49"),
                    "fg_made_49":    _sum_col(rows, "fg_made_49"),
                    "fg_att_50":     _sum_col(rows, "fg_att_50"),
                    "fg_made_50":    _sum_col(rows, "fg_made_50"),
                    "fg_long":       max(r.fg_long or 0 for r in rows),
                    "xp_att":        _sum_col(rows, "xp_att"),
                    "xp_made":       _sum_col(rows, "xp_made"),
                    "kickoffs":      _sum_col(rows, "kickoffs"),
                    "kickoff_yards": _sum_col(rows, "kickoff_yards"),
                    "touchbacks":    _sum_col(rows, "touchbacks"),
                })

    # --- Punting ---
    punting = db.query(PlayerPuntingStat).filter_by(season_id=season.id).all()
    by_player = {}
    for row in punting:
        by_player.setdefault(row.player_id, []).append(row)

    for player_id, rows in by_player.items():
        _upsert(db, SeasonPlayerPuntingStat,
                {"player_id": player_id, "season_id": season.id},
                {
                    "player_id":   player_id,
                    "team_id":     rows[-1].team_id,
                    "season_id":   season.id,
                    "games":       len(rows),
                    "punts":       _sum_col(rows, "punts"),
                    "gross_yards": _sum_col(rows, "gross_yards"),
                    "net_yards":   _sum_col(rows, "net_yards"),
                    "longest":     max(r.longest or 0 for r in rows),
                    "touchbacks":  _sum_col(rows, "touchbacks"),
                    "inside_20":   _sum_col(rows, "inside_20"),
                })

    db.flush()
    log.info("Season player stats rebuilt for season %d", season.season_index)


# ---------------------------------------------------------------------------
# 2. Season totals — Teams
# ---------------------------------------------------------------------------

def _rebuild_season_team_stats(db: Session, season: Season):
    """Accumulate all weekly team_stats rows for this season."""
    weekly = db.query(TeamStat).filter_by(season_id=season.id).all()
    by_team: dict[int, list] = {}
    for row in weekly:
        by_team.setdefault(row.team_id, []).append(row)

    # Also compute points_allowed from schedule
    games_map: dict[int, list[ScheduleGame]] = {}
    for g in db.query(ScheduleGame).filter_by(season_id=season.id, is_completed=True).all():
        games_map.setdefault(g.home_team_id, []).append(g)
        games_map.setdefault(g.away_team_id, []).append(g)

    for team_id, rows in by_team.items():
        # Points allowed = opponent scores from schedule
        team_games = games_map.get(team_id, [])
        pts_allowed = 0
        for g in team_games:
            if g.home_team_id == team_id:
                pts_allowed += g.away_score or 0
            else:
                pts_allowed += g.home_score or 0

        _upsert(db, SeasonTeamStat,
                {"team_id": team_id, "season_id": season.id},
                {
                    "team_id":              team_id,
                    "season_id":            season.id,
                    "games":                len(rows),
                    "points":               _sum_col(rows, "points"),
                    "points_allowed":       pts_allowed,
                    "total_yards":          _sum_col(rows, "total_yards"),
                    "pass_yards":           _sum_col(rows, "pass_yards"),
                    "rush_yards":           _sum_col(rows, "rush_yards"),
                    "pass_att":             _sum_col(rows, "pass_att"),
                    "pass_cmp":             _sum_col(rows, "pass_cmp"),
                    "pass_tds":             _sum_col(rows, "pass_tds"),
                    "pass_ints":            _sum_col(rows, "pass_ints"),
                    "sacks_allowed":        _sum_col(rows, "sacks_allowed"),
                    "sack_yards_allowed":   _sum_col(rows, "sack_yards_allowed"),
                    "rush_att":             _sum_col(rows, "rush_att"),
                    "rush_tds":             _sum_col(rows, "rush_tds"),
                    "first_downs":          _sum_col(rows, "first_downs"),
                    "third_att":            _sum_col(rows, "third_att"),
                    "third_conv":           _sum_col(rows, "third_conv"),
                    "fourth_att":           _sum_col(rows, "fourth_att"),
                    "fourth_conv":          _sum_col(rows, "fourth_conv"),
                    "rz_att":               _sum_col(rows, "rz_att"),
                    "rz_tds":               _sum_col(rows, "rz_tds"),
                    "rz_fgs":               _sum_col(rows, "rz_fgs"),
                    "turnovers":            _sum_col(rows, "turnovers"),
                    "fumbles_lost":         _sum_col(rows, "fumbles_lost"),
                    "penalties":            _sum_col(rows, "penalties"),
                    "penalty_yards":        _sum_col(rows, "penalty_yards"),
                    "top_seconds":          _sum_col(rows, "top_seconds"),
                    "def_sacks":            _sum_col(rows, "def_sacks"),
                    "def_ints":             _sum_col(rows, "def_ints"),
                    "def_forced_fumbles":   _sum_col(rows, "def_forced_fumbles"),
                    "def_fumble_recoveries": _sum_col(rows, "def_fumble_recoveries"),
                    "def_total_tackles":    _sum_col(rows, "def_total_tackles"),
                    "def_tfl":              _sum_col(rows, "def_tfl"),
                    "def_safeties":         _sum_col(rows, "def_safeties"),
                })

    db.flush()
    log.info("Season team stats rebuilt for season %d", season.season_index)


# ---------------------------------------------------------------------------
# 3. Standings
# ---------------------------------------------------------------------------

def _rebuild_standings(db: Session, season: Season, week: Week):
    """Recompute standings from the schedule table up through `week`."""
    teams = db.query(Team).all()
    completed_games = (
        db.query(ScheduleGame)
        .filter_by(season_id=season.id, is_completed=True)
        .all()
    )

    # Determine each team's division from the Team record
    team_div: dict[int, str] = {t.id: t.division or "" for t in teams}
    team_conf: dict[int, str] = {t.id: t.conference or "" for t in teams}

    # Tally W/L/T, home/away, div, conf
    records: dict[int, dict] = {
        t.id: {
            "wins": 0, "losses": 0, "ties": 0,
            "div_w": 0, "div_l": 0,
            "conf_w": 0, "conf_l": 0,
            "home_w": 0, "home_l": 0,
            "away_w": 0, "away_l": 0,
            "pf": 0, "pa": 0,
            "last_results": [],   # 'W' or 'L' for streak calc
        }
        for t in teams
    }

    for g in completed_games:
        h, a = g.home_team_id, g.away_team_id
        hs, as_ = g.home_score or 0, g.away_score or 0

        if h not in records or a not in records:
            continue

        records[h]["pf"] += hs
        records[h]["pa"] += as_
        records[a]["pf"] += as_
        records[a]["pa"] += hs

        # Determine result for each team
        if hs > as_:
            home_res, away_res = "W", "L"
        elif as_ > hs:
            home_res, away_res = "L", "W"
        else:
            home_res, away_res = "T", "T"

        for tid, res, opp_id, is_home in [
            (h, home_res, a, True),
            (a, away_res, h, False),
        ]:
            r = records[tid]
            if res == "W":
                r["wins"] += 1
                if is_home:
                    r["home_w"] += 1
                else:
                    r["away_w"] += 1
                if team_div.get(tid) == team_div.get(opp_id):
                    r["div_w"] += 1
                elif team_conf.get(tid) == team_conf.get(opp_id):
                    r["conf_w"] += 1
            elif res == "L":
                r["losses"] += 1
                if is_home:
                    r["home_l"] += 1
                else:
                    r["away_l"] += 1
                if team_div.get(tid) == team_div.get(opp_id):
                    r["div_l"] += 1
                elif team_conf.get(tid) == team_conf.get(opp_id):
                    r["conf_l"] += 1
            else:
                r["ties"] += 1

            r["last_results"].append(res)

    for t in teams:
        r = records[t.id]
        # Streak: count consecutive same results from the end
        streak = ""
        if r["last_results"]:
            last = r["last_results"][-1]
            cnt = 0
            for res in reversed(r["last_results"]):
                if res == last:
                    cnt += 1
                else:
                    break
            streak = f"{last}{cnt}"

        _upsert(db, Standing,
                {"team_id": t.id, "season_id": season.id, "week_id": week.id},
                {
                    "team_id":         t.id,
                    "season_id":       season.id,
                    "week_id":         week.id,
                    "wins":            r["wins"],
                    "losses":          r["losses"],
                    "ties":            r["ties"],
                    "division_wins":   r["div_w"],
                    "division_losses": r["div_l"],
                    "conf_wins":       r["conf_w"],
                    "conf_losses":     r["conf_l"],
                    "home_wins":       r["home_w"],
                    "home_losses":     r["home_l"],
                    "away_wins":       r["away_w"],
                    "away_losses":     r["away_l"],
                    "points_for":      r["pf"],
                    "points_against":  r["pa"],
                    "streak":          streak,
                })

    db.flush()
    log.info("Standings rebuilt through week %d", week.week_number)


# ---------------------------------------------------------------------------
# 4. Advanced player metrics
# ---------------------------------------------------------------------------

def _rebuild_advanced_player_metrics(db: Session, season: Season, week: Week):
    """
    Compute weekly and season-level advanced metrics for every player.
    Pulls from season_player_* tables (season scope) and weekly * tables (weekly scope).
    """
    # Team pass attempts this season (for target share)
    team_pass_att: dict[int, int] = {}
    for row in db.query(SeasonTeamStat).filter_by(season_id=season.id).all():
        team_pass_att[row.team_id] = row.pass_att or 0

    # --- Season scope ---
    player_ids = set()
    for tbl in [SeasonPlayerPassingStat, SeasonPlayerRushingStat,
                SeasonPlayerReceivingStat, SeasonPlayerDefenseStat,
                SeasonPlayerKickingStat, SeasonPlayerPuntingStat]:
        for row in db.query(tbl).filter_by(season_id=season.id).all():
            player_ids.add(row.player_id)

    for pid in player_ids:
        p = db.query(Player).get(pid)
        if p is None:
            continue

        ps = db.query(SeasonPlayerPassingStat).filter_by(player_id=pid, season_id=season.id).first()
        rs = db.query(SeasonPlayerRushingStat).filter_by(player_id=pid, season_id=season.id).first()
        rc = db.query(SeasonPlayerReceivingStat).filter_by(player_id=pid, season_id=season.id).first()
        df = db.query(SeasonPlayerDefenseStat).filter_by(player_id=pid, season_id=season.id).first()
        ks = db.query(SeasonPlayerKickingStat).filter_by(player_id=pid, season_id=season.id).first()
        pt = db.query(SeasonPlayerPuntingStat).filter_by(player_id=pid, season_id=season.id).first()

        # Resolve team for target share
        any_row = ps or rs or rc or df
        tpa = team_pass_att.get(any_row.team_id, 0) if any_row and any_row.team_id else 0

        metrics = build_player_metrics(
            position=p.position or "",
            team_pass_att=tpa,
            **_player_stat_kwargs(ps, rs, rc, df, ks, pt),
        )

        _upsert(db, AdvancedPlayerMetric,
                {"player_id": pid, "season_id": season.id, "week_id": None, "metric_scope": "season"},
                {"player_id": pid, "team_id": any_row.team_id if any_row else None,
                 "season_id": season.id, "week_id": None, "metric_scope": "season",
                 **metrics})

    db.flush()
    log.info("Advanced player metrics (season) computed for season %d", season.season_index)


def _player_stat_kwargs(ps, rs, rc, df, ks, pt) -> dict:
    """Flatten ORM rows into build_player_metrics kwargs."""
    kw: dict = {}

    if ps:
        kw.update(
            pass_cmp=ps.completions or 0,
            pass_att=ps.attempts or 0,
            pass_yds=ps.yards or 0,
            pass_tds=ps.tds or 0,
            pass_ints=ps.ints or 0,
            pass_sacks=ps.sacks or 0,
            pass_sack_yds=ps.sack_yards or 0,
            pass_yac=ps.yac or 0,
            pass_first_downs=ps.first_downs or 0,
            games=max(ps.games if hasattr(ps, "games") else 1, 1),
        )
    if rs:
        kw.update(
            rush_att=rs.attempts or 0,
            rush_yds=rs.yards or 0,
            rush_tds=rs.tds or 0,
            rush_fumbles=rs.fumbles or 0,
            rush_first_downs=rs.first_downs or 0,
            rush_broken_tackles=rs.broken_tackles or 0,
        )
    if rc:
        kw.update(
            rec_targets=rc.targets or 0,
            rec_catches=rc.receptions or 0,
            rec_yds=rc.yards or 0,
            rec_tds=rc.tds or 0,
            rec_drops=rc.drops or 0,
            rec_yac=rc.yac or 0,
            rec_first_downs=rc.first_downs or 0,
        )
    if df:
        kw.update(
            def_tackles=df.tackles_total or 0,
            def_missed_tackles=df.missed_tackles or 0,
            def_sacks=float(df.sacks or 0),
            def_tfl=float(df.tackles_for_loss or 0),
            def_forced_fumbles=df.forced_fumbles or 0,
            def_ints=df.ints or 0,
            def_pbу=df.pass_breakups or 0,
            # coverage targets approximated as pass_breakups + ints (minimum proxy)
            def_cov_targets=max((df.pass_breakups or 0) + (df.ints or 0), 1),
            def_cov_yds=df.int_yards or 0,
            def_cov_cmp=0,
            def_cov_td=df.int_tds or 0,
            def_cov_int=df.ints or 0,
            games=max(df.games if hasattr(df, "games") else 1, 1),
        )
    if ks:
        kw.update(
            kick_fg_att=ks.fg_att or 0,
            kick_fg_made=ks.fg_made or 0,
            kick_fg_att_30=ks.fg_att_39 or 0,
            kick_fg_made_30=ks.fg_made_39 or 0,
            kick_fg_att_40=ks.fg_att_49 or 0,
            kick_fg_made_40=ks.fg_made_49 or 0,
            kick_fg_att_50=ks.fg_att_50 or 0,
            kick_fg_made_50=ks.fg_made_50 or 0,
            kick_xp_att=ks.xp_att or 0,
            kick_xp_made=ks.xp_made or 0,
            kick_kickoffs=ks.kickoffs or 0,
            kick_kickoff_yds=ks.kickoff_yards or 0,
            kick_touchbacks=ks.touchbacks or 0,
        )
    if pt:
        kw.update(
            punt_count=pt.punts or 0,
            punt_gross_yds=pt.gross_yards or 0,
            punt_net_yds=pt.net_yards or 0,
            punt_inside_20=pt.inside_20 or 0,
        )

    return kw


# ---------------------------------------------------------------------------
# 5. Advanced team metrics
# ---------------------------------------------------------------------------

def _rebuild_advanced_team_metrics(db: Session, season: Season, week: Week):
    """Compute season-level team advanced metrics including SRS."""

    season_stats = db.query(SeasonTeamStat).filter_by(season_id=season.id).all()
    all_teams = {s.team_id: s for s in season_stats}
    standings_map: dict[int, Standing] = {}
    for s in db.query(Standing).filter_by(season_id=season.id, week_id=week.id).all():
        standings_map[s.team_id] = s

    # Build schedule structure for SRS
    completed_games = (
        db.query(ScheduleGame)
        .filter_by(season_id=season.id, is_completed=True)
        .all()
    )
    opp_lists: dict[int, list[int]] = {}
    for g in completed_games:
        opp_lists.setdefault(g.home_team_id, []).append(g.away_team_id)
        opp_lists.setdefault(g.away_team_id, []).append(g.home_team_id)

    # Aggregate opponent stats for each team (needed for defensive metrics)
    opp_stats: dict[int, dict] = {tid: {
        "pass_att": 0, "sacks": 0, "total_yards": 0,
        "pass_yards": 0, "rush_yards": 0, "rush_att": 0,
        "third_att": 0, "third_conv": 0,
        "rz_att": 0, "rz_tds": 0, "rz_fgs": 0,
        "pass_cmp": 0, "pass_tds": 0, "pass_ints": 0, "sack_yds": 0,
        "rush_tds": 0, "drives": 0,
    } for tid in all_teams}

    for g in completed_games:
        h_stats = all_teams.get(g.home_team_id)
        a_stats = all_teams.get(g.away_team_id)
        # Away team's offense = home team's defense faced
        if h_stats and a_stats:
            _acc_opp(opp_stats, g.home_team_id, a_stats)
            _acc_opp(opp_stats, g.away_team_id, h_stats)

    # SRS inputs
    team_ids = list(all_teams.keys())
    pf_map = {tid: all_teams[tid].points or 0 for tid in team_ids}
    pa_map = {tid: all_teams[tid].points_allowed or 0 for tid in team_ids}
    games_map = {tid: all_teams[tid].games or 1 for tid in team_ids}
    srs_vals = calc_srs(team_ids, pf_map, pa_map, games_map, opp_lists)

    # Strength of schedule: average opponent win pct
    win_pcts = {}
    for tid in team_ids:
        st = standings_map.get(tid)
        if st:
            total = (st.wins + st.losses + st.ties) or 1
            win_pcts[tid] = st.wins / total
        else:
            win_pcts[tid] = 0.5

    for team_id, ts in all_teams.items():
        opp = opp_stats.get(team_id, {})
        st = standings_map.get(team_id)
        g = max(ts.games or 1, 1)
        takeaways = (ts.def_ints or 0) + (ts.def_fumble_recoveries or 0)

        sos = calc_strength_of_schedule(
            team_id,
            [win_pcts.get(o, 0.5) for o in opp_lists.get(team_id, [])]
        )

        metrics = build_team_metrics(
            games=g,
            points=ts.points or 0,
            points_allowed=ts.points_allowed or 0,
            total_yards=ts.total_yards or 0,
            pass_yards=ts.pass_yards or 0,
            rush_yards=ts.rush_yards or 0,
            pass_att=ts.pass_att or 0,
            pass_cmp=ts.pass_cmp or 0,
            pass_tds=ts.pass_tds or 0,
            pass_ints=ts.pass_ints or 0,
            sacks_allowed=ts.sacks_allowed or 0,
            sack_yards_allowed=ts.sack_yards_allowed or 0,
            rush_att=ts.rush_att or 0,
            first_downs=ts.first_downs or 0,
            third_att=ts.third_att or 0,
            third_conv=ts.third_conv or 0,
            fourth_att=ts.fourth_att or 0,
            fourth_conv=ts.fourth_conv or 0,
            rz_att=ts.rz_att or 0,
            rz_tds=ts.rz_tds or 0,
            rz_fgs=ts.rz_fgs or 0,
            turnovers=ts.turnovers or 0,
            fumbles_lost=ts.fumbles_lost or 0,
            penalties=ts.penalties or 0,
            penalty_yards=ts.penalty_yards or 0,
            top_seconds=ts.top_seconds or 0,
            def_sacks=ts.def_sacks or 0,
            def_ints=ts.def_ints or 0,
            def_forced_fumbles=ts.def_forced_fumbles or 0,
            def_fumble_recoveries=ts.def_fumble_recoveries or 0,
            opp_pass_att=opp.get("pass_att", 0),
            opp_sacks=opp.get("sacks", 0),
            opp_total_yards=opp.get("total_yards", 0),
            opp_pass_yards=opp.get("pass_yards", 0),
            opp_rush_yards=opp.get("rush_yards", 0),
            opp_rush_att=opp.get("rush_att", 0),
            opp_third_att=opp.get("third_att", 0),
            opp_third_conv=opp.get("third_conv", 0),
            opp_rz_att=opp.get("rz_att", 0),
            opp_rz_tds=opp.get("rz_tds", 0),
            opp_rz_fgs=opp.get("rz_fgs", 0),
            opp_pass_cmp=opp.get("pass_cmp", 0),
            opp_pass_tds=opp.get("pass_tds", 0),
            opp_pass_ints=opp.get("pass_ints", 0),
            opp_sack_yds=opp.get("sack_yds", 0),
            opp_rush_tds=opp.get("rush_tds", 0),
            opp_drives=max(opp.get("drives", g), 1),
            takeaways=takeaways,
            srs=srs_vals.get(team_id, 0.0),
            sos=sos,
        )

        _upsert(db, AdvancedTeamMetric,
                {"team_id": team_id, "season_id": season.id, "week_id": None, "metric_scope": "season"},
                {"team_id": team_id, "season_id": season.id, "week_id": None,
                 "metric_scope": "season", **metrics})

    db.flush()
    log.info("Advanced team metrics (season) computed for season %d", season.season_index)


def _acc_opp(opp_stats: dict, team_id: int, opponent_row: SeasonTeamStat):
    """Accumulate opponent stats into opp_stats[team_id]."""
    o = opp_stats[team_id]
    o["pass_att"]    += opponent_row.pass_att or 0
    o["sacks"]       += opponent_row.sacks_allowed or 0
    o["total_yards"] += opponent_row.total_yards or 0
    o["pass_yards"]  += opponent_row.pass_yards or 0
    o["rush_yards"]  += opponent_row.rush_yards or 0
    o["rush_att"]    += opponent_row.rush_att or 0
    o["third_att"]   += opponent_row.third_att or 0
    o["third_conv"]  += opponent_row.third_conv or 0
    o["rz_att"]      += opponent_row.rz_att or 0
    o["rz_tds"]      += opponent_row.rz_tds or 0
    o["rz_fgs"]      += opponent_row.rz_fgs or 0
    o["pass_cmp"]    += opponent_row.pass_cmp or 0
    o["pass_tds"]    += opponent_row.pass_tds or 0
    o["pass_ints"]   += opponent_row.pass_ints or 0
    o["sack_yds"]    += opponent_row.sack_yards_allowed or 0
    o["rush_tds"]    += opponent_row.rush_tds or 0
    o["drives"]      += opponent_row.games or 0


# ---------------------------------------------------------------------------
# Master entry point
# ---------------------------------------------------------------------------

def run_aggregator(db: Session, season: Season, week: Week):
    """
    Run all aggregation steps in order.
    Called by ingest.py after loader completes.
    """
    log.info("Starting aggregation for season %d, week %d",
             season.season_index, week.week_index)

    _rebuild_season_player_stats(db, season)
    _rebuild_season_team_stats(db, season)
    _rebuild_standings(db, season, week)
    _rebuild_advanced_player_metrics(db, season, week)
    _rebuild_advanced_team_metrics(db, season, week)

    db.commit()
    log.info("Aggregation complete.")
