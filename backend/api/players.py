from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import or_
from database import get_db
from models import (
    Player, PlayerSnapshot, Team, Season, Week,
    SeasonPlayerPassingStat, SeasonPlayerRushingStat,
    SeasonPlayerReceivingStat, SeasonPlayerDefenseStat,
    SeasonPlayerKickingStat, SeasonPlayerPuntingStat,
    PlayerPassingStat, PlayerRushingStat, PlayerReceivingStat,
    PlayerDefenseStat, PlayerKickingStat, PlayerPuntingStat,
    AdvancedPlayerMetric,
)

router = APIRouter()


def _fmt_player(p: Player, snap: PlayerSnapshot = None, team: Team = None) -> dict:
    dev_labels = {0: "Normal", 1: "Impact", 2: "Star", 3: "X-Factor"}
    return {
        "id":             p.id,
        "roster_id":      p.roster_id,
        "full_name":      p.full_name,
        "first_name":     p.first_name,
        "last_name":      p.last_name,
        "position":       p.position,
        "archetype":      p.archetype,
        "jersey":         p.jersey_number,
        "team_id":        snap.team_id if snap else None,
        "team_name":      f"{team.city} {team.name}" if team else None,
        "team_abbr":      team.abbreviation if team else None,
        "overall":        snap.overall_rating if snap else None,
        "age":            snap.age if snap else None,
        "years_pro":      snap.years_pro if snap else None,
        "dev_trait":      dev_labels.get(snap.dev_trait, "Normal") if snap else "Normal",
        "injury_status":  snap.injury_status if snap else None,
        "contract_salary": snap.contract_salary if snap else None,
        "contract_years_left": snap.contract_years_left if snap else None,
    }


@router.get("/api/players")
def list_players(
    position: str = Query(None),
    team_id: int = Query(None),
    search: str = Query(None),
    season_index: int = Query(None),
    min_overall: int = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """List players with optional filtering."""
    # Get latest snapshot week
    from models import Season as S
    season = (db.query(S).filter_by(season_index=season_index).first()
              if season_index else db.query(S).order_by(S.season_index.desc()).first())

    week = None
    if season:
        week = (db.query(Week).filter_by(season_id=season.id)
                .order_by(Week.week_index.desc()).first())

    q = db.query(Player)
    if position:
        q = q.filter(Player.position == position.upper())
    if search:
        q = q.filter(or_(
            Player.full_name.ilike(f"%{search}%"),
            Player.last_name.ilike(f"%{search}%"),
        ))

    players = q.order_by(Player.last_name).offset(offset).limit(limit).all()

    result = []
    for p in players:
        snap = None
        if week:
            snap = db.query(PlayerSnapshot).filter_by(
                player_id=p.id, week_id=week.id
            ).first()
        if snap is None:
            snap = (db.query(PlayerSnapshot)
                    .filter_by(player_id=p.id)
                    .order_by(PlayerSnapshot.id.desc())
                    .first())

        if team_id and (snap is None or snap.team_id != team_id):
            continue
        if min_overall and (snap is None or (snap.overall_rating or 0) < min_overall):
            continue

        team = db.query(Team).get(snap.team_id) if snap and snap.team_id else None
        result.append(_fmt_player(p, snap, team))

    return result


@router.get("/api/players/{player_id}")
def get_player(player_id: int, db: Session = Depends(get_db)):
    """Full player profile: bio, latest attributes, career week-by-week stats."""
    p = db.query(Player).get(player_id)
    if not p:
        raise HTTPException(404, "Player not found")

    # All snapshots (attribute history)
    snapshots = (db.query(PlayerSnapshot, Week, Team)
                 .join(Week, PlayerSnapshot.week_id == Week.id)
                 .outerjoin(Team, PlayerSnapshot.team_id == Team.id)
                 .filter(PlayerSnapshot.player_id == p.id)
                 .order_by(Week.week_index)
                 .all())

    latest_snap = snapshots[-1][0] if snapshots else None
    latest_team = snapshots[-1][2] if snapshots else None

    profile = _fmt_player(p, latest_snap, latest_team)

    # Attribute history for charts
    profile["attribute_history"] = [
        {
            "week": w.week_number,
            "week_index": w.week_index,
            "season_index": db.query(Season).get(w.season_id).season_index if w.season_id else None,
            "overall": s.overall_rating,
            "speed": s.speed,
            "dev_trait": s.dev_trait,
            "team_id": s.team_id,
        }
        for s, w, _ in snapshots
    ]

    # Season passing stats
    profile["season_passing"] = _season_passing(db, p.id)
    profile["season_rushing"] = _season_rushing(db, p.id)
    profile["season_receiving"] = _season_receiving(db, p.id)
    profile["season_defense"] = _season_defense(db, p.id)
    profile["season_kicking"] = _season_kicking(db, p.id)
    profile["season_punting"] = _season_punting(db, p.id)

    # Weekly game log (all stat types across all seasons)
    profile["weekly_passing"] = _weekly_passing(db, p.id)
    profile["weekly_rushing"] = _weekly_rushing(db, p.id)
    profile["weekly_receiving"] = _weekly_receiving(db, p.id)
    profile["weekly_defense"] = _weekly_defense(db, p.id)

    # Advanced metrics (season)
    profile["advanced_metrics"] = _advanced_metrics(db, p.id)

    return profile


def _season_passing(db, player_id):
    rows = (db.query(SeasonPlayerPassingStat, Season)
            .join(Season)
            .filter(SeasonPlayerPassingStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "completions": r.completions, "attempts": r.attempts,
            "yards": r.yards, "tds": r.tds, "ints": r.ints,
            "sacks": r.sacks, "sack_yards": r.sack_yards,
            "yac": r.yac, "longest": r.longest, "first_downs": r.first_downs,
        }
        for r, s in rows
    ]


def _season_rushing(db, player_id):
    rows = (db.query(SeasonPlayerRushingStat, Season)
            .join(Season)
            .filter(SeasonPlayerRushingStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "attempts": r.attempts, "yards": r.yards, "tds": r.tds,
            "fumbles": r.fumbles, "fumbles_lost": r.fumbles_lost,
            "yac": r.yac, "longest": r.longest, "first_downs": r.first_downs,
            "broken_tackles": r.broken_tackles,
        }
        for r, s in rows
    ]


def _season_receiving(db, player_id):
    rows = (db.query(SeasonPlayerReceivingStat, Season)
            .join(Season)
            .filter(SeasonPlayerReceivingStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "targets": r.targets, "receptions": r.receptions,
            "yards": r.yards, "tds": r.tds, "drops": r.drops,
            "yac": r.yac, "longest": r.longest, "first_downs": r.first_downs,
        }
        for r, s in rows
    ]


def _season_defense(db, player_id):
    rows = (db.query(SeasonPlayerDefenseStat, Season)
            .join(Season)
            .filter(SeasonPlayerDefenseStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "tackles_solo": r.tackles_solo, "tackles_assist": r.tackles_assist,
            "tackles_total": r.tackles_total, "tackles_for_loss": r.tackles_for_loss,
            "sacks": r.sacks, "sack_yards": r.sack_yards,
            "ints": r.ints, "int_yards": r.int_yards, "int_tds": r.int_tds,
            "pass_breakups": r.pass_breakups,
            "forced_fumbles": r.forced_fumbles, "fumble_recoveries": r.fumble_recoveries,
            "safeties": r.safeties, "missed_tackles": r.missed_tackles,
        }
        for r, s in rows
    ]


def _season_kicking(db, player_id):
    rows = (db.query(SeasonPlayerKickingStat, Season)
            .join(Season)
            .filter(SeasonPlayerKickingStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "fg_att": r.fg_att, "fg_made": r.fg_made,
            "fg_att_19": r.fg_att_19, "fg_made_19": r.fg_made_19,
            "fg_att_29": r.fg_att_29, "fg_made_29": r.fg_made_29,
            "fg_att_39": r.fg_att_39, "fg_made_39": r.fg_made_39,
            "fg_att_49": r.fg_att_49, "fg_made_49": r.fg_made_49,
            "fg_att_50": r.fg_att_50, "fg_made_50": r.fg_made_50,
            "fg_long": r.fg_long,
            "xp_att": r.xp_att, "xp_made": r.xp_made,
            "kickoffs": r.kickoffs, "kickoff_yards": r.kickoff_yards,
            "touchbacks": r.touchbacks,
        }
        for r, s in rows
    ]


def _season_punting(db, player_id):
    rows = (db.query(SeasonPlayerPuntingStat, Season)
            .join(Season)
            .filter(SeasonPlayerPuntingStat.player_id == player_id)
            .order_by(Season.season_index)
            .all())
    return [
        {
            "season_index": s.season_index,
            "games": r.games,
            "punts": r.punts, "gross_yards": r.gross_yards,
            "net_yards": r.net_yards, "longest": r.longest,
            "touchbacks": r.touchbacks, "inside_20": r.inside_20,
        }
        for r, s in rows
    ]


def _weekly_passing(db, player_id):
    rows = (db.query(PlayerPassingStat, Week, Season)
            .join(Week, PlayerPassingStat.week_id == Week.id)
            .join(Season, PlayerPassingStat.season_id == Season.id)
            .filter(PlayerPassingStat.player_id == player_id)
            .order_by(Season.season_index, Week.week_index)
            .all())
    return [
        {
            "season_index": s.season_index, "week": w.week_number,
            "completions": r.completions, "attempts": r.attempts,
            "yards": r.yards, "tds": r.tds, "ints": r.ints,
            "sacks": r.sacks, "yac": r.yac, "longest": r.longest,
            "passer_rating": r.passer_rating,
        }
        for r, w, s in rows
    ]


def _weekly_rushing(db, player_id):
    rows = (db.query(PlayerRushingStat, Week, Season)
            .join(Week, PlayerRushingStat.week_id == Week.id)
            .join(Season, PlayerRushingStat.season_id == Season.id)
            .filter(PlayerRushingStat.player_id == player_id)
            .order_by(Season.season_index, Week.week_index)
            .all())
    return [
        {
            "season_index": s.season_index, "week": w.week_number,
            "attempts": r.attempts, "yards": r.yards, "tds": r.tds,
            "fumbles": r.fumbles, "longest": r.longest,
        }
        for r, w, s in rows
    ]


def _weekly_receiving(db, player_id):
    rows = (db.query(PlayerReceivingStat, Week, Season)
            .join(Week, PlayerReceivingStat.week_id == Week.id)
            .join(Season, PlayerReceivingStat.season_id == Season.id)
            .filter(PlayerReceivingStat.player_id == player_id)
            .order_by(Season.season_index, Week.week_index)
            .all())
    return [
        {
            "season_index": s.season_index, "week": w.week_number,
            "targets": r.targets, "receptions": r.receptions,
            "yards": r.yards, "tds": r.tds, "drops": r.drops, "yac": r.yac,
        }
        for r, w, s in rows
    ]


def _weekly_defense(db, player_id):
    rows = (db.query(PlayerDefenseStat, Week, Season)
            .join(Week, PlayerDefenseStat.week_id == Week.id)
            .join(Season, PlayerDefenseStat.season_id == Season.id)
            .filter(PlayerDefenseStat.player_id == player_id)
            .order_by(Season.season_index, Week.week_index)
            .all())
    return [
        {
            "season_index": s.season_index, "week": w.week_number,
            "tackles_total": r.tackles_total, "tackles_for_loss": r.tackles_for_loss,
            "sacks": r.sacks, "ints": r.ints, "pass_breakups": r.pass_breakups,
            "forced_fumbles": r.forced_fumbles, "missed_tackles": r.missed_tackles,
        }
        for r, w, s in rows
    ]


def _advanced_metrics(db, player_id):
    rows = (db.query(AdvancedPlayerMetric, Season)
            .join(Season)
            .filter(
                AdvancedPlayerMetric.player_id == player_id,
                AdvancedPlayerMetric.metric_scope == "season",
            )
            .order_by(Season.season_index)
            .all())
    results = []
    for m, s in rows:
        row = {"season_index": s.season_index}
        for col in AdvancedPlayerMetric.__table__.columns.keys():
            if col not in ("id", "player_id", "team_id", "season_id", "week_id", "metric_scope"):
                val = getattr(m, col)
                if val is not None:
                    row[col] = val
        results.append(row)
    return results


@router.get("/api/players/leaders/{metric}")
def stat_leaders(
    metric: str,
    position: str = Query(None),
    season_index: int = Query(None),
    limit: int = Query(25),
    db: Session = Depends(get_db),
):
    """
    Return top players for a given advanced metric.
    metric examples: any_a, ypc, catch_rate, passer_rating, sacks_per_game, fg_pct
    """
    from models import Season as S
    season = (db.query(S).filter_by(season_index=season_index).first()
              if season_index else db.query(S).order_by(S.season_index.desc()).first())
    if not season:
        return []

    col = getattr(AdvancedPlayerMetric, metric, None)
    if col is None:
        raise HTTPException(400, f"Unknown metric: {metric}")

    q = (db.query(AdvancedPlayerMetric, Player, Team)
         .join(Player, AdvancedPlayerMetric.player_id == Player.id)
         .outerjoin(Team, AdvancedPlayerMetric.team_id == Team.id)
         .filter(
             AdvancedPlayerMetric.season_id == season.id,
             AdvancedPlayerMetric.metric_scope == "season",
             col.isnot(None),
         ))

    if position:
        q = q.filter(Player.position == position.upper())

    q = q.order_by(col.desc()).limit(limit)

    return [
        {
            "rank":          i + 1,
            "player_id":     p.id,
            "name":          p.full_name,
            "position":      p.position,
            "team":          t.abbreviation if t else "—",
            metric:          getattr(m, metric),
        }
        for i, (m, p, t) in enumerate(q.all())
    ]
