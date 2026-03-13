"""
ETL Loader — parses incoming Madden Companion App JSON and writes it to the DB.

The Companion App POSTs a JSON object that may contain any subset of:
  leagueInfo, leagueRosterInfoList,
  playerPassingStatInfoList, playerRushingStatInfoList,
  playerReceivingStatInfoList, playerDefenseStatInfoList,
  playerKickingStatInfoList, playerPuntingStatInfoList,
  teamStatInfoList, scheduleInfoList

Each section is optional. We handle partial exports gracefully.

Field-name mapping: Madden uses camelCase keys. We map them to our snake_case
columns via the FIELD_MAP constants at the top of each section.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

from models import (
    League, Season, Week, Team, Player, PlayerSnapshot,
    PlayerPassingStat, PlayerRushingStat, PlayerReceivingStat,
    PlayerDefenseStat, PlayerKickingStat, PlayerPuntingStat,
    TeamStat, ScheduleGame, Standing,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int(val: Any, default: int = 0) -> int:
    try:
        return int(val or default)
    except (TypeError, ValueError):
        return default


def _float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val or default)
    except (TypeError, ValueError):
        return default


def _str(val: Any, default: str = "") -> str:
    return str(val) if val is not None else default


def _upsert(db: Session, model_class, lookup: dict, data: dict):
    """
    Get-or-create + update pattern.
    `lookup` is the dict of unique-key fields used to find an existing row.
    `data` is the full set of fields to set (including lookup fields).
    """
    obj = db.query(model_class).filter_by(**lookup).first()
    if obj is None:
        obj = model_class(**data)
        db.add(obj)
    else:
        for k, v in data.items():
            setattr(obj, k, v)
    return obj


# ---------------------------------------------------------------------------
# Section: League Info
# ---------------------------------------------------------------------------

def load_league_info(db: Session, payload: dict) -> tuple[League, Season, Week]:
    """
    Parses the `leagueInfo` key and ensures league/season/week rows exist.
    Returns (league, season, week) ORM objects.
    """
    info = payload.get("leagueInfo", {})

    # Some exports wrap it in a list
    if isinstance(info, list):
        info = info[0] if info else {}

    league_id = _str(info.get("leagueId") or info.get("LEAGUEID") or "default")
    league_name = _str(info.get("leagueName") or info.get("LEAGUENAME") or "My League")
    season_index = _int(info.get("seasonIndex") or info.get("currentSeason") or 0)
    week_index = _int(info.get("weekIndex") or info.get("currentWeek") or 0)
    week_type_raw = _str(info.get("stageIndex") or info.get("phase") or "1")

    # stageIndex: 0=preseason, 1=regular season, 2=playoffs
    stage_map = {"0": "preseason", "1": "regular", "2": "postseason"}
    week_type = stage_map.get(week_type_raw, "regular")

    # Week number: Madden weekIndex is 0-based, we display 1-based
    week_number = week_index + 1

    league = _upsert(db, League,
                     {"league_id": league_id},
                     {"league_id": league_id, "name": league_name, "platform": "mixed"})

    db.flush()

    season = _upsert(db, Season,
                     {"league_id": league.id, "season_index": season_index},
                     {"league_id": league.id, "season_index": season_index,
                      "current_week": week_number, "phase": week_type})

    db.flush()

    week = _upsert(db, Week,
                   {"season_id": season.id, "week_index": week_index},
                   {"season_id": season.id, "week_index": week_index,
                    "week_number": week_number, "week_type": week_type})

    db.flush()

    # Load teams from leagueInfo.teamInfoList if present
    for t in info.get("teamInfoList", []):
        _upsert(db, Team,
                {"team_id": _int(t.get("teamId"))},
                {
                    "team_id":      _int(t.get("teamId")),
                    "city":         _str(t.get("cityName") or t.get("city")),
                    "name":         _str(t.get("nickName") or t.get("name")),
                    "abbreviation": _str(t.get("abbrName") or t.get("abbreviation")),
                    "division":     _str(t.get("divName") or t.get("division")),
                    "conference":   _str(t.get("confName") or t.get("conference")),
                })

    db.flush()
    return league, season, week


# ---------------------------------------------------------------------------
# Section: Roster
# ---------------------------------------------------------------------------

# Madden attribute field names → our column names
_ATTR_MAP = {
    "speedRating":         "speed",
    "accelRating":         "acceleration",
    "agilityRating":       "agility",
    "strengthRating":      "strength",
    "awareRating":         "awareness",
    "catchRating":         "catching",
    "carryRating":         "carrying",
    "jumpRating":          "jumping",
    "staminaRating":       "stamina",
    "injuryRating":        "injury",
    "toughRating":         "toughness",
    "throwPowerRating":    "throw_power",
    "throwAccShortRating": "throw_acc_short",
    "throwAccMidRating":   "throw_acc_mid",
    "throwAccDeepRating":  "throw_acc_deep",
    "throwOnRunRating":    "throw_on_run",
    "brkTackleRating":     "break_tackle",
    "brkSackRating":       "break_sack",
    "passBlockRating":     "pass_block",
    "passBlockPwrRating":  "pass_block_power",
    "passBlockFinRating":  "pass_block_finesse",
    "runBlockRating":      "run_block",
    "runBlockPwrRating":   "run_block_power",
    "runBlockFinRating":   "run_block_finesse",
    "tackleRating":        "tackle",
    "hitPowerRating":      "hit_power",
    "pursuitRating":       "pursuit",
    "playRecRating":       "play_recognition",
    "manCoverRating":      "man_coverage",
    "zoneCoverRating":     "zone_coverage",
    "pressRating":         "press",
    "kickPowerRating":     "kick_power",
    "kickAccRating":       "kick_accuracy",
    "kickRetRating":       "kick_return",
    "specCatchRating":     "spectacular_catch",
    "catchInTrafficRating":"catch_in_traffic",
    "shortRouteRating":    "short_route_running",
    "medRouteRating":      "medium_route_running",
    "deepRouteRating":     "deep_route_running",
    "releaseRating":       "release",
    "jukeMoveRating":      "juke_move",
    "spinMoveRating":      "spin_move",
    "stiffArmRating":      "stiff_arm",
    "truckingRating":      "trucking",
    "changeOfDirRating":   "change_of_direction",
}

# Dev trait numeric map from Madden's values
_DEV_TRAIT_MAP = {
    "Normal": 0, "Impact": 1, "Star": 2, "XFactor": 3,
    "0": 0, "1": 1, "2": 2, "3": 3,
}


def load_roster(db: Session, payload: dict, season: Season, week: Week):
    """Parse leagueRosterInfoList and upsert players + snapshots."""
    roster_list = payload.get("leagueRosterInfoList", [])
    if not roster_list:
        return 0

    count = 0
    for r in roster_list:
        roster_id = _int(r.get("rosterId"))
        if roster_id == 0:
            continue

        first = _str(r.get("firstName"))
        last = _str(r.get("lastName"))
        full = _str(r.get("fullName") or f"{first} {last}").strip()

        player = _upsert(db, Player,
                         {"roster_id": roster_id},
                         {
                             "roster_id":    roster_id,
                             "first_name":   first,
                             "last_name":    last,
                             "full_name":    full,
                             "position":     _str(r.get("position")),
                             "archetype":    _str(r.get("archetype")),
                             "college":      _str(r.get("college")),
                             "home_state":   _str(r.get("homeState")),
                             "jersey_number": _int(r.get("jerseyNum")),
                         })
        db.flush()

        # Build snapshot data
        snapshot_data = {
            "player_id":         player.id,
            "week_id":           week.id,
            "team_id":           _get_team_id(db, _int(r.get("teamId"))),
            "age":               _int(r.get("age")),
            "years_pro":         _int(r.get("yearsPro")),
            "dev_trait":         _DEV_TRAIT_MAP.get(str(r.get("devTrait", 0)), 0),
            "overall_rating":    _int(r.get("playerBestOvr") or r.get("trueOverall") or r.get("overall")),
            "injury_status":     _str(r.get("injuryType")),
            "injury_length":     _int(r.get("injuryLength")),
            "contract_salary":   _int(r.get("contractSalary")),
            "contract_bonus":    _int(r.get("contractBonus")),
            "contract_length":   _int(r.get("contractLength")),
            "contract_years_left": _int(r.get("contractYearsLeft")),
            "depth_position":    _str(r.get("depthPos")),
            "depth_order":       _int(r.get("depthOrder")),
        }
        # Map all attribute ratings
        for madden_key, col_name in _ATTR_MAP.items():
            snapshot_data[col_name] = _int(r.get(madden_key))

        _upsert(db, PlayerSnapshot,
                {"player_id": player.id, "week_id": week.id},
                snapshot_data)
        count += 1

    db.flush()
    log.info("Roster: loaded %d player snapshots", count)
    return count


def _get_team_id(db: Session, madden_team_id: int) -> int | None:
    """Resolve Madden teamId → our teams.id FK. Returns None for free agents (teamId 0)."""
    if madden_team_id == 0:
        return None
    team = db.query(Team).filter_by(team_id=madden_team_id).first()
    return team.id if team else None


def _get_player_id(db: Session, roster_id: int) -> int | None:
    player = db.query(Player).filter_by(roster_id=roster_id).first()
    return player.id if player else None


# ---------------------------------------------------------------------------
# Section: Player Stats — common helper
# ---------------------------------------------------------------------------

def _stat_meta(db: Session, row: dict, season: Season, week: Week) -> tuple | None:
    """Returns (player_id, team_id, season.id, week.id) or None if player unknown."""
    roster_id = _int(row.get("rosterId"))
    if roster_id == 0:
        return None

    # Auto-create minimal player record if we've never seen them (stats-only export)
    player = db.query(Player).filter_by(roster_id=roster_id).first()
    if player is None:
        first = _str(row.get("firstName"))
        last = _str(row.get("lastName"))
        player = Player(
            roster_id=roster_id,
            first_name=first,
            last_name=last,
            full_name=_str(row.get("fullName") or f"{first} {last}").strip(),
            position=_str(row.get("position")),
        )
        db.add(player)
        db.flush()

    team_id = _get_team_id(db, _int(row.get("teamId")))
    return player.id, team_id, season.id, week.id


# ---------------------------------------------------------------------------
# Section: Passing Stats
# ---------------------------------------------------------------------------

def load_passing_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerPassingStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        _upsert(db, PlayerPassingStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":    player_id,
                    "team_id":      team_id,
                    "season_id":    season_id,
                    "week_id":      week_id,
                    "completions":  _int(row.get("passCmp")),
                    "attempts":     _int(row.get("passAtt")),
                    "yards":        _int(row.get("passYds")),
                    "tds":          _int(row.get("passTDs") or row.get("passTD")),
                    "ints":         _int(row.get("passInts") or row.get("passInt")),
                    "sacks":        _int(row.get("passSacks") or row.get("passSack")),
                    "sack_yards":   _int(row.get("passSackYds") or row.get("passSackedYds")),
                    "yac":          _int(row.get("passYdsAfterCatch") or row.get("passYAC")),
                    "longest":      _int(row.get("passLong")),
                    "first_downs":  _int(row.get("passFirstDowns") or row.get("passFirstDown")),
                    "passer_rating": _float(row.get("passRating")),
                })
        count += 1

    db.flush()
    log.info("Passing stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Rushing Stats
# ---------------------------------------------------------------------------

def load_rushing_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerRushingStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        _upsert(db, PlayerRushingStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":       player_id,
                    "team_id":         team_id,
                    "season_id":       season_id,
                    "week_id":         week_id,
                    "attempts":        _int(row.get("rushAtt")),
                    "yards":           _int(row.get("rushYds")),
                    "tds":             _int(row.get("rushTDs") or row.get("rushTD")),
                    "fumbles":         _int(row.get("rushFumbles") or row.get("rushFum")),
                    "fumbles_lost":    _int(row.get("rushFumblesLost") or row.get("rushFumLost")),
                    "yac":             _int(row.get("rushYdsAfterContact") or row.get("rushYAC")),
                    "longest":         _int(row.get("rushLong")),
                    "first_downs":     _int(row.get("rushFirstDowns") or row.get("rushFirstDown")),
                    "broken_tackles":  _int(row.get("rushBrokenTackles") or row.get("rushBrkTackles")),
                })
        count += 1

    db.flush()
    log.info("Rushing stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Receiving Stats
# ---------------------------------------------------------------------------

def load_receiving_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerReceivingStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        _upsert(db, PlayerReceivingStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":   player_id,
                    "team_id":     team_id,
                    "season_id":   season_id,
                    "week_id":     week_id,
                    "targets":     _int(row.get("recTargets") or row.get("recTgt")),
                    "receptions":  _int(row.get("recCatches") or row.get("recCatch") or row.get("recRec")),
                    "yards":       _int(row.get("recYds")),
                    "tds":         _int(row.get("recTDs") or row.get("recTD")),
                    "drops":       _int(row.get("recDrops") or row.get("recDrop")),
                    "yac":         _int(row.get("recYdsAfterCatch") or row.get("recYAC")),
                    "longest":     _int(row.get("recLong")),
                    "first_downs": _int(row.get("recFirstDowns") or row.get("recFirstDown")),
                })
        count += 1

    db.flush()
    log.info("Receiving stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Defense Stats
# ---------------------------------------------------------------------------

def load_defense_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerDefenseStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        total = (_int(row.get("defTotalTackles") or row.get("defTackles"))
                 or _int(row.get("defSoloTackles")) + _int(row.get("defAssistedTackles")))

        _upsert(db, PlayerDefenseStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":          player_id,
                    "team_id":            team_id,
                    "season_id":          season_id,
                    "week_id":            week_id,
                    "tackles_solo":       _int(row.get("defSoloTackles")),
                    "tackles_assist":     _int(row.get("defAssistedTackles") or row.get("defAssistTackles")),
                    "tackles_total":      total,
                    "tackles_for_loss":   _float(row.get("defTFL") or row.get("defTacklesForLoss")),
                    "sacks":              _float(row.get("defSacks") or row.get("defSack")),
                    "sack_yards":         _int(row.get("defSackYds") or row.get("defSackYards")),
                    "ints":               _int(row.get("defInts") or row.get("defInt")),
                    "int_yards":          _int(row.get("defIntYds") or row.get("defIntYards")),
                    "int_tds":            _int(row.get("defIntTDs") or row.get("defIntTD")),
                    "pass_breakups":      _int(row.get("defPBUs") or row.get("defPassDef") or row.get("defPBU")),
                    "forced_fumbles":     _int(row.get("defForcedFumbles") or row.get("defForceFumbles") or row.get("defFF")),
                    "fumble_recoveries":  _int(row.get("defFumRecoveries") or row.get("defFumRec")),
                    "safeties":           _int(row.get("defSafeties") or row.get("defSafety")),
                    "kick_blocks":        _int(row.get("defKickBlocks") or row.get("defKickBlock")),
                    "missed_tackles":     _int(row.get("defMissedTackles") or row.get("defMissTackles")),
                    "td_allowed":         _int(row.get("defTDsAllowed")),
                })
        count += 1

    db.flush()
    log.info("Defense stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Kicking Stats
# ---------------------------------------------------------------------------

def load_kicking_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerKickingStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        _upsert(db, PlayerKickingStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":     player_id,
                    "team_id":       team_id,
                    "season_id":     season_id,
                    "week_id":       week_id,
                    "fg_att":        _int(row.get("kickFGAtt")),
                    "fg_made":       _int(row.get("kickFGMade")),
                    "fg_att_19":     _int(row.get("kickFGAtt19")),
                    "fg_made_19":    _int(row.get("kickFGMade19")),
                    "fg_att_29":     _int(row.get("kickFGAtt29")),
                    "fg_made_29":    _int(row.get("kickFGMade29")),
                    "fg_att_39":     _int(row.get("kickFGAtt39")),
                    "fg_made_39":    _int(row.get("kickFGMade39")),
                    "fg_att_49":     _int(row.get("kickFGAtt49")),
                    "fg_made_49":    _int(row.get("kickFGMade49")),
                    "fg_att_50":     _int(row.get("kickFG50PlusAtt") or row.get("kickFGAtt50")),
                    "fg_made_50":    _int(row.get("kickFG50PlusMade") or row.get("kickFGMade50")),
                    "fg_long":       _int(row.get("kickFGLong")),
                    "xp_att":        _int(row.get("kickXPAtt")),
                    "xp_made":       _int(row.get("kickXPMade")),
                    "kickoffs":      _int(row.get("kickoffAtt") or row.get("kickoffs")),
                    "kickoff_yards": _int(row.get("kickoffYds") or row.get("kickoffYards")),
                    "touchbacks":    _int(row.get("kickoffTouchbacks") or row.get("touchbacks")),
                })
        count += 1

    db.flush()
    log.info("Kicking stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Punting Stats
# ---------------------------------------------------------------------------

def load_punting_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("playerPuntingStatInfoList", [])
    count = 0
    for row in rows:
        meta = _stat_meta(db, row, season, week)
        if meta is None:
            continue
        player_id, team_id, season_id, week_id = meta

        gross = _int(row.get("puntYds") or row.get("puntGrossYds"))
        net = _int(row.get("puntNetYds"))

        _upsert(db, PlayerPuntingStat,
                {"player_id": player_id, "week_id": week_id},
                {
                    "player_id":   player_id,
                    "team_id":     team_id,
                    "season_id":   season_id,
                    "week_id":     week_id,
                    "punts":       _int(row.get("puntAtt") or row.get("punts")),
                    "gross_yards": gross,
                    "net_yards":   net,
                    "longest":     _int(row.get("puntLong")),
                    "touchbacks":  _int(row.get("puntTouchbacks") or row.get("puntTouchback")),
                    "inside_20":   _int(row.get("puntsInside20") or row.get("puntInside20")),
                })
        count += 1

    db.flush()
    log.info("Punting stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Team Stats
# ---------------------------------------------------------------------------

def load_team_stats(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("teamStatInfoList", [])
    count = 0
    for row in rows:
        madden_team_id = _int(row.get("teamId"))
        team_id = _get_team_id(db, madden_team_id)
        if team_id is None:
            # Auto-create bare team if missing (stats-only export)
            team = Team(team_id=madden_team_id, name=f"Team {madden_team_id}")
            db.add(team)
            db.flush()
            team_id = team.id

        _upsert(db, TeamStat,
                {"team_id": team_id, "week_id": week.id},
                {
                    "team_id":             team_id,
                    "season_id":           season.id,
                    "week_id":             week.id,
                    "points":              _int(row.get("ptsScored") or row.get("score") or row.get("points")),
                    "total_yards":         _int(row.get("totalYds") or row.get("totalOffenseYds")),
                    "pass_yards":          _int(row.get("totalPassYds") or row.get("passYds")),
                    "rush_yards":          _int(row.get("totalRushYds") or row.get("rushYds")),
                    "pass_att":            _int(row.get("passAtt")),
                    "pass_cmp":            _int(row.get("passCmp")),
                    "pass_tds":            _int(row.get("passTDs") or row.get("passTD")),
                    "pass_ints":           _int(row.get("passInts") or row.get("passInt")),
                    "sacks_allowed":       _int(row.get("sacksAllowed") or row.get("sacks")),
                    "sack_yards_allowed":  _int(row.get("sackYardsAllowed") or row.get("sackYdsAllowed")),
                    "rush_att":            _int(row.get("rushAtt")),
                    "rush_tds":            _int(row.get("rushTDs") or row.get("rushTD")),
                    "first_downs":         _int(row.get("firstDowns")),
                    "third_att":           _int(row.get("thirdDownAtt") or row.get("thirdDownAttempts")),
                    "third_conv":          _int(row.get("thirdDownConv") or row.get("thirdDownConversions")),
                    "fourth_att":          _int(row.get("fourthDownAtt") or row.get("fourthDownAttempts")),
                    "fourth_conv":         _int(row.get("fourthDownConv") or row.get("fourthDownConversions")),
                    "rz_att":              _int(row.get("redZoneAtt") or row.get("rzAtt")),
                    "rz_tds":              _int(row.get("redZoneTDs") or row.get("rzTDs") or row.get("rzTD")),
                    "rz_fgs":              _int(row.get("redZoneFGs") or row.get("rzFGs") or row.get("rzFG")),
                    "turnovers":           _int(row.get("turnovers") or row.get("turnoverTotal")),
                    "fumbles_lost":        _int(row.get("fumblesLost") or row.get("fumLost")),
                    "penalties":           _int(row.get("penalties")),
                    "penalty_yards":       _int(row.get("penaltyYards") or row.get("penYds")),
                    "top_seconds":         _int(row.get("topSeconds") or row.get("timeOfPossession")),
                    "def_sacks":           _int(row.get("defSacks") or row.get("sacksMade")),
                    "def_ints":            _int(row.get("defInts") or row.get("defInt")),
                    "def_forced_fumbles":  _int(row.get("defForcedFumbles") or row.get("defFF")),
                    "def_fumble_recoveries": _int(row.get("defFumRecoveries") or row.get("defFumRec")),
                    "def_total_tackles":   _int(row.get("defTotalTackles") or row.get("defTackles")),
                    "def_tfl":             _int(row.get("defTFL") or row.get("defTacklesForLoss")),
                    "def_safeties":        _int(row.get("defSafeties") or row.get("defSafety")),
                })
        count += 1

    db.flush()
    log.info("Team stats: loaded %d rows", count)
    return count


# ---------------------------------------------------------------------------
# Section: Schedule
# ---------------------------------------------------------------------------

def load_schedule(db: Session, payload: dict, season: Season, week: Week) -> int:
    rows = payload.get("scheduleInfoList", [])
    count = 0
    for row in rows:
        home_id = _get_team_id(db, _int(row.get("homeTeamId")))
        away_id = _get_team_id(db, _int(row.get("awayTeamId")))

        if home_id is None or away_id is None:
            continue

        # Find the week for this game's weekIndex (may differ from current week)
        game_week_index = _int(row.get("weekIndex", week.week_index))
        game_week = db.query(Week).filter_by(
            season_id=season.id, week_index=game_week_index
        ).first() or week

        is_done = bool(row.get("isCompleted") or row.get("isGameCompleted"))
        home_score = _int(row.get("homeScore")) if is_done else None
        away_score = _int(row.get("awayScore")) if is_done else None

        _upsert(db, ScheduleGame,
                {
                    "season_id":    season.id,
                    "week_id":      game_week.id,
                    "home_team_id": home_id,
                    "away_team_id": away_id,
                },
                {
                    "season_id":           season.id,
                    "week_id":             game_week.id,
                    "madden_schedule_id":  _int(row.get("scheduleId")),
                    "home_team_id":        home_id,
                    "away_team_id":        away_id,
                    "home_score":          home_score,
                    "away_score":          away_score,
                    "is_completed":        is_done,
                })
        count += 1

    db.flush()
    log.info("Schedule: loaded %d games", count)
    return count


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_loader(db: Session, payload: dict) -> dict:
    """
    Master loader — calls each section in order.
    Returns a summary dict of record counts.
    """
    summary = {}

    # 1. League / season / week (required — everything hangs off this)
    league, season, week = load_league_info(db, payload)
    summary["league"] = league.name
    summary["season_index"] = season.season_index
    summary["week_index"] = week.week_index

    # 2. Roster (optional)
    summary["roster"] = load_roster(db, payload, season, week)

    # 3. Player stats (optional — each section independent)
    summary["passing"] = load_passing_stats(db, payload, season, week)
    summary["rushing"] = load_rushing_stats(db, payload, season, week)
    summary["receiving"] = load_receiving_stats(db, payload, season, week)
    summary["defense"] = load_defense_stats(db, payload, season, week)
    summary["kicking"] = load_kicking_stats(db, payload, season, week)
    summary["punting"] = load_punting_stats(db, payload, season, week)

    # 4. Team stats (optional)
    summary["team_stats"] = load_team_stats(db, payload, season, week)

    # 5. Schedule (optional)
    summary["schedule"] = load_schedule(db, payload, season, week)

    db.commit()
    log.info("Loader complete: %s", summary)
    return summary
