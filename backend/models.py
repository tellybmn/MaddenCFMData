"""
SQLAlchemy ORM models for Madden CFM Data System.

Table hierarchy:
  leagues → seasons → weeks
  teams (static reference)
  players (static profile) → player_snapshots (weekly attribute/roster snapshots)
  player_*_stats (weekly stat lines, one row per player per week)
  team_stats (weekly, one row per team per week)
  season_player_*_stats / season_team_stats (running season totals)
  advanced_player_metrics / advanced_team_metrics (computed)
  schedule, standings
  export_log (audit trail)
"""

import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Text, Index,
)
from sqlalchemy.orm import relationship
from database import Base


# ---------------------------------------------------------------------------
# Core reference tables
# ---------------------------------------------------------------------------

class League(Base):
    __tablename__ = "leagues"

    id = Column(Integer, primary_key=True)
    league_id = Column(String, unique=True, nullable=False)   # Madden's leagueId
    name = Column(String)
    platform = Column(String)                                  # xbox / ps5 / pc / mixed
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    seasons = relationship("Season", back_populates="league", cascade="all, delete-orphan")


class Season(Base):
    __tablename__ = "seasons"

    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey("leagues.id"), nullable=False)
    season_index = Column(Integer, nullable=False)   # Madden's 0-based seasonIndex
    year = Column(Integer)                           # calendar year label (optional display)
    current_week = Column(Integer, default=1)
    phase = Column(String, default="preseason")      # preseason | regular | postseason

    __table_args__ = (UniqueConstraint("league_id", "season_index"),)

    league = relationship("League", back_populates="seasons")
    weeks = relationship("Week", back_populates="season", cascade="all, delete-orphan")


class Week(Base):
    __tablename__ = "weeks"

    id = Column(Integer, primary_key=True)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_index = Column(Integer, nullable=False)     # Madden's 0-based weekIndex
    week_number = Column(Integer)                    # human-readable label
    week_type = Column(String, default="regular")    # preseason | regular | postseason

    __table_args__ = (UniqueConstraint("season_id", "week_index"),)

    season = relationship("Season", back_populates="weeks")


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, unique=True, nullable=False)   # Madden's teamId
    city = Column(String)
    name = Column(String)
    abbreviation = Column(String)
    division = Column(String)
    conference = Column(String)
    logo_url = Column(String)                                 # optional, for UI


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

class Player(Base):
    """Immutable player identity. One row per real player, ever."""
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    roster_id = Column(Integer, unique=True, nullable=False)   # Madden's rosterId
    first_name = Column(String)
    last_name = Column(String)
    full_name = Column(String)
    position = Column(String)
    archetype = Column(String)
    college = Column(String)
    home_state = Column(String)
    jersey_number = Column(Integer)

    snapshots = relationship("PlayerSnapshot", back_populates="player", cascade="all, delete-orphan")

    __table_args__ = (Index("ix_players_position", "position"),)


class PlayerSnapshot(Base):
    """
    Player attributes and roster slot captured each time data is imported.
    Tracks development trait changes, rating upgrades, injuries, contract changes.
    One row per player per week.
    """
    __tablename__ = "player_snapshots"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))

    # Biographical
    age = Column(Integer)
    years_pro = Column(Integer)
    dev_trait = Column(Integer)    # 0=Normal 1=Impact 2=Star 3=XFactor

    # Overall + core ratings
    overall_rating = Column(Integer)
    speed = Column(Integer)
    acceleration = Column(Integer)
    agility = Column(Integer)
    strength = Column(Integer)
    awareness = Column(Integer)
    catching = Column(Integer)
    carrying = Column(Integer)
    jumping = Column(Integer)
    stamina = Column(Integer)
    injury = Column(Integer)
    toughness = Column(Integer)

    # Skill ratings
    throw_power = Column(Integer)
    throw_acc_short = Column(Integer)
    throw_acc_mid = Column(Integer)
    throw_acc_deep = Column(Integer)
    throw_on_run = Column(Integer)
    break_tackle = Column(Integer)
    break_sack = Column(Integer)
    pass_block = Column(Integer)
    pass_block_power = Column(Integer)
    pass_block_finesse = Column(Integer)
    run_block = Column(Integer)
    run_block_power = Column(Integer)
    run_block_finesse = Column(Integer)
    tackle = Column(Integer)
    hit_power = Column(Integer)
    pursuit = Column(Integer)
    play_recognition = Column(Integer)
    man_coverage = Column(Integer)
    zone_coverage = Column(Integer)
    press = Column(Integer)
    kick_power = Column(Integer)
    kick_accuracy = Column(Integer)
    kick_return = Column(Integer)
    spectacular_catch = Column(Integer)
    catch_in_traffic = Column(Integer)
    short_route_running = Column(Integer)
    medium_route_running = Column(Integer)
    deep_route_running = Column(Integer)
    release = Column(Integer)
    juke_move = Column(Integer)
    spin_move = Column(Integer)
    stiff_arm = Column(Integer)
    trucking = Column(Integer)
    change_of_direction = Column(Integer)

    # Status
    injury_status = Column(String)
    injury_length = Column(Integer)

    # Contract
    contract_salary = Column(Integer)
    contract_bonus = Column(Integer)
    contract_length = Column(Integer)
    contract_years_left = Column(Integer)

    # Roster position
    depth_position = Column(String)
    depth_order = Column(Integer)

    player = relationship("Player", back_populates="snapshots")

    __table_args__ = (UniqueConstraint("player_id", "week_id"),)


# ---------------------------------------------------------------------------
# Weekly player stat lines
# ---------------------------------------------------------------------------

class PlayerPassingStat(Base):
    __tablename__ = "player_passing_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    completions = Column(Integer, default=0)
    attempts = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    ints = Column(Integer, default=0)
    sacks = Column(Integer, default=0)
    sack_yards = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)
    passer_rating = Column(Float)           # EA's computed value (also recalculated by us)

    __table_args__ = (
        UniqueConstraint("player_id", "week_id"),
        Index("ix_pass_season", "season_id"),
    )


class PlayerRushingStat(Base):
    __tablename__ = "player_rushing_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    attempts = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    fumbles = Column(Integer, default=0)
    fumbles_lost = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)
    broken_tackles = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("player_id", "week_id"),
        Index("ix_rush_season", "season_id"),
    )


class PlayerReceivingStat(Base):
    __tablename__ = "player_receiving_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    targets = Column(Integer, default=0)
    receptions = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    drops = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("player_id", "week_id"),
        Index("ix_rec_season", "season_id"),
    )


class PlayerDefenseStat(Base):
    __tablename__ = "player_defense_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    tackles_solo = Column(Integer, default=0)
    tackles_assist = Column(Integer, default=0)
    tackles_total = Column(Integer, default=0)
    tackles_for_loss = Column(Float, default=0.0)
    sacks = Column(Float, default=0.0)
    sack_yards = Column(Integer, default=0)
    ints = Column(Integer, default=0)
    int_yards = Column(Integer, default=0)
    int_tds = Column(Integer, default=0)
    pass_breakups = Column(Integer, default=0)
    forced_fumbles = Column(Integer, default=0)
    fumble_recoveries = Column(Integer, default=0)
    safeties = Column(Integer, default=0)
    kick_blocks = Column(Integer, default=0)
    missed_tackles = Column(Integer, default=0)
    td_allowed = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("player_id", "week_id"),
        Index("ix_def_season", "season_id"),
    )


class PlayerKickingStat(Base):
    __tablename__ = "player_kicking_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    fg_att = Column(Integer, default=0)
    fg_made = Column(Integer, default=0)
    fg_att_19 = Column(Integer, default=0)
    fg_made_19 = Column(Integer, default=0)
    fg_att_29 = Column(Integer, default=0)
    fg_made_29 = Column(Integer, default=0)
    fg_att_39 = Column(Integer, default=0)
    fg_made_39 = Column(Integer, default=0)
    fg_att_49 = Column(Integer, default=0)
    fg_made_49 = Column(Integer, default=0)
    fg_att_50 = Column(Integer, default=0)
    fg_made_50 = Column(Integer, default=0)
    fg_long = Column(Integer, default=0)
    xp_att = Column(Integer, default=0)
    xp_made = Column(Integer, default=0)
    kickoffs = Column(Integer, default=0)
    kickoff_yards = Column(Integer, default=0)
    touchbacks = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "week_id"),)


class PlayerPuntingStat(Base):
    __tablename__ = "player_punting_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    punts = Column(Integer, default=0)
    gross_yards = Column(Integer, default=0)
    net_yards = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    touchbacks = Column(Integer, default=0)
    inside_20 = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "week_id"),)


# ---------------------------------------------------------------------------
# Weekly team stats
# ---------------------------------------------------------------------------

class TeamStat(Base):
    __tablename__ = "team_stats"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)

    # Offense
    points = Column(Integer, default=0)
    total_yards = Column(Integer, default=0)
    pass_yards = Column(Integer, default=0)
    rush_yards = Column(Integer, default=0)
    pass_att = Column(Integer, default=0)
    pass_cmp = Column(Integer, default=0)
    pass_tds = Column(Integer, default=0)
    pass_ints = Column(Integer, default=0)
    sacks_allowed = Column(Integer, default=0)
    sack_yards_allowed = Column(Integer, default=0)
    rush_att = Column(Integer, default=0)
    rush_tds = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)
    third_att = Column(Integer, default=0)
    third_conv = Column(Integer, default=0)
    fourth_att = Column(Integer, default=0)
    fourth_conv = Column(Integer, default=0)
    rz_att = Column(Integer, default=0)
    rz_tds = Column(Integer, default=0)
    rz_fgs = Column(Integer, default=0)
    turnovers = Column(Integer, default=0)
    fumbles_lost = Column(Integer, default=0)
    penalties = Column(Integer, default=0)
    penalty_yards = Column(Integer, default=0)
    top_seconds = Column(Integer, default=0)   # time of possession in seconds

    # Defense (from this team's defense)
    def_sacks = Column(Integer, default=0)
    def_ints = Column(Integer, default=0)
    def_forced_fumbles = Column(Integer, default=0)
    def_fumble_recoveries = Column(Integer, default=0)
    def_total_tackles = Column(Integer, default=0)
    def_tfl = Column(Integer, default=0)
    def_safeties = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("team_id", "week_id"),
        Index("ix_teamstat_season", "season_id"),
    )


# ---------------------------------------------------------------------------
# Season-to-date accumulated totals (recalculated on every import)
# ---------------------------------------------------------------------------

class SeasonPlayerPassingStat(Base):
    __tablename__ = "season_player_passing_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    completions = Column(Integer, default=0)
    attempts = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    ints = Column(Integer, default=0)
    sacks = Column(Integer, default=0)
    sack_yards = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonPlayerRushingStat(Base):
    __tablename__ = "season_player_rushing_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    attempts = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    fumbles = Column(Integer, default=0)
    fumbles_lost = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)
    broken_tackles = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonPlayerReceivingStat(Base):
    __tablename__ = "season_player_receiving_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    targets = Column(Integer, default=0)
    receptions = Column(Integer, default=0)
    yards = Column(Integer, default=0)
    tds = Column(Integer, default=0)
    drops = Column(Integer, default=0)
    yac = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonPlayerDefenseStat(Base):
    __tablename__ = "season_player_defense_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    tackles_solo = Column(Integer, default=0)
    tackles_assist = Column(Integer, default=0)
    tackles_total = Column(Integer, default=0)
    tackles_for_loss = Column(Float, default=0.0)
    sacks = Column(Float, default=0.0)
    sack_yards = Column(Integer, default=0)
    ints = Column(Integer, default=0)
    int_yards = Column(Integer, default=0)
    int_tds = Column(Integer, default=0)
    pass_breakups = Column(Integer, default=0)
    forced_fumbles = Column(Integer, default=0)
    fumble_recoveries = Column(Integer, default=0)
    safeties = Column(Integer, default=0)
    kick_blocks = Column(Integer, default=0)
    missed_tackles = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonPlayerKickingStat(Base):
    __tablename__ = "season_player_kicking_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    fg_att = Column(Integer, default=0)
    fg_made = Column(Integer, default=0)
    fg_att_19 = Column(Integer, default=0)
    fg_made_19 = Column(Integer, default=0)
    fg_att_29 = Column(Integer, default=0)
    fg_made_29 = Column(Integer, default=0)
    fg_att_39 = Column(Integer, default=0)
    fg_made_39 = Column(Integer, default=0)
    fg_att_49 = Column(Integer, default=0)
    fg_made_49 = Column(Integer, default=0)
    fg_att_50 = Column(Integer, default=0)
    fg_made_50 = Column(Integer, default=0)
    fg_long = Column(Integer, default=0)
    xp_att = Column(Integer, default=0)
    xp_made = Column(Integer, default=0)
    kickoffs = Column(Integer, default=0)
    kickoff_yards = Column(Integer, default=0)
    touchbacks = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonPlayerPuntingStat(Base):
    __tablename__ = "season_player_punting_stats"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    punts = Column(Integer, default=0)
    gross_yards = Column(Integer, default=0)
    net_yards = Column(Integer, default=0)
    longest = Column(Integer, default=0)
    touchbacks = Column(Integer, default=0)
    inside_20 = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("player_id", "season_id"),)


class SeasonTeamStat(Base):
    __tablename__ = "season_team_stats"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    games = Column(Integer, default=0)

    points = Column(Integer, default=0)
    points_allowed = Column(Integer, default=0)
    total_yards = Column(Integer, default=0)
    pass_yards = Column(Integer, default=0)
    rush_yards = Column(Integer, default=0)
    pass_att = Column(Integer, default=0)
    pass_cmp = Column(Integer, default=0)
    pass_tds = Column(Integer, default=0)
    pass_ints = Column(Integer, default=0)
    sacks_allowed = Column(Integer, default=0)
    sack_yards_allowed = Column(Integer, default=0)
    rush_att = Column(Integer, default=0)
    rush_tds = Column(Integer, default=0)
    first_downs = Column(Integer, default=0)
    third_att = Column(Integer, default=0)
    third_conv = Column(Integer, default=0)
    fourth_att = Column(Integer, default=0)
    fourth_conv = Column(Integer, default=0)
    rz_att = Column(Integer, default=0)
    rz_tds = Column(Integer, default=0)
    rz_fgs = Column(Integer, default=0)
    turnovers = Column(Integer, default=0)
    fumbles_lost = Column(Integer, default=0)
    penalties = Column(Integer, default=0)
    penalty_yards = Column(Integer, default=0)
    top_seconds = Column(Integer, default=0)
    def_sacks = Column(Integer, default=0)
    def_ints = Column(Integer, default=0)
    def_forced_fumbles = Column(Integer, default=0)
    def_fumble_recoveries = Column(Integer, default=0)
    def_total_tackles = Column(Integer, default=0)
    def_tfl = Column(Integer, default=0)
    def_safeties = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint("team_id", "season_id"),)


# ---------------------------------------------------------------------------
# Schedule & Standings
# ---------------------------------------------------------------------------

class ScheduleGame(Base):
    __tablename__ = "schedule"

    id = Column(Integer, primary_key=True)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)
    madden_schedule_id = Column(Integer)
    home_team_id = Column(Integer, ForeignKey("teams.id"))
    away_team_id = Column(Integer, ForeignKey("teams.id"))
    home_score = Column(Integer)
    away_score = Column(Integer)
    is_completed = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint("season_id", "week_id", "home_team_id", "away_team_id"),
    )


class Standing(Base):
    __tablename__ = "standings"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"), nullable=False)   # snapshot at this week

    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    ties = Column(Integer, default=0)
    division_wins = Column(Integer, default=0)
    division_losses = Column(Integer, default=0)
    conf_wins = Column(Integer, default=0)
    conf_losses = Column(Integer, default=0)
    home_wins = Column(Integer, default=0)
    home_losses = Column(Integer, default=0)
    away_wins = Column(Integer, default=0)
    away_losses = Column(Integer, default=0)
    points_for = Column(Integer, default=0)
    points_against = Column(Integer, default=0)
    streak = Column(String)                        # e.g. "W3" or "L2"

    __table_args__ = (UniqueConstraint("team_id", "season_id", "week_id"),)


# ---------------------------------------------------------------------------
# Advanced metrics (computed by metrics.py after each import)
# ---------------------------------------------------------------------------

class AdvancedPlayerMetric(Base):
    __tablename__ = "advanced_player_metrics"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"))
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"))    # NULL = season-level
    metric_scope = Column(String, nullable=False)        # "weekly" | "season"

    # --- Passing ---
    passer_rating = Column(Float)
    any_a = Column(Float)
    ny_a = Column(Float)
    y_a = Column(Float)
    completion_pct = Column(Float)
    td_pct = Column(Float)
    int_pct = Column(Float)
    sack_pct = Column(Float)
    air_yards_per_att = Column(Float)
    yac_per_cmp = Column(Float)
    pass_first_down_rate = Column(Float)

    # --- Rushing ---
    ypc = Column(Float)
    rush_td_rate = Column(Float)
    rush_first_down_rate = Column(Float)
    explosive_run_rate = Column(Float)     # carries >= 10 yds / total
    stuff_rate = Column(Float)             # carries <= 0 yds / total
    fumble_rate = Column(Float)            # fumbles / touches
    broken_tackle_rate = Column(Float)

    # --- Receiving ---
    y_tgt = Column(Float)
    y_rec = Column(Float)
    catch_rate = Column(Float)
    target_share = Column(Float)
    yac_per_rec = Column(Float)
    drop_rate = Column(Float)
    air_yards_per_tgt = Column(Float)
    rec_first_down_rate = Column(Float)
    rec_td_rate = Column(Float)

    # --- Defense ---
    sacks_per_game = Column(Float)
    tfl_per_game = Column(Float)
    pass_breakup_rate = Column(Float)      # pbу / coverage targets (approx)
    int_rate = Column(Float)
    forced_fumble_rate = Column(Float)
    missed_tackle_rate = Column(Float)
    coverage_yards_per_target = Column(Float)
    passer_rating_allowed = Column(Float)

    # --- Kicking ---
    fg_pct = Column(Float)
    fg_pct_30_39 = Column(Float)
    fg_pct_40_49 = Column(Float)
    fg_pct_50_plus = Column(Float)
    xp_pct = Column(Float)
    touchback_pct = Column(Float)
    kickoff_avg = Column(Float)

    # --- Punting ---
    gross_punt_avg = Column(Float)
    net_punt_avg = Column(Float)
    inside_20_rate = Column(Float)

    __table_args__ = (
        UniqueConstraint("player_id", "season_id", "week_id", "metric_scope"),
        Index("ix_apm_scope", "metric_scope", "season_id"),
    )


class AdvancedTeamMetric(Base):
    __tablename__ = "advanced_team_metrics"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=False)
    week_id = Column(Integer, ForeignKey("weeks.id"))    # NULL = season-level
    metric_scope = Column(String, nullable=False)        # "weekly" | "season"

    # --- Offense ---
    points_per_game = Column(Float)
    yards_per_play = Column(Float)
    pass_yards_per_game = Column(Float)
    rush_yards_per_game = Column(Float)
    points_per_drive = Column(Float)
    yards_per_drive = Column(Float)
    plays_per_drive = Column(Float)
    top_per_game_minutes = Column(Float)
    first_down_rate = Column(Float)
    completion_pct = Column(Float)
    passer_rating = Column(Float)
    rush_ypc = Column(Float)
    third_down_conv_rate = Column(Float)
    fourth_down_conv_rate = Column(Float)
    rz_td_rate = Column(Float)
    rz_scoring_rate = Column(Float)
    turnover_rate = Column(Float)
    fumble_rate = Column(Float)
    explosive_play_rate = Column(Float)    # plays >= 10 rush or >= 20 pass
    pass_ratio = Column(Float)
    scoring_drive_rate = Column(Float)
    penalty_rate = Column(Float)
    any_a = Column(Float)
    ny_a = Column(Float)

    # --- Defense ---
    points_allowed_per_game = Column(Float)
    yards_allowed_per_play = Column(Float)
    pass_yards_allowed_per_game = Column(Float)
    rush_yards_allowed_per_game = Column(Float)
    third_down_stop_rate = Column(Float)
    rz_stop_rate = Column(Float)
    sack_rate = Column(Float)
    opp_passer_rating = Column(Float)
    opp_ypc = Column(Float)
    turnover_forced_rate = Column(Float)
    takeaways_per_game = Column(Float)

    # --- Composite / SRS ---
    turnover_differential = Column(Float)
    point_differential = Column(Float)
    pythagorean_win_pct = Column(Float)
    srs = Column(Float)
    strength_of_schedule = Column(Float)

    __table_args__ = (
        UniqueConstraint("team_id", "season_id", "week_id", "metric_scope"),
        Index("ix_atm_scope", "metric_scope", "season_id"),
    )


# ---------------------------------------------------------------------------
# Export audit log
# ---------------------------------------------------------------------------

class ExportLog(Base):
    __tablename__ = "export_log"

    id = Column(Integer, primary_key=True)
    received_at = Column(DateTime, default=datetime.datetime.utcnow)
    season_index = Column(Integer)
    week_index = Column(Integer)
    file_hash = Column(String)
    raw_file_path = Column(String)
    status = Column(String)          # success | duplicate | error
    error_message = Column(Text)
    records_processed = Column(Integer, default=0)
