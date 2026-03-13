"""
Advanced metrics calculations.

All functions are pure — they take raw stat values and return computed floats.
None of them touch the database directly; the aggregator calls these and
then writes the results.

Sections:
  1. Passing / QB
  2. Rushing
  3. Receiving
  4. Defense (individual)
  5. Kicking / Punting
  6. Team — offense
  7. Team — defense
  8. Team — composite / SRS
"""

from __future__ import annotations

import math
from typing import Optional


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Division that returns `default` instead of raising ZeroDivisionError."""
    if denominator == 0:
        return default
    return round(numerator / denominator, 4)


# =============================================================================
# 1. Passing / QB
# =============================================================================

def calc_passer_rating(cmp: int, att: int, yds: int, tds: int, ints: int) -> float:
    """
    NFL official passer rating formula.
    Each of the four components is clamped to [0, 2.375], then averaged and scaled.
    Max possible = 158.3
    """
    if att == 0:
        return 0.0
    a = max(0.0, min(((cmp / att) - 0.3) * 5, 2.375))
    b = max(0.0, min(((yds / att) - 3.0) * 0.25, 2.375))
    c = max(0.0, min((tds / att) * 20.0, 2.375))
    d = max(0.0, min(2.375 - ((ints / att) * 25.0), 2.375))
    return round(((a + b + c + d) / 6.0) * 100.0, 1)


def calc_any_a(yds: int, tds: int, ints: int, sack_yds: int,
               att: int, sacks: int) -> float:
    """
    Adjusted Net Yards per Attempt — the gold-standard passing efficiency metric.
    ANY/A = (Pass Yds + 20×TD - 45×INT - Sack Yds) / (Attempts + Sacks)
    """
    denom = att + sacks
    if denom == 0:
        return 0.0
    return round((yds + 20 * tds - 45 * ints - sack_yds) / denom, 2)


def calc_ny_a(yds: int, sack_yds: int, att: int, sacks: int) -> float:
    """Net Yards per Attempt — pass yards minus sack yards, over (attempts + sacks)."""
    denom = att + sacks
    return _safe_div(yds - sack_yds, denom)


def calc_y_a(yds: int, att: int) -> float:
    """Raw Yards per Attempt."""
    return _safe_div(yds, att)


def calc_completion_pct(cmp: int, att: int) -> float:
    return round(_safe_div(cmp, att) * 100, 1)


def calc_td_pct(tds: int, att: int) -> float:
    return round(_safe_div(tds, att) * 100, 2)


def calc_int_pct(ints: int, att: int) -> float:
    return round(_safe_div(ints, att) * 100, 2)


def calc_sack_pct(sacks: int, att: int, sacks2: Optional[int] = None) -> float:
    """Sack percentage = sacks / (attempts + sacks)."""
    total = att + (sacks2 if sacks2 is not None else sacks)
    return round(_safe_div(sacks, total) * 100, 2)


def calc_air_yards_per_att(yds: int, yac: int, att: int) -> float:
    """Air yards = pass yards minus yards after catch, divided by attempts."""
    air = max(0, yds - yac)
    return _safe_div(air, att)


def calc_yac_per_cmp(yac: int, cmp: int) -> float:
    return _safe_div(yac, cmp)


def calc_pass_first_down_rate(first_downs: int, att: int) -> float:
    return round(_safe_div(first_downs, att) * 100, 1)


# =============================================================================
# 2. Rushing
# =============================================================================

def calc_ypc(yards: int, attempts: int) -> float:
    """Yards per carry."""
    return _safe_div(yards, attempts)


def calc_rush_td_rate(tds: int, attempts: int) -> float:
    return round(_safe_div(tds, attempts) * 100, 2)


def calc_rush_first_down_rate(first_downs: int, attempts: int) -> float:
    return round(_safe_div(first_downs, attempts) * 100, 1)


def calc_fumble_rate(fumbles: int, touches: int) -> float:
    """Fumbles per touch (rush attempts + receptions combined — pass touches separately)."""
    return round(_safe_div(fumbles, touches) * 100, 2)


def calc_broken_tackle_rate(broken_tackles: int, attempts: int) -> float:
    return round(_safe_div(broken_tackles, attempts) * 100, 1)


def calc_explosive_run_rate(yards_list: list[int]) -> float:
    """
    Percent of carries that gain 10+ yards.
    Requires individual carry data — approximated from total if unavailable.
    When we only have totals we return None and skip this metric.
    """
    if not yards_list:
        return 0.0
    explosive = sum(1 for y in yards_list if y >= 10)
    return round(_safe_div(explosive, len(yards_list)) * 100, 1)


def calc_stuff_rate(yards_list: list[int]) -> float:
    """Percent of carries that gain 0 or fewer yards."""
    if not yards_list:
        return 0.0
    stuffed = sum(1 for y in yards_list if y <= 0)
    return round(_safe_div(stuffed, len(yards_list)) * 100, 1)


# =============================================================================
# 3. Receiving
# =============================================================================

def calc_y_tgt(yards: int, targets: int) -> float:
    return _safe_div(yards, targets)


def calc_y_rec(yards: int, receptions: int) -> float:
    return _safe_div(yards, receptions)


def calc_catch_rate(receptions: int, targets: int) -> float:
    return round(_safe_div(receptions, targets) * 100, 1)


def calc_drop_rate(drops: int, targets: int) -> float:
    return round(_safe_div(drops, targets) * 100, 1)


def calc_air_yards_per_tgt(yards: int, yac: int, targets: int) -> float:
    air = max(0, yards - yac)
    return _safe_div(air, targets)


def calc_yac_per_rec(yac: int, receptions: int) -> float:
    return _safe_div(yac, receptions)


def calc_rec_first_down_rate(first_downs: int, targets: int) -> float:
    return round(_safe_div(first_downs, targets) * 100, 1)


def calc_rec_td_rate(tds: int, targets: int) -> float:
    return round(_safe_div(tds, targets) * 100, 2)


def calc_target_share(player_targets: int, team_pass_attempts: int) -> float:
    """Player's share of total team pass attempts."""
    return round(_safe_div(player_targets, team_pass_attempts) * 100, 1)


# =============================================================================
# 4. Defense — Individual
# =============================================================================

def calc_missed_tackle_rate(missed: int, total_attempts: int) -> float:
    """missed_tackle_attempts = tackles + missed_tackles."""
    return round(_safe_div(missed, total_attempts) * 100, 1)


def calc_forced_fumble_rate(forced: int, tackles: int) -> float:
    return round(_safe_div(forced, tackles) * 100, 2)


def calc_pass_breakup_rate(pbу: int, coverage_targets: int) -> float:
    """PBU / estimated coverage targets."""
    return round(_safe_div(pbу, coverage_targets) * 100, 1)


def calc_int_rate(ints: int, coverage_targets: int) -> float:
    return round(_safe_div(ints, coverage_targets) * 100, 2)


def calc_coverage_yards_per_target(yards_allowed: int, coverage_targets: int) -> float:
    return _safe_div(yards_allowed, coverage_targets)


def calc_passer_rating_allowed(
    cmp: int, att: int, yds: int, tds: int, ints: int
) -> float:
    """Passer rating when targeted in coverage — same formula, defender perspective."""
    return calc_passer_rating(cmp, att, yds, tds, ints)


# =============================================================================
# 5. Kicking / Punting
# =============================================================================

def calc_fg_pct(made: int, att: int) -> float:
    return round(_safe_div(made, att) * 100, 1)


def calc_xp_pct(made: int, att: int) -> float:
    return round(_safe_div(made, att) * 100, 1)


def calc_touchback_pct(touchbacks: int, kickoffs: int) -> float:
    return round(_safe_div(touchbacks, kickoffs) * 100, 1)


def calc_kickoff_avg(yards: int, kickoffs: int) -> float:
    return _safe_div(yards, kickoffs)


def calc_gross_punt_avg(yards: int, punts: int) -> float:
    return _safe_div(yards, punts)


def calc_net_punt_avg(net_yards: int, punts: int) -> float:
    return _safe_div(net_yards, punts)


def calc_inside_20_rate(inside_20: int, punts: int) -> float:
    return round(_safe_div(inside_20, punts) * 100, 1)


# =============================================================================
# 6. Team — Offense
# =============================================================================

def calc_yards_per_play(total_yards: int, pass_att: int, rush_att: int,
                         sacks_allowed: int) -> float:
    """Total yards divided by total snaps (pass att + rush att + sacks)."""
    plays = pass_att + rush_att + sacks_allowed
    return _safe_div(total_yards, plays)


def calc_top_minutes(top_seconds: int) -> float:
    return round(top_seconds / 60, 1)


def calc_third_down_conv_rate(conv: int, att: int) -> float:
    return round(_safe_div(conv, att) * 100, 1)


def calc_fourth_down_conv_rate(conv: int, att: int) -> float:
    return round(_safe_div(conv, att) * 100, 1)


def calc_rz_td_rate(rz_tds: int, rz_att: int) -> float:
    """Red zone touchdown rate — TDs / trips."""
    return round(_safe_div(rz_tds, rz_att) * 100, 1)


def calc_rz_scoring_rate(rz_tds: int, rz_fgs: int, rz_att: int) -> float:
    """Red zone scoring rate — (TDs + FGs) / trips."""
    return round(_safe_div(rz_tds + rz_fgs, rz_att) * 100, 1)


def calc_turnover_rate(turnovers: int, drives: int) -> float:
    return round(_safe_div(turnovers, drives), 3)


def calc_explosive_play_rate_team(rush_att: int, pass_att: int, sacks: int,
                                   big_plays: int) -> float:
    """
    big_plays is the count of plays gaining 10+ rush or 20+ pass yards.
    When we only have game totals from Madden, we approximate big plays from
    longest rush/pass fields and a league-average distribution. If unavailable
    this stays NULL.
    """
    total_plays = rush_att + pass_att + sacks
    return round(_safe_div(big_plays, total_plays) * 100, 1)


def calc_pass_ratio(pass_att: int, rush_att: int) -> float:
    total = pass_att + rush_att
    return round(_safe_div(pass_att, total) * 100, 1)


def calc_penalty_rate(penalties: int, plays: int) -> float:
    return round(_safe_div(penalties, plays) * 100, 2)


def calc_scoring_drive_rate(scoring_drives: int, total_drives: int) -> float:
    return round(_safe_div(scoring_drives, total_drives) * 100, 1)


# =============================================================================
# 7. Team — Defense
# =============================================================================

def calc_sack_rate(sacks: int, opp_pass_att: int, opp_sacks: int) -> float:
    """Sacks / (opponent pass attempts + opponent sacks taken)."""
    denom = opp_pass_att + opp_sacks
    return round(_safe_div(sacks, denom) * 100, 2)


def calc_turnover_forced_rate(takeaways: int, opp_drives: int) -> float:
    return round(_safe_div(takeaways, opp_drives), 3)


# =============================================================================
# 8. Team — Composite / SRS
# =============================================================================

def calc_point_differential(pf: int, pa: int) -> float:
    return float(pf - pa)


def calc_pythagorean_win_pct(pf: int, pa: int, exp: float = 2.37) -> float:
    """
    Pythagorean Win Expectancy — how many wins a team "should" have based on
    scoring margin. Uses Daryl Morey's 2.37 exponent.
    """
    if pf + pa == 0:
        return 0.5
    return round((pf ** exp) / ((pf ** exp) + (pa ** exp)), 3)


def calc_srs(
    team_ids: list[int],
    pf_map: dict[int, int],
    pa_map: dict[int, int],
    games_map: dict[int, int],
    opponent_lists: dict[int, list[int]],
    n_iter: int = 100,
) -> dict[int, float]:
    """
    Simple Rating System (SRS) — iterative calculation.

    SRS[team] = MOV[team] + SOS[team]
    where SOS = average SRS of opponents played.

    Converges in ~50 iterations to a stable value.

    Returns a dict of {team_id: srs_value}.
    """
    # Seed with margin of victory
    srs: dict[int, float] = {}
    for tid in team_ids:
        games = max(games_map.get(tid, 1), 1)
        srs[tid] = (pf_map.get(tid, 0) - pa_map.get(tid, 0)) / games

    for _ in range(n_iter):
        new_srs: dict[int, float] = {}
        for tid in team_ids:
            games = max(games_map.get(tid, 1), 1)
            mov = (pf_map.get(tid, 0) - pa_map.get(tid, 0)) / games
            opps = opponent_lists.get(tid, [])
            sos = sum(srs.get(o, 0.0) for o in opps) / max(len(opps), 1)
            new_srs[tid] = mov + sos
        srs = new_srs

    return {tid: round(v, 2) for tid, v in srs.items()}


def calc_strength_of_schedule(
    team_id: int,
    opponent_win_pcts: list[float],
) -> float:
    """Average win percentage of all opponents faced."""
    if not opponent_win_pcts:
        return 0.0
    return round(sum(opponent_win_pcts) / len(opponent_win_pcts), 3)


# =============================================================================
# 9. Composite player metric builder
#    Called by aggregator.py with a dict of raw stats for one player.
# =============================================================================

def build_player_metrics(
    *,
    # identity
    position: str,
    team_pass_att: int = 0,

    # passing
    pass_cmp: int = 0,
    pass_att: int = 0,
    pass_yds: int = 0,
    pass_tds: int = 0,
    pass_ints: int = 0,
    pass_sacks: int = 0,
    pass_sack_yds: int = 0,
    pass_yac: int = 0,
    pass_first_downs: int = 0,

    # rushing
    rush_att: int = 0,
    rush_yds: int = 0,
    rush_tds: int = 0,
    rush_fumbles: int = 0,
    rush_first_downs: int = 0,
    rush_broken_tackles: int = 0,

    # receiving
    rec_targets: int = 0,
    rec_catches: int = 0,
    rec_yds: int = 0,
    rec_tds: int = 0,
    rec_drops: int = 0,
    rec_yac: int = 0,
    rec_first_downs: int = 0,

    # defense
    def_tackles: int = 0,
    def_missed_tackles: int = 0,
    def_sacks: float = 0.0,
    def_tfl: float = 0.0,
    def_forced_fumbles: int = 0,
    def_ints: int = 0,
    def_pbу: int = 0,
    def_cov_targets: int = 0,
    def_cov_yds: int = 0,
    def_cov_cmp: int = 0,
    def_cov_td: int = 0,
    def_cov_int: int = 0,
    games: int = 1,

    # kicking
    kick_fg_att: int = 0,
    kick_fg_made: int = 0,
    kick_fg_att_30: int = 0,
    kick_fg_made_30: int = 0,
    kick_fg_att_40: int = 0,
    kick_fg_made_40: int = 0,
    kick_fg_att_50: int = 0,
    kick_fg_made_50: int = 0,
    kick_xp_att: int = 0,
    kick_xp_made: int = 0,
    kick_kickoffs: int = 0,
    kick_kickoff_yds: int = 0,
    kick_touchbacks: int = 0,

    # punting
    punt_count: int = 0,
    punt_gross_yds: int = 0,
    punt_net_yds: int = 0,
    punt_inside_20: int = 0,
) -> dict:
    """
    Compute all applicable advanced metrics for a single player.
    Returns a flat dict matching AdvancedPlayerMetric columns.
    Metrics that don't apply (zero denominators) return None.
    """
    m: dict = {}
    touches = rush_att + rec_catches

    # --- Passing ---
    if pass_att > 0:
        m["passer_rating"] = calc_passer_rating(pass_cmp, pass_att, pass_yds, pass_tds, pass_ints)
        m["any_a"] = calc_any_a(pass_yds, pass_tds, pass_ints, pass_sack_yds, pass_att, pass_sacks)
        m["ny_a"] = calc_ny_a(pass_yds, pass_sack_yds, pass_att, pass_sacks)
        m["y_a"] = calc_y_a(pass_yds, pass_att)
        m["completion_pct"] = calc_completion_pct(pass_cmp, pass_att)
        m["td_pct"] = calc_td_pct(pass_tds, pass_att)
        m["int_pct"] = calc_int_pct(pass_ints, pass_att)
        m["sack_pct"] = calc_sack_pct(pass_sacks, pass_att)
        m["air_yards_per_att"] = calc_air_yards_per_att(pass_yds, pass_yac, pass_att)
        m["yac_per_cmp"] = calc_yac_per_cmp(pass_yac, pass_cmp)
        m["pass_first_down_rate"] = calc_pass_first_down_rate(pass_first_downs, pass_att)

    # --- Rushing ---
    if rush_att > 0:
        m["ypc"] = calc_ypc(rush_yds, rush_att)
        m["rush_td_rate"] = calc_rush_td_rate(rush_tds, rush_att)
        m["rush_first_down_rate"] = calc_rush_first_down_rate(rush_first_downs, rush_att)
        m["broken_tackle_rate"] = calc_broken_tackle_rate(rush_broken_tackles, rush_att)

    if touches > 0:
        m["fumble_rate"] = calc_fumble_rate(rush_fumbles, touches)

    # --- Receiving ---
    if rec_targets > 0:
        m["y_tgt"] = calc_y_tgt(rec_yds, rec_targets)
        m["catch_rate"] = calc_catch_rate(rec_catches, rec_targets)
        m["drop_rate"] = calc_drop_rate(rec_drops, rec_targets)
        m["air_yards_per_tgt"] = calc_air_yards_per_tgt(rec_yds, rec_yac, rec_targets)
        m["rec_first_down_rate"] = calc_rec_first_down_rate(rec_first_downs, rec_targets)
        m["rec_td_rate"] = calc_rec_td_rate(rec_tds, rec_targets)
        if team_pass_att > 0:
            m["target_share"] = calc_target_share(rec_targets, team_pass_att)
    if rec_catches > 0:
        m["y_rec"] = calc_y_rec(rec_yds, rec_catches)
        m["yac_per_rec"] = calc_yac_per_rec(rec_yac, rec_catches)

    # --- Defense ---
    tackle_attempts = def_tackles + def_missed_tackles
    if tackle_attempts > 0:
        m["missed_tackle_rate"] = calc_missed_tackle_rate(def_missed_tackles, tackle_attempts)
    if def_tackles > 0:
        m["forced_fumble_rate"] = calc_forced_fumble_rate(def_forced_fumbles, def_tackles)
    if def_cov_targets > 0:
        m["pass_breakup_rate"] = calc_pass_breakup_rate(def_pbу, def_cov_targets)
        m["int_rate"] = calc_int_rate(def_ints, def_cov_targets)
        m["coverage_yards_per_target"] = calc_coverage_yards_per_target(def_cov_yds, def_cov_targets)
        m["passer_rating_allowed"] = calc_passer_rating_allowed(
            def_cov_cmp, def_cov_targets, def_cov_yds, def_cov_td, def_cov_int
        )
    if games > 0:
        m["sacks_per_game"] = round(def_sacks / games, 2)
        m["tfl_per_game"] = round(def_tfl / games, 2)

    # --- Kicking ---
    if kick_fg_att > 0:
        m["fg_pct"] = calc_fg_pct(kick_fg_made, kick_fg_att)
    if kick_fg_att_30 > 0:
        m["fg_pct_30_39"] = calc_fg_pct(kick_fg_made_30, kick_fg_att_30)
    if kick_fg_att_40 > 0:
        m["fg_pct_40_49"] = calc_fg_pct(kick_fg_made_40, kick_fg_att_40)
    if kick_fg_att_50 > 0:
        m["fg_pct_50_plus"] = calc_fg_pct(kick_fg_made_50, kick_fg_att_50)
    if kick_xp_att > 0:
        m["xp_pct"] = calc_xp_pct(kick_xp_made, kick_xp_att)
    if kick_kickoffs > 0:
        m["touchback_pct"] = calc_touchback_pct(kick_touchbacks, kick_kickoffs)
        m["kickoff_avg"] = calc_kickoff_avg(kick_kickoff_yds, kick_kickoffs)

    # --- Punting ---
    if punt_count > 0:
        m["gross_punt_avg"] = calc_gross_punt_avg(punt_gross_yds, punt_count)
        m["net_punt_avg"] = calc_net_punt_avg(punt_net_yds, punt_count)
        m["inside_20_rate"] = calc_inside_20_rate(punt_inside_20, punt_count)

    return m


def build_team_metrics(
    *,
    games: int = 1,
    # offense
    points: int = 0,
    points_allowed: int = 0,
    total_yards: int = 0,
    pass_yards: int = 0,
    rush_yards: int = 0,
    pass_att: int = 0,
    pass_cmp: int = 0,
    pass_tds: int = 0,
    pass_ints: int = 0,
    sacks_allowed: int = 0,
    sack_yards_allowed: int = 0,
    rush_att: int = 0,
    first_downs: int = 0,
    third_att: int = 0,
    third_conv: int = 0,
    fourth_att: int = 0,
    fourth_conv: int = 0,
    rz_att: int = 0,
    rz_tds: int = 0,
    rz_fgs: int = 0,
    turnovers: int = 0,
    fumbles_lost: int = 0,
    penalties: int = 0,
    penalty_yards: int = 0,
    top_seconds: int = 0,
    # defense
    def_sacks: int = 0,
    def_ints: int = 0,
    def_forced_fumbles: int = 0,
    def_fumble_recoveries: int = 0,
    # opponent stats (from schedule + opponent team_stats)
    opp_pass_att: int = 0,
    opp_sacks: int = 0,
    opp_total_yards: int = 0,
    opp_pass_yards: int = 0,
    opp_rush_yards: int = 0,
    opp_rush_att: int = 0,
    opp_third_att: int = 0,
    opp_third_conv: int = 0,
    opp_rz_att: int = 0,
    opp_rz_tds: int = 0,
    opp_rz_fgs: int = 0,
    opp_pass_cmp: int = 0,
    opp_pass_tds: int = 0,
    opp_pass_ints: int = 0,
    opp_sack_yds: int = 0,
    opp_rush_tds: int = 0,
    # derived
    scoring_drives: int = 0,
    total_drives: int = 0,
    opp_total_plays: int = 0,
    opp_drives: int = 0,
    takeaways: int = 0,
    # SRS inputs (computed separately, injected here for storage)
    srs: float = 0.0,
    sos: float = 0.0,
) -> dict:
    """Compute all advanced team metrics. Returns a flat dict."""
    m: dict = {}
    g = max(games, 1)
    total_plays = pass_att + rush_att + sacks_allowed
    opp_plays = opp_total_plays if opp_total_plays > 0 else (opp_pass_att + opp_rush_att + opp_sacks)

    # --- Offense ---
    m["points_per_game"] = round(points / g, 2)
    m["yards_per_play"] = calc_yards_per_play(total_yards, pass_att, rush_att, sacks_allowed)
    m["pass_yards_per_game"] = round(pass_yards / g, 1)
    m["rush_yards_per_game"] = round(rush_yards / g, 1)
    m["top_per_game_minutes"] = calc_top_minutes(top_seconds // g) if g > 0 else 0.0
    m["completion_pct"] = calc_completion_pct(pass_cmp, pass_att)
    m["passer_rating"] = calc_passer_rating(pass_cmp, pass_att, pass_yards, pass_tds, pass_ints)
    m["any_a"] = calc_any_a(pass_yards, pass_tds, pass_ints, sack_yards_allowed, pass_att, sacks_allowed)
    m["ny_a"] = calc_ny_a(pass_yards, sack_yards_allowed, pass_att, sacks_allowed)
    m["rush_ypc"] = calc_ypc(rush_yards, rush_att)
    m["first_down_rate"] = round(_safe_div(first_downs, total_plays) * 100, 1)
    m["third_down_conv_rate"] = calc_third_down_conv_rate(third_conv, third_att)
    m["fourth_down_conv_rate"] = calc_fourth_down_conv_rate(fourth_conv, fourth_att)
    m["rz_td_rate"] = calc_rz_td_rate(rz_tds, rz_att)
    m["rz_scoring_rate"] = calc_rz_scoring_rate(rz_tds, rz_fgs, rz_att)
    m["turnover_rate"] = calc_turnover_rate(turnovers, max(total_drives, 1))
    m["pass_ratio"] = calc_pass_ratio(pass_att, rush_att)
    m["penalty_rate"] = calc_penalty_rate(penalties, max(total_plays, 1))
    if total_drives > 0:
        m["points_per_drive"] = round(points / total_drives, 2)
        m["yards_per_drive"] = round(total_yards / total_drives, 1)
        m["plays_per_drive"] = round(total_plays / total_drives, 1)
        m["scoring_drive_rate"] = calc_scoring_drive_rate(scoring_drives, total_drives)

    # --- Defense ---
    m["points_allowed_per_game"] = round(points_allowed / g, 2)
    m["third_down_stop_rate"] = round(100 - calc_third_down_conv_rate(opp_third_conv, opp_third_att), 1) if opp_third_att > 0 else None
    m["rz_stop_rate"] = round(100 - calc_rz_scoring_rate(opp_rz_tds, opp_rz_fgs, opp_rz_att), 1) if opp_rz_att > 0 else None
    m["sack_rate"] = calc_sack_rate(def_sacks, opp_pass_att, opp_sacks)
    m["opp_passer_rating"] = calc_passer_rating(opp_pass_cmp, opp_pass_att, opp_pass_yards, opp_pass_tds, opp_pass_ints)
    m["opp_ypc"] = calc_ypc(opp_rush_yards, opp_rush_att)
    if opp_plays > 0:
        m["yards_allowed_per_play"] = _safe_div(opp_total_yards, opp_plays)
        m["pass_yards_allowed_per_game"] = round(opp_pass_yards / g, 1)
        m["rush_yards_allowed_per_game"] = round(opp_rush_yards / g, 1)
    m["takeaways_per_game"] = round(takeaways / g, 2)
    if opp_drives > 0:
        m["turnover_forced_rate"] = calc_turnover_forced_rate(takeaways, opp_drives)

    # --- Composite ---
    m["turnover_differential"] = float((def_ints + def_fumble_recoveries) - turnovers)
    m["point_differential"] = calc_point_differential(points, points_allowed)
    m["pythagorean_win_pct"] = calc_pythagorean_win_pct(points, points_allowed)
    m["srs"] = srs
    m["strength_of_schedule"] = sos

    return m
