"""
playoff_probabilities.py -- Monte Carlo Playoff Probability Dashboard
======================================================================
Simulates the remainder of the Euroleague regular season 10,000 times
and displays each team's probability of reaching Top 10 (Play-In),
Top 6 (Direct Playoffs), Top 4 (Home Court), and #1 seed.
"""

import pandas as pd
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, TEAM_NAME_MAP,
    _cfg_default, render_page_header,
    skeleton_dataframe, skeleton_kpi_row,
    is_feature_enabled, show_disabled_message,
)


@st.cache_data(ttl=3600, show_spinner=False)
def _run_simulation(season: int) -> pd.DataFrame:
    """Run the Monte Carlo simulation (cached for 1 hour).

    Uses ``fetch_full_schedule`` (API-sourced) so that unplayed future
    games are included.  The DB-only ``fetch_season_schedule`` omits them
    because the ETL only loads played games.
    """
    from streamlit_app.queries import fetch_league_efficiency_landscape
    from data_pipeline.monte_carlo import fetch_full_schedule, simulate_season

    schedule = fetch_full_schedule(season)
    if schedule.empty:
        return pd.DataFrame()

    eff = fetch_league_efficiency_landscape(season)
    if eff.empty:
        return pd.DataFrame()

    if "net_rtg" not in eff.columns:
        eff["net_rtg"] = eff["ortg"] - eff["drtg"]

    net_rtg_map = dict(zip(eff["team_code"], eff["net_rtg"]))

    return simulate_season(schedule, net_rtg_map, runs=10_000)


def _compute_movers(sim_df: pd.DataFrame) -> pd.DataFrame:
    """Identify teams whose projected rank differs from current standing."""
    df = sim_df.copy()
    sorted_df = df.sort_values(
        ["current_wins", "avg_wins"], ascending=[False, False]
    ).reset_index(drop=True)
    sorted_df["current_rank"] = sorted_df.index + 1
    rank_map = dict(zip(sorted_df["team_code"], sorted_df["current_rank"]))
    df["current_rank"] = df["team_code"].map(rank_map)
    df["rank_diff"] = df["current_rank"] - df["proj_rank"]
    return df


def render():
    if not is_feature_enabled("ENABLE_ML_PREDICTIONS"):
        show_disabled_message("ENABLE_ML_PREDICTIONS")
        st.stop()

    render_page_header(
        t("hdr_playoff_picture", default="Playoff Picture"),
        t("sub_playoff_picture", default="Monte Carlo simulation of the remaining season (10,000 runs) to estimate playoff probabilities."),
        icon="🎯",
    )

    season = st.session_state.get("selected_season", _cfg_default)

    # Skeleton loaders while data loads
    insight_placeholder = st.empty()
    with insight_placeholder.container():
        skeleton_kpi_row(columns=3)

    table_placeholder = st.empty()
    with table_placeholder.container():
        skeleton_dataframe(rows=10, cols=7)

    try:
        with st.status(
            t("sim_running", default="Running 10,000 season simulations..."),
            expanded=True,
        ) as sim_status:
            st.write("Calculating win probabilities for remaining games...")
            sim_df = _run_simulation(season)
            sim_status.update(label="Simulation complete.", state="complete", expanded=False)
    except Exception as e:
        insight_placeholder.empty()
        table_placeholder.error(
            f"Could not run playoff simulation. Error: {type(e).__name__}"
        )
        return

    if sim_df.empty:
        insight_placeholder.empty()
        table_placeholder.warning(
            t("no_sim_data", default="No schedule or efficiency data available to simulate.")
        )
        return

    # Detect completed season (no games were simulated)
    season_complete = (
        "games_simulated" in sim_df.columns
        and int(sim_df["games_simulated"].iloc[0]) == 0
    )

    if season_complete:
        # --- Final Standings mode ---
        insight_placeholder.empty()
        st.info(
            t("season_complete_notice",
              default="The regular season is complete. Showing final standings."),
        )

        display_df = sim_df.copy()
        display_df["team_name"] = display_df["team_code"].map(
            lambda c: TEAM_NAME_MAP.get(c, c)
        )
        display_df["record"] = (
            display_df["current_wins"].astype(int).astype(str)
            + "-"
            + display_df["current_losses"].astype(int).astype(str)
        )
        display_df["result"] = display_df.apply(
            lambda r: "Play-In" if r["proj_rank"] <= 10
            else "Eliminated", axis=1,
        )
        display_df.loc[display_df["proj_rank"] <= 6, "result"] = "Playoffs"
        display_df.loc[display_df["proj_rank"] <= 4, "result"] = "Home Court"
        display_df.loc[display_df["proj_rank"] == 1, "result"] = "#1 Seed"

        show_df = display_df[["proj_rank", "team_name", "record", "result"]].copy()
        show_df.columns = ["#", "Team", "Record", "Result"]

        with table_placeholder.container():
            st.dataframe(show_df, hide_index=True, width="stretch")
        return

    # --- Active simulation mode ---
    movers = _compute_movers(sim_df)

    risers = movers[movers["rank_diff"] > 0].sort_values("rank_diff", ascending=False)
    fallers = movers[movers["rank_diff"] < 0].sort_values("rank_diff", ascending=True)

    with insight_placeholder.container():
        cols = st.columns(3)

        # Biggest Riser
        if not risers.empty:
            top_riser = risers.iloc[0]
            riser_name = TEAM_NAME_MAP.get(top_riser["team_code"], top_riser["team_code"])
            r_diff = int(top_riser["rank_diff"])
            r_current = int(top_riser["current_rank"])
            r_proj = int(top_riser["proj_rank"])
            with cols[0]:
                with st.container(border=True):
                    st.markdown(
                        f"<p style='color:#10b981; font-size:0.8rem; margin-bottom:2px; font-weight:600;'>"
                        f"BIGGEST RISER</p>"
                        f"<p style='font-size:1.1rem; font-weight:600; color:#f0f0ff; margin-bottom:4px;'>"
                        f"{riser_name}</p>"
                        f"<p style='color:#10b981; font-size:1.3rem; font-weight:700; margin-bottom:4px;'>"
                        f"+{r_diff} spots</p>"
                        f"<p style='color:#9ca3af; font-size:0.85rem;'>"
                        f"Currently {_ordinal(r_current)}, projected {_ordinal(r_proj)}</p>",
                        unsafe_allow_html=True,
                    )
        else:
            with cols[0]:
                with st.container(border=True):
                    st.markdown(
                        "<p style='color:#9ca3af; font-size:0.9rem;'>No significant risers detected.</p>",
                        unsafe_allow_html=True,
                    )

        # Biggest Faller
        if not fallers.empty:
            top_faller = fallers.iloc[0]
            faller_name = TEAM_NAME_MAP.get(top_faller["team_code"], top_faller["team_code"])
            f_diff = int(top_faller["rank_diff"])
            f_current = int(top_faller["current_rank"])
            f_proj = int(top_faller["proj_rank"])
            with cols[1]:
                with st.container(border=True):
                    st.markdown(
                        f"<p style='color:#ef4444; font-size:0.8rem; margin-bottom:2px; font-weight:600;'>"
                        f"BIGGEST FALLER</p>"
                        f"<p style='font-size:1.1rem; font-weight:600; color:#f0f0ff; margin-bottom:4px;'>"
                        f"{faller_name}</p>"
                        f"<p style='color:#ef4444; font-size:1.3rem; font-weight:700; margin-bottom:4px;'>"
                        f"{f_diff} spots</p>"
                        f"<p style='color:#9ca3af; font-size:0.85rem;'>"
                        f"Currently {_ordinal(f_current)}, projected {_ordinal(f_proj)}</p>",
                        unsafe_allow_html=True,
                    )
        else:
            with cols[1]:
                with st.container(border=True):
                    st.markdown(
                        "<p style='color:#9ca3af; font-size:0.9rem;'>No significant fallers detected.</p>",
                        unsafe_allow_html=True,
                    )

        # Tightest Race
        bubble_teams = movers[
            (movers["make_top_6_pct"] > 10) & (movers["make_top_6_pct"] < 90)
        ].sort_values("make_top_6_pct", ascending=False)
        with cols[2]:
            with st.container(border=True):
                if not bubble_teams.empty:
                    n_bubble = len(bubble_teams)
                    closest = bubble_teams.iloc[0]
                    closest_name = TEAM_NAME_MAP.get(closest["team_code"], closest["team_code"])
                    st.markdown(
                        f"<p style='color:#f59e0b; font-size:0.8rem; margin-bottom:2px; font-weight:600;'>"
                        f"BUBBLE WATCH</p>"
                        f"<p style='font-size:1.1rem; font-weight:600; color:#f0f0ff; margin-bottom:4px;'>"
                        f"{n_bubble} teams in the mix</p>"
                        f"<p style='color:#9ca3af; font-size:0.85rem;'>"
                        f"{closest_name} leads the bubble at {closest['make_top_6_pct']:.1f}% Top 6 chance</p>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        "<p style='color:#9ca3af; font-size:0.9rem;'>Playoff picture is largely settled.</p>",
                        unsafe_allow_html=True,
                    )

    # --- Main Probability Table ---
    n_simulated = int(sim_df["games_simulated"].iloc[0]) if "games_simulated" in sim_df.columns else "?"
    display_df = sim_df.copy()
    display_df["team_name"] = display_df["team_code"].map(
        lambda c: TEAM_NAME_MAP.get(c, c)
    )
    display_df["record"] = (
        display_df["current_wins"].astype(int).astype(str)
        + "-"
        + display_df["current_losses"].astype(int).astype(str)
    )

    n_teams = len(display_df)
    pos_cols = [c for c in display_df.columns if c.startswith("pos_") and c.endswith("_pct")]
    pos_cols = sorted(pos_cols, key=lambda c: int(c.split("_")[1]))

    # Summary table
    table_cols = [
        "proj_rank", "team_name", "record", "avg_wins",
        "make_top_10_pct", "make_top_6_pct", "make_top_4_pct", "win_rs_pct",
    ]
    show_df = display_df[table_cols].copy()
    show_df.columns = [
        "#", "Team", "Record", "Proj. Wins",
        "Top 10 %", "Top 6 %", "Top 4 %", "#1 Seed %",
    ]

    with table_placeholder.container():
        st.dataframe(
            show_df,
            hide_index=True,
            width="stretch",
            column_config={
                "#": st.column_config.NumberColumn(width="small"),
                "Team": st.column_config.TextColumn(width="medium"),
                "Record": st.column_config.TextColumn(width="small"),
                "Proj. Wins": st.column_config.NumberColumn(
                    format="%.1f", width="small",
                ),
                "Top 10 %": st.column_config.ProgressColumn(
                    min_value=0, max_value=100, format="%.1f%%",
                ),
                "Top 6 %": st.column_config.ProgressColumn(
                    min_value=0, max_value=100, format="%.1f%%",
                ),
                "Top 4 %": st.column_config.ProgressColumn(
                    min_value=0, max_value=100, format="%.1f%%",
                ),
                "#1 Seed %": st.column_config.ProgressColumn(
                    min_value=0, max_value=100, format="%.1f%%",
                ),
            },
        )

    # Full position-by-position breakdown with team-colored highlights
    if pos_cols:
        with st.expander(
            t("pos_breakdown", default="Position-by-Position Breakdown"),
            expanded=False,
        ):
            pos_df = display_df[["proj_rank", "team_code", "team_name"] + pos_cols].copy()
            pos_rename = {"proj_rank": "#", "team_name": "Team"}
            pos_labels = []
            for c in pos_cols:
                label = _ordinal(int(c.split("_")[1]))
                pos_rename[c] = label
                pos_labels.append(label)

            team_codes = pos_df["team_code"].tolist()
            pos_df = pos_df.drop(columns=["team_code"]).rename(columns=pos_rename)

            def _highlight_max_with_team_color(row):
                row_idx = row.name
                team_code = team_codes[row_idx] if row_idx < len(team_codes) else None
                primary = TEAM_COLORS.get(team_code, DEFAULT_ACCENT)[0] if team_code else DEFAULT_ACCENT[0]
                styles = [""] * len(row)
                numeric_vals = row[pos_labels]
                if numeric_vals.max() > 0:
                    max_col = numeric_vals.idxmax()
                    col_idx = list(row.index).index(max_col)
                    styles[col_idx] = (
                        f"background-color: {primary}33; "
                        f"color: {primary}; "
                        f"font-weight: bold"
                    )
                return styles

            styled = (
                pos_df.style
                .apply(_highlight_max_with_team_color, axis=1)
                .format(
                    {label: "{:.1f}%" for label in pos_labels},
                )
            )
            st.dataframe(styled, hide_index=True, height=750)

    st.caption(
        t("sim_caption_dynamic",
          default=f"Based on 10,000 Monte Carlo simulations of {n_simulated} remaining games. "
          "Team strength derived from current Net Rating. "
          "Home-court advantage of +2.5 Net Rating points is applied. "
          "Tiebreakers are randomized.")
    )


def _ordinal(n: int) -> str:
    """Convert an integer to its ordinal string (1st, 2nd, 3rd, etc.)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"
