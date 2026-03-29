import pandas as pd
import streamlit as st
from streamlit_app.shared import t, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default, render_team_sidebar


def render():
    render_team_sidebar()
    st.markdown(f'<p class="section-header">{t("hdr_referee_stats")}</p>', unsafe_allow_html=True)
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_referee_stats')}</p>", unsafe_allow_html=True)
    st.markdown("---")

    season_to_fetch = st.session_state.get("selected_season", _cfg_default)
    selected_team = st.session_state.get("selected_team")

    if not selected_team:
        st.warning(t("warn_select_team"))
        st.stop()

    min_ref_games = st.slider(
        t("lbl_min_ref_games"), min_value=1, max_value=10, value=3, key="min_ref_games",
    )

    try:
        with st.spinner(t("fetching_referee_stats")):
            from streamlit_app.queries import fetch_referee_stats
            ref_stats = fetch_referee_stats(season_to_fetch, selected_team, min_games=min_ref_games)
    except Exception as e:
        st.error(f"Could not load referee data. The API may be temporarily unavailable. Error: {type(e).__name__}")
        st.stop()

    if ref_stats.empty:
        st.info(t("no_referee_stats"))
    else:
        best_pct = ref_stats["win_pct"].max()
        worst_pct = ref_stats["win_pct"].min()

        c1, c2, c3 = st.columns(3)
        c1.metric(
            t("metric_total_refs"), len(ref_stats),
            help=t("tooltip_total_refs", n=min_ref_games),
        )
        c2.metric(
            t("metric_best_ref"), f"{best_pct:.1f}%",
            help=t("tooltip_best_ref"),
        )
        c3.metric(
            t("metric_worst_ref"), f"{worst_pct:.1f}%",
            help=t("tooltip_worst_ref"),
        )

        st.markdown("---")

        win_pct_label = t("col_win_pct")
        display_df = ref_stats.rename(columns={
            "referee": t("col_referee"), "games": t("col_games"),
            "wins": t("col_wins"), "losses": t("col_losses"), "win_pct": win_pct_label,
        })

        def highlight_win_pct(row):
            styles = [""] * len(row)
            if win_pct_label in row.index:
                idx = list(row.index).index(win_pct_label)
                if row.iloc[idx] == best_pct:
                    styles[idx] = "background-color: rgba(16,185,129,0.25); color: #10b981; font-weight: bold"
                elif row.iloc[idx] == worst_pct:
                    styles[idx] = "background-color: rgba(239,68,68,0.25); color: #ef4444; font-weight: bold"
            return styles

        styled = display_df.style.apply(highlight_win_pct, axis=1).format(precision=1)
        st.dataframe(styled, hide_index=True, height=450)
