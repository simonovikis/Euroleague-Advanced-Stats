import streamlit as st
from streamlit_app.shared import (
    t, is_feature_enabled,
    get_favorite_team, show_favorite_team_selector,
)

# Card definitions: (icon, title_key, desc_key, feature_flag_or_None)
# Icons match the st.Page icons in app.py navigation.
_CARDS = [
    ("🏆", "card_single_title", "card_single_desc", None),
    ("📊", "card_season_title", "card_season_desc", None),
    ("⚡", "card_advanced_title", "card_advanced_desc", None),
    ("📡", "card_live_title", "card_live_desc", "ENABLE_LIVE_MATCH"),
    ("🏅", "card_leaders_title", "card_leaders_desc", None),
    ("🔍", "card_scouting_title", "card_scouting_desc", "ENABLE_SCOUTING"),
    ("🧪", "card_lineup_title", "card_lineup_desc", "ENABLE_ML_PREDICTIONS"),
    ("👁️", "card_oracle_title", "card_oracle_desc", "ENABLE_ML_PREDICTIONS"),
    ("📋", "card_referee_title", "card_referee_desc", None),
    ("💬", "card_chat_title", "card_chat_desc", "ENABLE_LLM_CHAT"),
    ("📖", "card_glossary_title", "card_glossary_desc", None),
]


def render():
    if get_favorite_team() is None and not st.session_state.get("favorite_team_skipped"):
        show_favorite_team_selector()

    st.markdown("")
    st.markdown(
        f'<h1 style="text-align:center; color:#e4e4f0; margin-bottom:4px;">{t("home_welcome_title")}</h1>'
        f'<p style="text-align:center; color:#9ca3af; font-size:1.05rem; margin-bottom:2rem;">{t("home_welcome_sub")}</p>',
        unsafe_allow_html=True,
    )

    visible = [
        (icon, t(title_key), t(desc_key))
        for icon, title_key, desc_key, flag in _CARDS
        if flag is None or is_feature_enabled(flag)
    ]

    # Render in rows of 3
    for row_start in range(0, len(visible), 3):
        row_cards = visible[row_start : row_start + 3]
        cols = st.columns(3)
        for col, (icon, title, desc) in zip(cols, row_cards):
            with col:
                st.markdown(
                    f'<div class="landing-card">'
                    f'  <div class="card-icon">{icon}</div>'
                    f"  <h3>{title}</h3>"
                    f"  <p>{desc}</p>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
        st.markdown("")
