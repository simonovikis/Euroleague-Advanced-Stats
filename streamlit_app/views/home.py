import streamlit as st
from streamlit_app.shared import t


def render():
    st.markdown("")
    st.markdown(
        f'<h1 style="text-align:center; color:#e4e4f0; margin-bottom:4px;">{t("home_welcome_title")}</h1>'
        f'<p style="text-align:center; color:#9ca3af; font-size:1.05rem; margin-bottom:2rem;">{t("home_welcome_sub")}</p>',
        unsafe_allow_html=True,
    )

    row1 = st.columns(3)
    cards_row1 = [
        ("🏀", t("card_single_title"), t("card_single_desc")),
        ("📊", t("card_season_title"), t("card_season_desc")),
        ("🧠", t("card_advanced_title"), t("card_advanced_desc")),
    ]
    for col, (icon, title, desc) in zip(row1, cards_row1):
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
    row2 = st.columns(3)
    cards_row2 = [
        ("📡", t("card_live_title"), t("card_live_desc")),
        ("🏅", t("card_leaders_title"), t("card_leaders_desc")),
        ("🔍", t("card_scouting_title"), t("card_scouting_desc")),
    ]
    for col, (icon, title, desc) in zip(row2, cards_row2):
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
    row3 = st.columns(4)
    cards_row3 = [
        ("⚖️", t("card_referee_title"), t("card_referee_desc")),
        ("💬", t("card_chat_title"), t("card_chat_desc")),
        ("📖", t("card_glossary_title"), t("card_glossary_desc")),
    ]
    for col, (icon, title, desc) in zip(row3, cards_row3):
        with col:
            st.markdown(
                f'<div class="landing-card">'
                f'  <div class="card-icon">{icon}</div>'
                f"  <h3>{title}</h3>"
                f"  <p>{desc}</p>"
                f"</div>",
                unsafe_allow_html=True,
            )
