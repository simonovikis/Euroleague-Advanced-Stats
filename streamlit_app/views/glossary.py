import streamlit as st
from streamlit_app.shared import t, render_page_header


def render():
    render_page_header(
        t("hdr_glossary", default="Glossary"),
        t("sub_glossary", default="Statistical definitions and formulas"),
        icon="📖",
    )

    st.markdown(f"""
### {t('gloss_ortg_title')}
{t('gloss_ortg_desc')}

### {t('gloss_drtg_title')}
{t('gloss_drtg_desc')}

### {t('gloss_net_title')}
{t('gloss_net_desc')}

### {t('gloss_ts_title')}
{t('gloss_ts_desc')}

### {t('gloss_tusg_title')}
{t('gloss_tusg_desc')}

### {t('gloss_stop_title')}
{t('gloss_stop_desc')}

### {t('gloss_ast_title')}
{t('gloss_ast_desc')}

### {t('gloss_tov_title')}
{t('gloss_tov_desc')}
""")

    st.info(f"{t('gloss_tip_title')} {t('gloss_tip_desc')}")
