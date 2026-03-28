import hashlib
import streamlit as st
from streamlit_app.shared import t, CFG, _cfg_default, is_feature_enabled, show_disabled_message, OPENAI_API_KEY


def render():
    if not is_feature_enabled("ENABLE_LLM_CHAT"):
        show_disabled_message("ENABLE_LLM_CHAT")
        st.stop()

    st.markdown(f'<p class="section-header">{t("chat_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('chat_subtitle')}</p>",
        unsafe_allow_html=True,
    )

    # --- API Key Validation ---
    def _validate_openai_key(api_key: str) -> bool:
        """Validate an OpenAI API key by making a lightweight models.list() call."""
        try:
            from openai import OpenAI, AuthenticationError
            client = OpenAI(api_key=api_key)
            client.models.list()
            return True
        except AuthenticationError:
            return False
        except Exception:
            return False

    # --- Key Retrieval Priority: .env → st.secrets → user input ---
    if "openai_api_key" not in st.session_state:
        if OPENAI_API_KEY:
            st.session_state["openai_api_key"] = OPENAI_API_KEY

    # --- BYOK Fallback ---
    if not st.session_state.get("openai_api_key"):
        st.info("🔑 No OpenAI API key found in the environment. Please provide your own key to enable the chatbot.")
        user_key = st.text_input(
            "Enter your OpenAI API Key to enable the Chatbot.",
            type="password",
            key="byok_input",
        )
        if user_key:
            with st.spinner("Validating API key..."):
                if _validate_openai_key(user_key):
                    st.session_state["openai_api_key"] = user_key
                    st.success("API Key verified! You can now start asking questions.")
                    st.rerun()
                else:
                    st.error("Invalid API Key. Please check your key and try again.")
        st.stop()

    # --- Load data for the agent ---
    season_chat = st.session_state.get("selected_season", _cfg_default)

    @st.cache_data(ttl=CFG["data"]["cache_ttl_seconds"], show_spinner=False)
    def _load_chat_dataframes(season: int):
        from streamlit_app.queries import fetch_scouting_player_pool, fetch_league_efficiency_landscape
        player_df = fetch_scouting_player_pool(season)
        team_df = fetch_league_efficiency_landscape(season)
        return player_df, team_df

    try:
        with st.spinner(t("chat_loading_data")):
            player_df, team_df = _load_chat_dataframes(season_chat)
    except Exception as e:
        st.error(f"Could not load data for the chat agent. Error: {type(e).__name__}")
        st.stop()

    if player_df.empty and team_df.empty:
        st.warning(t("chat_no_data"))
        st.stop()

    # --- Build agent (cached in session state, keyed by season + key hash) ---
    _key_hash = hashlib.sha256(st.session_state["openai_api_key"].encode()).hexdigest()[:8]
    agent_cache_key = f"chat_agent_{season_chat}_{_key_hash}"
    if agent_cache_key not in st.session_state:
        try:
            from streamlit_app.chat_agent import build_chat_agent
            st.session_state[agent_cache_key] = build_chat_agent(
                player_df, team_df, api_key=st.session_state["openai_api_key"],
            )
        except Exception as e:
            st.error(f"{t('chat_agent_error')}: {e}")
            st.stop()

    agent = st.session_state[agent_cache_key]

    # --- Initialize chat history ---
    if "chat_messages" not in st.session_state:
        st.session_state["chat_messages"] = []

    # --- Display existing messages ---
    for msg in st.session_state["chat_messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # --- Chat input ---
    if prompt := st.chat_input(t("chat_placeholder")):
        st.session_state["chat_messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner(t("chat_thinking")):
                from streamlit_app.chat_agent import ask_agent
                answer = ask_agent(agent, prompt)
            st.markdown(answer)

        st.session_state["chat_messages"].append({"role": "assistant", "content": answer})

    # --- Suggested questions ---
    if not st.session_state["chat_messages"]:
        st.markdown(f"#### {t('chat_suggestions_title')}")
        suggestions = [
            t("chat_suggestion_1"),
            t("chat_suggestion_2"),
            t("chat_suggestion_3"),
            t("chat_suggestion_4"),
        ]
        cols = st.columns(2)
        for i, suggestion in enumerate(suggestions):
            with cols[i % 2]:
                st.markdown(
                    f'<div style="background:linear-gradient(135deg,#1e1e3f,#2a2a5a); '
                    f'border:1px solid rgba(255,255,255,0.08); border-radius:12px; '
                    f'padding:16px; margin-bottom:12px; color:#9ca3af; font-size:0.9rem;">'
                    f'💡 {suggestion}</div>',
                    unsafe_allow_html=True,
                )
