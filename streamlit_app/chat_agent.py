"""
chat_agent.py — LLM-powered Pandas Agent for Euroleague Stats
===============================================================
Builds a LangChain Pandas DataFrame agent that can answer natural
language questions about player and team season statistics.

Uses:
  - create_pandas_dataframe_agent from langchain_experimental
  - ChatOpenAI (GPT-4o-mini by default) via st.secrets["OPENAI_API_KEY"]
"""

import logging
from typing import Dict, List
import os
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

SYSTEM_PREFIX = """You are a Euroleague basketball analytics expert. You have access to
two Pandas DataFrames:

1. `df1` — **Player Season Advanced Stats**: One row per player with columns including
   player_name, team_code, team_name, games_played, minutes_pg (minutes per game),
   points_pg (points per game), ts_pct (True Shooting %), true_usg_pct (True Usage %),
   stop_rate, assist_ratio, ast_tov_ratio, orb_pct, drb_pct, three_pt_rate, ft_rate,
   steals_pg, blocks_pg, and more.

2. `df2` — **Team Season Advanced Stats**: One row per team with columns including
   team_code, team_name, games (games played), ortg (Offensive Rating per 100 poss),
   drtg (Defensive Rating per 100 poss), net_rtg (ORtg − DRtg), pace (possessions/game),
   and poss_off (total offensive possessions).

Key metric definitions:
  - AAQ (Adjusted Assist Quality): Average xP of shots created by a player's assists.
  - AxP (Assisted xPoints): Total expected points on assisted shots.
  - ORtg: Points produced per 100 possessions (higher = better offense).
  - DRtg: Points allowed per 100 possessions (lower = better defense).
  - tUSG%: Percentage of team plays used by a player while on court.
  - TS%: True Shooting % — holistic shooting efficiency including 2PT, 3PT, FT.
  - Net Rating: ORtg minus DRtg.

When answering:
  - Write correct Pandas code to query the DataFrames.
  - Always return clear, concise answers with the relevant numbers.
  - If the question is ambiguous, state your assumptions.
  - Format percentages nicely (e.g., 58.3% instead of 0.583).
  - If the data doesn't contain the information needed, say so clearly.
"""


def build_chat_agent(
    player_df: pd.DataFrame,
    team_df: pd.DataFrame,
    model_name: str = "gpt-4o-mini",
    temperature: float = 0.0,
):
    """
    Build a LangChain Pandas DataFrame agent with two DataFrames.

    Parameters
    ----------
    player_df : Player Season Advanced Stats DataFrame
    team_df   : Team Season Advanced Stats DataFrame
    model_name : OpenAI model to use
    temperature : LLM temperature (0 = deterministic)

    Returns
    -------
    agent : a runnable LangChain agent
    """
    from langchain_openai import ChatOpenAI
    from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent

    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in st.secrets")

    llm = ChatOpenAI(
        model=model_name,
        temperature=temperature,
        api_key=api_key,
    )

    agent = create_pandas_dataframe_agent(
        llm,
        [player_df, team_df],
        agent_type="tool-calling",
        prefix=SYSTEM_PREFIX,
        verbose=False,
        allow_dangerous_code=True,
        return_intermediate_steps=False,
    )

    return agent


def ask_agent(agent, question: str) -> str:
    """
    Send a question to the agent and return the answer string.
    Handles common errors gracefully.
    """
    try:
        response = agent.invoke({"input": question})
        return response.get("output", str(response))
    except Exception as e:
        logger.error(f"Agent error: {e}")
        error_msg = str(e)
        if "rate_limit" in error_msg.lower():
            return "Rate limit reached. Please wait a moment and try again."
        if "context_length" in error_msg.lower() or "token" in error_msg.lower():
            return "The query generated too much data for the model to process. Try a more specific question."
        return f"I couldn't process that question. Error: {error_msg}"
