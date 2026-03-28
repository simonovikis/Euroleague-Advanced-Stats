"""
pdf_report.py — Automated PDF Scouting Report Generator
=========================================================
Generates a one-page scouting report PDF for a selected player using fpdf2.
Returns the PDF as a BytesIO buffer for Streamlit download.
"""

import io
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from fpdf import FPDF


_CLR_BG = (15, 15, 35)
_CLR_HEADER = (99, 102, 241)
_CLR_WHITE = (228, 228, 240)
_CLR_MUTED = (156, 163, 175)
_CLR_TABLE_HDR = (42, 42, 90)
_CLR_TABLE_ROW1 = (30, 30, 63)
_CLR_TABLE_ROW2 = (24, 24, 50)
_CLR_ACCENT = (245, 158, 11)


class ScoutingReportPDF(FPDF):
    def header(self):
        self.set_fill_color(*_CLR_BG)
        self.rect(0, 0, 210, 297, "F")

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*_CLR_MUTED)
        self.cell(
            0, 10,
            f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Euroleague Advanced Analytics Platform",
            align="C",
        )

    def _section_title(self, title: str):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*_CLR_HEADER)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*_CLR_HEADER)
        self.line(self.l_margin, self.get_y(), 200, self.get_y())
        self.ln(3)

    def _key_value(self, label: str, value: str, bold_value: bool = False):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*_CLR_MUTED)
        self.cell(38, 5, label, new_x="END")
        self.set_font("Helvetica", "B" if bold_value else "", 9)
        self.set_text_color(*_CLR_WHITE)
        self.cell(0, 5, value, new_x="LMARGIN", new_y="NEXT")

    def _table(self, headers: List[str], rows: List[List[str]], col_widths: Optional[List[float]] = None):
        if col_widths is None:
            usable = 190
            col_widths = [usable / len(headers)] * len(headers)

        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(*_CLR_TABLE_HDR)
        self.set_text_color(*_CLR_WHITE)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 6, h, border=0, fill=True, align="C")
        self.ln()

        self.set_font("Helvetica", "", 8)
        for ri, row in enumerate(rows):
            fill_clr = _CLR_TABLE_ROW1 if ri % 2 == 0 else _CLR_TABLE_ROW2
            self.set_fill_color(*fill_clr)
            self.set_text_color(*_CLR_WHITE)
            for i, val in enumerate(row):
                self.cell(col_widths[i], 5.5, str(val), border=0, fill=True, align="C")
            self.ln()


def generate_player_report(
    player_name: str,
    season: int,
    player_pool: pd.DataFrame,
    similar_df: pd.DataFrame,
    recent_form_df: Optional[pd.DataFrame] = None,
) -> io.BytesIO:
    """
    Generate a one-page PDF scouting report for a player.

    Parameters
    ----------
    player_name : str
    season : int
    player_pool : pd.DataFrame  — full scouting player pool
    similar_df : pd.DataFrame   — pre-computed similar players (from find_similar_players)
    recent_form_df : pd.DataFrame or None — per-game stats for the player (last N games)

    Returns
    -------
    io.BytesIO — PDF bytes ready for st.download_button
    """
    target_row = player_pool[player_pool["player_name"] == player_name]
    if target_row.empty:
        raise ValueError(f"Player '{player_name}' not found in pool")
    tr = target_row.iloc[0]

    pdf = ScoutingReportPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # ── Title ──
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*_CLR_HEADER)
    pdf.cell(0, 10, "SCOUTING REPORT", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*_CLR_MUTED)
    pdf.cell(
        0, 6,
        f"Euroleague Season {season} | {datetime.now().strftime('%B %d, %Y')}",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )
    pdf.ln(6)

    # ── Player Bio ──
    pdf._section_title("PLAYER BIO")
    pdf._key_value("Name:", player_name, bold_value=True)
    pdf._key_value("Team:", str(tr.get("team_name", tr.get("team_code", "N/A"))))
    pdf._key_value("Position:", str(tr.get("position", "N/A")))
    pdf._key_value("Games Played:", str(int(tr.get("games_played", 0))))
    pdf._key_value("Minutes/Game:", f"{tr.get('minutes_pg', 0):.1f}")
    pdf._key_value("Points/Game:", f"{tr.get('points_pg', 0):.1f}")
    pdf.ln(4)

    # ── Core Advanced Stats ──
    pdf._section_title("CORE ADVANCED STATS")

    stats_headers = ["TS%", "tUSG%", "Stop Rate", "AST Ratio", "AST/TOV", "3PA Rate", "FT Rate"]
    stats_row = [
        f"{tr.get('ts_pct', 0):.1%}",
        f"{tr.get('true_usg_pct', 0):.1%}",
        f"{tr.get('stop_rate', 0):.1%}",
        f"{tr.get('assist_ratio', 0):.1%}",
        f"{tr.get('ast_tov_ratio', 0):.2f}",
        f"{tr.get('three_pt_rate', 0):.1%}",
        f"{tr.get('ft_rate', 0):.1%}",
    ]
    col_w = [190 / 7] * 7
    pdf._table(stats_headers, [stats_row], col_widths=col_w)

    # Secondary line: rebounds, assists, steals, blocks
    pdf.ln(2)
    sec_headers = ["REB/G", "AST/G", "STL/G", "BLK/G", "ORB%", "DRB%"]
    sec_row = [
        f"{tr.get('rebounds_pg', 0):.1f}",
        f"{tr.get('assists_pg', 0):.1f}",
        f"{tr.get('steals_pg', 0):.2f}",
        f"{tr.get('blocks_pg', 0):.2f}",
        f"{tr.get('orb_pct', 0):.1%}",
        f"{tr.get('drb_pct', 0):.1%}",
    ]
    sec_w = [190 / 6] * 6
    pdf._table(sec_headers, [sec_row], col_widths=sec_w)
    pdf.ln(4)

    # ── Player Similarity ──
    pdf._section_title("PLAYER SIMILARITY - Top 3 Matches")

    if not similar_df.empty:
        sim_top = similar_df.head(3)
        sim_headers = ["Rank", "Player", "Team", "Similarity", "PPG", "TS%", "tUSG%"]
        sim_rows = []
        for i, (_, row) in enumerate(sim_top.iterrows(), 1):
            sim_rows.append([
                f"#{i}",
                str(row.get("player_name", "")),
                str(row.get("team_code", "")),
                f"{row.get('similarity_pct', 0):.1f}%",
                f"{row.get('points_pg', 0):.1f}",
                f"{row.get('ts_pct', 0):.1%}",
                f"{row.get('true_usg_pct', 0):.1%}",
            ])
        sim_w = [15, 50, 25, 25, 20, 27.5, 27.5]
        pdf._table(sim_headers, sim_rows, col_widths=sim_w)
    else:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*_CLR_MUTED)
        pdf.cell(0, 6, "No similarity data available.", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ── Recent Form (Last 5 Games) ──
    pdf._section_title("RECENT FORM - Last 5 Games")

    if recent_form_df is not None and not recent_form_df.empty:
        form = recent_form_df.tail(5).copy()
        form_headers = ["Game", "MIN", "PTS", "TPC", "TS%", "ORtg", "DRtg", "Net"]
        form_rows = []
        for gi, (_, row) in enumerate(form.iterrows(), 1):
            off_rtg = row.get("off_rating", 0) or 0
            def_rtg = row.get("def_rating", 0) or 0
            net_rtg = off_rtg - def_rtg
            tpc = row.get("total_pts_created", row.get("points", 0)) or 0
            ts = row.get("ts_pct", 0) or 0
            form_rows.append([
                f"G{gi}",
                f"{row.get('minutes', 0):.0f}",
                f"{row.get('points', 0):.0f}",
                f"{tpc:.1f}",
                f"{ts:.1%}" if ts > 0 else "-",
                f"{off_rtg:.0f}" if off_rtg > 0 else "-",
                f"{def_rtg:.0f}" if def_rtg > 0 else "-",
                f"{net_rtg:+.0f}" if off_rtg > 0 else "-",
            ])
        form_w = [18, 22, 22, 24, 24, 26, 26, 28]
        pdf._table(form_headers, form_rows, col_widths=form_w)
    else:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*_CLR_MUTED)
        pdf.cell(0, 6, "Recent form data not available.", new_x="LMARGIN", new_y="NEXT")

    # ── Output ──
    buf = io.BytesIO()
    pdf.output(buf)
    buf.seek(0)
    return buf
