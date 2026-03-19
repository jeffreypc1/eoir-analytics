"""EOIR Analytics Platform — Immigration Court Intelligence.

Port 8519. Investor-grade data visualization platform over 160M+ rows
of EOIR immigration court data in DuckDB.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# .env + path setup
# ---------------------------------------------------------------------------
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
if _ENV_PATH.exists():
    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "eoir.duckdb"

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EOIR Analytics",
    page_icon="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x2696;&#xFE0F;</text></svg>",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Design tokens
# ---------------------------------------------------------------------------
PRIMARY = "#0F172A"
ACCENT_BLUE = "#3B82F6"
ACCENT_PURPLE = "#8B5CF6"
ACCENT_GREEN = "#10B981"
ACCENT_AMBER = "#F59E0B"
ACCENT_RED = "#EF4444"
BG = "#F8FAFC"
CARD_BG = "#FFFFFF"
TEXT_PRIMARY = "#1E293B"
TEXT_SECONDARY = "#64748B"

CHART_COLORS = [ACCENT_BLUE, ACCENT_PURPLE, ACCENT_GREEN, ACCENT_AMBER, ACCENT_RED,
                "#06B6D4", "#EC4899", "#14B8A6", "#F97316", "#6366F1"]

# Plotly template
PLOTLY_TEMPLATE = dict(
    layout=go.Layout(
        font=dict(family="Inter, -apple-system, sans-serif", color=TEXT_PRIMARY),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=CHART_COLORS,
        margin=dict(l=40, r=24, t=32, b=40),
        xaxis=dict(gridcolor="#E2E8F0", gridwidth=1, zeroline=False),
        yaxis=dict(gridcolor="#E2E8F0", gridwidth=1, zeroline=False),
        hoverlabel=dict(bgcolor="#1E293B", font_color="white", font_size=13,
                        bordercolor="rgba(0,0,0,0)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    font=dict(size=12)),
    )
)

# ---------------------------------------------------------------------------
# Custom CSS — premium SaaS look
# ---------------------------------------------------------------------------
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, div[data-testid="stToolbar"],
header[data-testid="stHeader"] {{ display: none !important; }}

/* ── Global ── */
.stApp {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: {BG};
}}

/* ── Dark sidebar ── */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, {PRIMARY} 0%, #1E293B 100%);
    border-right: 1px solid rgba(255,255,255,0.06);
}}
section[data-testid="stSidebar"] * {{
    color: #CBD5E1 !important;
}}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stDateInput label {{
    color: #94A3B8 !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}}
section[data-testid="stSidebar"] .stMarkdown h3 {{
    color: #F8FAFC !important;
    font-weight: 700 !important;
}}
section[data-testid="stSidebar"] hr {{
    border-color: rgba(255,255,255,0.08) !important;
}}

/* ── KPI metric cards ── */
div[data-testid="stMetric"] {{
    background: linear-gradient(135deg, #1E293B 0%, {PRIMARY} 100%);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 20px 24px;
    box-shadow: 0 4px 24px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.08);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}}
div[data-testid="stMetric"]:hover {{
    transform: translateY(-2px);
    box-shadow: 0 8px 32px rgba(0,0,0,0.18);
}}
div[data-testid="stMetric"] label {{
    color: #94A3B8 !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
}}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
    color: #F8FAFC !important;
    font-size: 2.25rem !important;
    font-weight: 800 !important;
    letter-spacing: -0.02em !important;
}}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {{
    font-size: 0.85rem !important;
}}

/* ── Tab styling ── */
button[data-baseweb="tab"] {{
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    color: {TEXT_SECONDARY} !important;
    padding: 12px 20px !important;
    border-radius: 8px 8px 0 0 !important;
}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color: {ACCENT_BLUE} !important;
    border-bottom: 3px solid {ACCENT_BLUE} !important;
}}

/* ── Card wrapper ── */
.premium-card {{
    background: {CARD_BG};
    border: 1px solid #E2E8F0;
    border-radius: 16px;
    padding: 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 16px;
}}
.premium-card h4 {{
    margin: 0 0 16px 0;
    color: {TEXT_PRIMARY};
    font-size: 1rem;
    font-weight: 700;
}}

/* ── Hero header ── */
.hero-header {{
    background: linear-gradient(135deg, {PRIMARY} 0%, #1E293B 60%, #334155 100%);
    border-radius: 20px;
    padding: 32px 40px;
    margin-bottom: 24px;
    color: white;
    position: relative;
    overflow: hidden;
}}
.hero-header::after {{
    content: '';
    position: absolute;
    top: -50%;
    right: -10%;
    width: 300px;
    height: 300px;
    background: radial-gradient(circle, rgba(59,130,246,0.15) 0%, transparent 70%);
    border-radius: 50%;
}}
.hero-header h1 {{
    font-size: 1.75rem;
    font-weight: 800;
    margin: 0 0 4px 0;
    letter-spacing: -0.02em;
}}
.hero-header p {{
    color: #94A3B8;
    font-size: 0.9rem;
    margin: 0;
}}
.hero-badge {{
    display: inline-block;
    background: rgba(59,130,246,0.15);
    color: {ACCENT_BLUE};
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 8px;
}}

/* ── Section headers ── */
.section-header {{
    font-size: 1.1rem;
    font-weight: 700;
    color: {TEXT_PRIMARY};
    margin: 8px 0 16px 0;
    letter-spacing: -0.01em;
}}

/* ── Data tables ── */
.stDataFrame {{
    border-radius: 12px !important;
    overflow: hidden !important;
    border: 1px solid #E2E8F0 !important;
}}

/* ── Buttons ── */
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, {ACCENT_BLUE} 0%, {ACCENT_PURPLE} 100%) !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    padding: 8px 24px !important;
    transition: transform 0.15s ease !important;
}}
.stButton > button[kind="primary"]:hover {{
    transform: translateY(-1px) !important;
}}

/* ── Chat messages (AI tab) ── */
.chat-user {{
    background: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 16px 16px 4px 16px;
    padding: 14px 18px;
    margin: 8px 0;
    font-size: 0.9rem;
}}
.chat-ai {{
    background: {CARD_BG};
    border: 1px solid #E2E8F0;
    border-radius: 16px 16px 16px 4px;
    padding: 14px 18px;
    margin: 8px 0;
    font-size: 0.9rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}}
.chip {{
    display: inline-block;
    background: #EFF6FF;
    border: 1px solid #BFDBFE;
    color: {ACCENT_BLUE};
    padding: 6px 14px;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 500;
    cursor: pointer;
    margin: 4px;
    transition: all 0.15s ease;
}}
.chip:hover {{
    background: {ACCENT_BLUE};
    color: white;
}}

/* ── Plotly chart containers ── */
.stPlotlyChart {{
    border-radius: 12px;
    overflow: hidden;
}}

/* ── Reduce default spacing ── */
.block-container {{
    padding-top: 1rem !important;
    padding-bottom: 1rem !important;
    max-width: 100% !important;
}}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

@st.cache_resource
def get_db():
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    con = get_db()
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_table_list() -> list[str]:
    con = get_db()
    if con is None:
        return []
    result = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in result]


@st.cache_data(ttl=3600)
def get_lookup_values(table: str, code_col: str, desc_col: str | None = None) -> dict:
    con = get_db()
    if con is None:
        return {}
    try:
        if desc_col:
            rows = con.execute(f'SELECT "{code_col}", "{desc_col}" FROM "{table}"').fetchall()
            return {str(r[0]).strip(): str(r[1]).strip() for r in rows if r[0]}
        else:
            rows = con.execute(f'SELECT DISTINCT "{code_col}" FROM "{table}" ORDER BY "{code_col}"').fetchall()
            return {str(r[0]).strip(): str(r[0]).strip() for r in rows if r[0]}
    except Exception:
        return {}


@st.cache_data(ttl=3600)
def load_all_lookups():
    """Pre-load all lookup tables into memory for instant code resolution."""
    con = get_db()
    if con is None:
        return {}
    lookups = {}

    # Each entry: lookup_name -> {code: display_name}
    mapping = {
        "nationality": ("tbllookupnationality", "NAT_CODE", "NAT_NAME"),
        "language": ("tbllanguage", "strCode", "strDescription"),
        "base_city": ("tbllookupbasecity", "BASE_CITY_CODE", "BASE_CITY_NAME"),
        "judge": ("tbllookupjudge", "JUDGE_CODE", "JUDGE_NAME"),
        "decision": ("tbllookupcourtdecision", "strDecCode", "strDecDescription"),
        "case_type": ("tbllookupcasetype", "strCode", "strDescription"),
        "charge": ("tbllookupcharges", "strCode", "strCodeDescription"),
        "adjournment": ("tbladjournmentcodes", "strcode", "strDesciption"),
        "application": ("tbllookup_appln", "strcode", "strdescription"),
        "hearing_loc": ("tbllookuphloc", "HEARING_LOC_CODE", "HEARING_LOC_NAME"),
        "custody": ("tbllookupcustodystatus", "strCode", "strDescription"),
        "cal_type": ("tbllookupcal_type", "strCalTypeCode", "strCalTypeDescription"),
        "schedule_type": ("tbllookupschedule_type", "strCode", "strDescription"),
        "state": ("tbllookupstate", "state_code", "state_name"),
        "motion_type": ("tbllookupmotiontype", "strMotionCode", "strMotionDesc"),
        "filed_by": ("tbllookupfiledby", "strCode", "strDescription"),
        "appeal_type": ("tbllookupappealtype", "strApplCode", "strApplDescription"),
        "bia_decision": ("tbllookupbiadecision", "strCode", "strDescription"),
        "court_app_dec": ("tbllookupcourtappdecisions", "strCourtApplnDecCode", "strCourtApplnDecDesc"),
        "notice": ("tbllookupnotice", "Notice_Code", "Notice_Disp"),
    }

    for key, (table, code_col, desc_col) in mapping.items():
        try:
            rows = con.execute(f'SELECT "{code_col}", "{desc_col}" FROM "{table}"').fetchall()
            lookups[key] = {str(r[0]).strip(): str(r[1]).strip() for r in rows if r[0] and r[1]}
        except Exception:
            lookups[key] = {}

    return lookups


LOOKUPS = load_all_lookups()


# ---------------------------------------------------------------------------
# Helpers — chart creation
# ---------------------------------------------------------------------------

def _fmt_number(n: int | float) -> str:
    """Format large numbers with K/M suffix."""
    if pd.isna(n):
        return "N/A"
    n = float(n)
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:,.0f}"


def _make_area_chart(df, x, y, color=ACCENT_BLUE, title=None, height=350):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x], y=df[y], mode="lines",
        line=dict(color=color, width=2.5),
        fill="tozeroy",
        fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.08)",
        hovertemplate="%{x|%b %Y}<br><b>%{y:,.0f}</b><extra></extra>",
    ))
    fig.update_layout(
        height=height,
        template=PLOTLY_TEMPLATE,
        title=dict(text=title, font=dict(size=14, color=TEXT_PRIMARY)) if title else None,
        xaxis_title=None, yaxis_title=None,
        showlegend=False,
    )
    return fig


def _make_bar_chart(df, x, y, color=ACCENT_BLUE, horizontal=False, title=None, height=400):
    if horizontal:
        fig = go.Figure(go.Bar(
            x=df[y], y=df[x], orientation="h",
            marker=dict(color=color, cornerradius=6),
            hovertemplate="%{y}<br><b>%{x:,.0f}</b><extra></extra>",
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"))
    else:
        fig = go.Figure(go.Bar(
            x=df[x], y=df[y],
            marker=dict(color=color, cornerradius=6),
            hovertemplate="%{x}<br><b>%{y:,.0f}</b><extra></extra>",
        ))
    fig.update_layout(
        height=height,
        template=PLOTLY_TEMPLATE,
        title=dict(text=title, font=dict(size=14, color=TEXT_PRIMARY)) if title else None,
        showlegend=False,
    )
    return fig


def _make_donut(df, values, names, title=None, height=380, center_text=None):
    fig = go.Figure(go.Pie(
        values=df[values], labels=df[names],
        hole=0.55, textinfo="percent+label", textposition="outside",
        marker=dict(colors=CHART_COLORS[:len(df)]),
        hovertemplate="%{label}<br><b>%{value:,.0f}</b> (%{percent})<extra></extra>",
    ))
    if center_text:
        fig.add_annotation(
            text=center_text, x=0.5, y=0.5, font=dict(size=22, color=TEXT_PRIMARY, family="Inter"),
            showarrow=False, xref="paper", yref="paper",
        )
    fig.update_layout(
        height=height,
        template=PLOTLY_TEMPLATE,
        title=dict(text=title, font=dict(size=14, color=TEXT_PRIMARY)) if title else None,
        showlegend=False,
    )
    return fig


def card_open(title: str | None = None):
    html = '<div class="premium-card">'
    if title:
        html += f"<h4>{title}</h4>"
    st.markdown(html, unsafe_allow_html=True)


def card_close():
    st.markdown("</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Database check
# ---------------------------------------------------------------------------
con = get_db()
if con is None:
    st.error("Database not found. Run the import script first.")
    st.stop()


# ---------------------------------------------------------------------------
# Sidebar — premium filter controls
# ---------------------------------------------------------------------------

st.sidebar.markdown("""
<div style="text-align:center; padding: 16px 0 8px 0;">
    <div style="font-size: 1.4rem; font-weight: 800; color: #F8FAFC; letter-spacing: -0.02em;">
        EOIR Analytics
    </div>
    <div style="font-size: 0.7rem; color: #64748B; font-weight: 500; text-transform: uppercase; letter-spacing: 0.1em;">
        Immigration Court Intelligence
    </div>
</div>
""", unsafe_allow_html=True)

st.sidebar.divider()

# Date type selector — FIRST filter in sidebar
date_type = st.sidebar.selectbox(
    "Date Type",
    options=["Completion Date", "Filing Date", "Hearing Date"],
    index=0,
    help="Which date to filter by across the dashboard",
)

# Map date_type to the correct column per table
_DATE_COL_MAP = {
    "Completion Date": {"proc": "COMP_DATE", "sched": "ADJ_DATE", "motion": "COMP_DATE"},
    "Filing Date": {"proc": "OSC_DATE", "sched": "OSC_DATE", "motion": "OSC_DATE"},
    "Hearing Date": {"proc": "HEARING_DATE", "sched": "ADJ_DATE", "motion": "COMP_DATE"},
}
_active_date_cols = _DATE_COL_MAP[date_type]
_proc_date = _active_date_cols["proc"]   # shorthand for proc queries
_sched_date = _active_date_cols["sched"]  # shorthand for schedule queries

st.sidebar.markdown("")

# Date range
st.sidebar.markdown("**Date Range**")
d1, d2 = st.sidebar.columns(2)
with d1:
    date_from = st.date_input("From", value=date(2020, 1, 1), label_visibility="collapsed")
with d2:
    date_to = st.date_input("To", value=date.today(), label_visibility="collapsed")

date_from_str = date_from.strftime("%Y-%m-%d")
date_to_str = date_to.strftime("%Y-%m-%d")

st.sidebar.divider()

# Court filter — uses pre-loaded lookups
_court_lookup = LOOKUPS.get("base_city", {})
selected_courts = st.sidebar.multiselect(
    "Immigration Court",
    options=sorted(_court_lookup.keys(), key=lambda x: _court_lookup.get(x, x)),
    format_func=lambda x: _court_lookup.get(x, x),
    placeholder="All Courts",
)

# Nationality filter
_nat_lookup = LOOKUPS.get("nationality", {})
selected_nats = st.sidebar.multiselect(
    "Nationality",
    options=sorted(_nat_lookup.keys(), key=lambda x: _nat_lookup.get(x, x)),
    format_func=lambda x: _nat_lookup.get(x, x),
    placeholder="All Nationalities",
)

# Case type filter
_ct_lookup = LOOKUPS.get("case_type", {})
selected_case_types = st.sidebar.multiselect(
    "Case Type",
    options=sorted(_ct_lookup.keys(), key=lambda x: _ct_lookup.get(x, x)),
    format_func=lambda x: _ct_lookup.get(x, x),
    placeholder="All Types",
)

# Judge filter
_judge_lookup = {k: v for k, v in LOOKUPS.get("judge", {}).items() if v != "<All Judges>"}
selected_judges = st.sidebar.multiselect(
    "Immigration Judge",
    options=sorted(_judge_lookup.keys(), key=lambda x: _judge_lookup.get(x, x)),
    format_func=lambda x: _judge_lookup.get(x, x),
    placeholder="All Judges",
)

# Custody status filter
_custody_lookup = LOOKUPS.get("custody", {})
selected_custody = st.sidebar.multiselect(
    "Custody Status",
    options=sorted(_custody_lookup.keys(), key=lambda x: _custody_lookup.get(x, x)),
    format_func=lambda x: _custody_lookup.get(x, x),
    placeholder="All Statuses",
)

st.sidebar.divider()
st.sidebar.markdown(
    '<div style="text-align:center; font-size:0.65rem; color:#475569; padding:8px 0;">'
    f'Filtering by: {date_type}<br>'
    f'Data period: {date_from_str} to {date_to_str}<br>'
    '160M+ records &middot; 89 tables &middot; DuckDB'
    '</div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# WHERE clause builder (preserved from original — correct cross-table logic)
# ---------------------------------------------------------------------------

def build_where(
    proc_alias: str = "p",
    date_col: str | None = None,
    case_alias: str | None = None,
    table_type: str = "proc",
) -> tuple[str, bool]:
    """Build SQL WHERE clause from sidebar filters.

    table_type: "proc" | "sched" | "motion" — selects the right date column
    for the active date_type. Override with explicit date_col if needed.

    Returns (where_clause, needs_case_join).
    """
    conditions: list[str] = []
    needs_case_join = False

    # Resolve date column: explicit override > date type mapping
    if date_col is None:
        date_col = _active_date_cols.get(table_type, "COMP_DATE")

    conditions.append(
        f'TRY_CAST({proc_alias}."{date_col}" AS TIMESTAMP) >= \'{date_from_str}\''
    )
    conditions.append(
        f'TRY_CAST({proc_alias}."{date_col}" AS TIMESTAMP) <= \'{date_to_str}\''
    )
    conditions.append(f'{proc_alias}."{date_col}" IS NOT NULL')

    if selected_courts:
        quoted = ", ".join(f"'{c}'" for c in selected_courts)
        conditions.append(f'{proc_alias}."BASE_CITY_CODE" IN ({quoted})')
    if selected_case_types:
        quoted = ", ".join(f"'{t}'" for t in selected_case_types)
        conditions.append(f'{proc_alias}."CASE_TYPE" IN ({quoted})')
    if selected_judges:
        quoted = ", ".join(f"'{j}'" for j in selected_judges)
        conditions.append(f'{proc_alias}."IJ_CODE" IN ({quoted})')

    if selected_nats:
        needs_case_join = True
        ca = case_alias or "c"
        quoted = ", ".join(f"'{n}'" for n in selected_nats)
        conditions.append(f'{ca}."NAT" IN ({quoted})')

    if selected_custody:
        needs_case_join = True
        ca = case_alias or "c"
        quoted = ", ".join(f"'{s}'" for s in selected_custody)
        conditions.append(f'{ca}."CUSTODY" IN ({quoted})')

    return " AND ".join(conditions), needs_case_join


def _proc_from(proc_alias: str = "p", case_alias: str = "c", needs_case_join: bool = False) -> str:
    base = f"b_tblproceeding {proc_alias}"
    if needs_case_join:
        base += f"\n        JOIN a_tblcase {case_alias} ON TRY_CAST({proc_alias}.IDNCASE AS BIGINT) = {case_alias}.IDNCASE"
    return base


def _sched_from(sched_alias: str = "s", case_alias: str = "c", needs_case_join: bool = False) -> str:
    base = f"tbl_schedule {sched_alias}"
    if needs_case_join:
        base += f"\n        JOIN a_tblcase {case_alias} ON TRY_CAST({sched_alias}.IDNCASE AS BIGINT) = {case_alias}.IDNCASE"
    return base


# ---------------------------------------------------------------------------
# Hero header
# ---------------------------------------------------------------------------

st.markdown("""
<div class="hero-header">
    <span class="hero-badge">Live Data Platform</span>
    <h1>EOIR Analytics</h1>
    <p>Real-time intelligence across 160M+ immigration court records &mdash; proceedings, hearings, judges, and outcomes.</p>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_exec, tab_outcomes, tab_courts, tab_judges, tab_explore, tab_ai = st.tabs([
    "Executive Summary",
    "Case Outcomes",
    "Court Performance",
    "Judge Analytics",
    "Data Explorer",
    "AI Analyst",
])


# ===== TAB 1: Executive Summary =============================================
with tab_exec:
    where, needs_join = build_where("p", case_alias="c", table_type="proc")
    from_clause = _proc_from("p", "c", needs_join)

    # --- KPI row ---
    kpi_sql = f"""
        SELECT
            COUNT(*) as total_proceedings,
            COUNT(DISTINCT p.IDNCASE) as unique_cases,
            COUNT(DISTINCT p.IJ_CODE) as unique_judges,
            COUNT(DISTINCT p.BASE_CITY_CODE) as courts_active,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate,
            ROUND(AVG(CASE
                WHEN TRY_CAST(p.COMP_DATE AS TIMESTAMP) IS NOT NULL
                     AND TRY_CAST(p.OSC_DATE AS TIMESTAMP) IS NOT NULL
                THEN DATEDIFF('day', TRY_CAST(p.OSC_DATE AS TIMESTAMP), TRY_CAST(p.COMP_DATE AS TIMESTAMP))
                ELSE NULL END), 0) as avg_days_to_decision
        FROM {from_clause}
        WHERE {where}
    """
    kpi = run_query(kpi_sql)

    # Also get this-year completions for comparison
    this_year = date.today().year
    kpi_year_sql = f"""
        SELECT COUNT(*) as completed_this_year
        FROM {from_clause}
        WHERE {where}
            AND TRY_CAST(p."{_proc_date}" AS TIMESTAMP) >= '{this_year}-01-01'
    """
    kpi_year = run_query(kpi_year_sql)

    if not kpi.empty:
        k = kpi.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.metric("Total Proceedings", _fmt_number(k["total_proceedings"]))
        with c2:
            st.metric("Unique Cases", _fmt_number(k["unique_cases"]))
        with c3:
            days = k["avg_days_to_decision"]
            st.metric("Avg Days to Decision", f"{int(days):,}" if pd.notna(days) else "N/A")
        with c4:
            gr = k["grant_rate"]
            st.metric("National Grant Rate", f"{gr:.1f}%" if pd.notna(gr) else "N/A")
        with c5:
            yr_count = kpi_year.iloc[0]["completed_this_year"] if not kpi_year.empty else 0
            st.metric("Completed This Year", _fmt_number(yr_count))

    st.markdown("")

    # --- Cases over time (area chart) ---
    time_sql = f"""
        SELECT
            DATE_TRUNC('month', TRY_CAST(p."{_proc_date}" AS TIMESTAMP)) as month,
            COUNT(*) as cases
        FROM {from_clause}
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """
    time_df = run_query(time_sql)
    if not time_df.empty:
        fig = _make_area_chart(time_df, "month", "cases", ACCENT_BLUE, "Completed Cases Over Time", 340)
        st.plotly_chart(fig, use_container_width=True, key="exec_time")

    # --- Two-column: Nationalities + Outcomes ---
    col_left, col_right = st.columns(2)

    # Always need case join for nationality
    where_nat, _ = build_where("p", case_alias="c", table_type="proc")
    from_nat = _proc_from("p", "c", needs_case_join=True)

    with col_left:
        nat_sql = f"""
            SELECT n.NAT_NAME as nationality, COUNT(*) as cases
            FROM {from_nat}
            LEFT JOIN tbllookupnationality n ON c.NAT = n.NAT_CODE
            WHERE {where_nat} AND n.NAT_NAME IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
        nat_df = run_query(nat_sql)
        if not nat_df.empty:
            # Gradient coloring for bars
            n = len(nat_df)
            bar_colors = [f"rgba(59,130,246,{0.4 + 0.6 * (n - i) / n})" for i in range(n)]
            fig = go.Figure(go.Bar(
                x=nat_df["cases"], y=nat_df["nationality"], orientation="h",
                marker=dict(color=bar_colors, cornerradius=6),
                hovertemplate="%{y}<br><b>%{x:,.0f} cases</b><extra></extra>",
            ))
            fig.update_layout(
                height=420, template=PLOTLY_TEMPLATE,
                title=dict(text="Top 10 Nationalities", font=dict(size=14, color=TEXT_PRIMARY)),
                yaxis=dict(autorange="reversed"), showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="exec_nat")

    with col_right:
        dec_sql = f"""
            SELECT d.strDecDescription as outcome, COUNT(*) as cases
            FROM {from_clause}
            LEFT JOIN tbllookupcourtdecision d ON p.DEC_CODE = d.strDecCode
                AND p.CASE_TYPE = d.strCaseType
            WHERE {where} AND d.strDecDescription IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """
        dec_df = run_query(dec_sql)
        if not dec_df.empty:
            total = dec_df["cases"].sum()
            fig = _make_donut(dec_df, "cases", "outcome", "Case Outcome Breakdown", 420,
                              center_text=_fmt_number(total))
            st.plotly_chart(fig, use_container_width=True, key="exec_donut")


# ===== TAB 2: Case Outcomes ==================================================
with tab_outcomes:
    where, needs_join = build_where("p", case_alias="c", table_type="proc")
    from_clause = _proc_from("p", "c", needs_join)

    # --- Grant vs Denial rate over time ---
    gd_sql = f"""
        SELECT
            DATE_TRUNC('quarter', TRY_CAST(p."{_proc_date}" AS TIMESTAMP)) as quarter,
            SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) as grants,
            SUM(CASE WHEN p.DEC_CODE IN ('D','R','X') THEN 1 ELSE 0 END) as denials,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate
        FROM {from_clause}
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """
    gd_df = run_query(gd_sql)
    if not gd_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=gd_df["quarter"], y=gd_df["grants"], name="Grants",
            mode="lines", fill="tonexty" if False else "tozeroy",
            line=dict(color=ACCENT_GREEN, width=2),
            fillcolor="rgba(16,185,129,0.1)",
            stackgroup="one",
            hovertemplate="Grants: <b>%{y:,.0f}</b><extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=gd_df["quarter"], y=gd_df["denials"], name="Denials",
            mode="lines", fill="tonexty",
            line=dict(color=ACCENT_RED, width=2),
            fillcolor="rgba(239,68,68,0.1)",
            stackgroup="one",
            hovertemplate="Denials: <b>%{y:,.0f}</b><extra></extra>",
        ))
        fig.update_layout(
            height=380, template=PLOTLY_TEMPLATE,
            title=dict(text="Grant vs Denial Volume Over Time", font=dict(size=14)),
        )
        st.plotly_chart(fig, use_container_width=True, key="out_gd_vol")

    # Grant rate trend line
    if not gd_df.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=gd_df["quarter"], y=gd_df["grant_rate"],
            mode="lines+markers",
            line=dict(color=ACCENT_BLUE, width=3),
            marker=dict(size=5, color=ACCENT_BLUE),
            fill="tozeroy",
            fillcolor="rgba(59,130,246,0.06)",
            hovertemplate="%{x|%b %Y}<br>Grant Rate: <b>%{y:.1f}%</b><extra></extra>",
        ))
        fig2.update_layout(
            height=300, template=PLOTLY_TEMPLATE,
            title=dict(text="Grant Rate Trend (%)", font=dict(size=14)),
            yaxis=dict(ticksuffix="%"),
        )
        st.plotly_chart(fig2, use_container_width=True, key="out_gr_trend")

    st.markdown("")

    # --- In Absentia rate ---
    abs_sql = f"""
        SELECT
            DATE_TRUNC('quarter', TRY_CAST(p."{_proc_date}" AS TIMESTAMP)) as quarter,
            SUM(CASE WHEN p.ABSENTIA = 'Y' THEN 1 ELSE 0 END) as absentia,
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN p.ABSENTIA = 'Y' THEN 1 ELSE 0 END) / COUNT(*), 1) as absentia_rate
        FROM {from_clause}
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """
    abs_df = run_query(abs_sql)
    if not abs_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=abs_df["quarter"], y=abs_df["absentia_rate"],
            mode="lines", line=dict(color=ACCENT_AMBER, width=2.5),
            fill="tozeroy", fillcolor="rgba(245,158,11,0.08)",
            hovertemplate="%{x|%b %Y}<br>In Absentia: <b>%{y:.1f}%</b><extra></extra>",
        ))
        avg_rate = abs_df["absentia_rate"].mean()
        fig.add_hline(y=avg_rate, line_dash="dot", line_color=TEXT_SECONDARY,
                      annotation_text=f"Avg: {avg_rate:.1f}%",
                      annotation_position="top right")
        fig.update_layout(
            height=300, template=PLOTLY_TEMPLATE,
            title=dict(text="In Absentia Order Rate", font=dict(size=14)),
            yaxis=dict(ticksuffix="%"),
        )
        st.plotly_chart(fig, use_container_width=True, key="out_absentia")

    st.markdown("")

    # --- Decision breakdown table ---
    dec_tbl_sql = f"""
        SELECT
            d.strDecDescription as decision,
            COUNT(*) as total_cases,
            SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) as grants,
            SUM(CASE WHEN p.DEC_CODE IN ('D','R','X') THEN 1 ELSE 0 END) as denials,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate_pct
        FROM {from_clause}
        LEFT JOIN tbllookupcourtdecision d ON p.DEC_CODE = d.strDecCode
            AND p.CASE_TYPE = d.strCaseType
        WHERE {where} AND d.strDecDescription IS NOT NULL
        GROUP BY 1
        ORDER BY total_cases DESC
        LIMIT 20
    """
    dec_tbl_df = run_query(dec_tbl_sql)
    if not dec_tbl_df.empty:
        st.markdown('<p class="section-header">Decision Breakdown</p>', unsafe_allow_html=True)
        st.dataframe(
            dec_tbl_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "decision": st.column_config.TextColumn("Decision"),
                "total_cases": st.column_config.NumberColumn("Total Cases", format="%d"),
                "grants": st.column_config.NumberColumn("Grants", format="%d"),
                "denials": st.column_config.NumberColumn("Denials", format="%d"),
                "grant_rate_pct": st.column_config.ProgressColumn(
                    "Grant Rate", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
        )

    # --- Outcome by nationality heatmap ---
    where_nat_out, _ = build_where("p", case_alias="c", table_type="proc")
    from_nat_out = _proc_from("p", "c", needs_case_join=True)

    heat_sql = f"""
        WITH top_nats AS (
            SELECT c.NAT, n.NAT_NAME
            FROM {from_nat_out}
            LEFT JOIN tbllookupnationality n ON c.NAT = n.NAT_CODE
            WHERE {where_nat_out} AND n.NAT_NAME IS NOT NULL
            GROUP BY 1, 2 ORDER BY COUNT(*) DESC LIMIT 12
        ),
        top_decs AS (
            SELECT d.strDecDescription
            FROM b_tblproceeding p2
            LEFT JOIN tbllookupcourtdecision d ON p2.DEC_CODE = d.strDecCode
                AND p2.CASE_TYPE = d.strCaseType
            WHERE TRY_CAST(p2."{_proc_date}" AS TIMESTAMP) >= '{date_from_str}'
                AND TRY_CAST(p2."{_proc_date}" AS TIMESTAMP) <= '{date_to_str}'
                AND p2."{_proc_date}" IS NOT NULL
                AND d.strDecDescription IS NOT NULL
            GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5
        )
        SELECT n.NAT_NAME as nationality, d.strDecDescription as outcome, COUNT(*) as cases
        FROM {from_nat_out}
        LEFT JOIN tbllookupnationality n ON c.NAT = n.NAT_CODE
        LEFT JOIN tbllookupcourtdecision d ON p.DEC_CODE = d.strDecCode
            AND p.CASE_TYPE = d.strCaseType
        WHERE {where_nat_out}
            AND n.NAT_NAME IN (SELECT NAT_NAME FROM top_nats)
            AND d.strDecDescription IN (SELECT strDecDescription FROM top_decs)
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    heat_df = run_query(heat_sql)
    if not heat_df.empty:
        st.markdown('<p class="section-header">Outcomes by Nationality</p>', unsafe_allow_html=True)
        pivot = heat_df.pivot_table(index="nationality", columns="outcome", values="cases", fill_value=0)
        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, "#F8FAFC"], [0.5, "#93C5FD"], [1, "#1D4ED8"]],
            hovertemplate="%{y}<br>%{x}<br><b>%{z:,.0f} cases</b><extra></extra>",
            showscale=True,
            colorbar=dict(title="Cases", thickness=12, len=0.6),
        ))
        fig.update_layout(
            height=max(350, len(pivot) * 35 + 100),
            template=PLOTLY_TEMPLATE,
            xaxis=dict(side="top"),
        )
        st.plotly_chart(fig, use_container_width=True, key="out_heatmap")


# ===== TAB 3: Court Performance ==============================================
with tab_courts:
    where, needs_join = build_where("p", case_alias="c", table_type="proc")
    from_clause = _proc_from("p", "c", needs_join)

    # --- Court comparison table ---
    court_sql = f"""
        SELECT
            l.BASE_CITY_NAME as court,
            p.BASE_CITY_CODE as code,
            COUNT(*) as caseload,
            SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) as grants,
            SUM(CASE WHEN p.DEC_CODE IN ('D','R','X') THEN 1 ELSE 0 END) as denials,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate,
            ROUND(AVG(CASE
                WHEN TRY_CAST(p.COMP_DATE AS TIMESTAMP) IS NOT NULL
                     AND TRY_CAST(p.OSC_DATE AS TIMESTAMP) IS NOT NULL
                THEN DATEDIFF('day', TRY_CAST(p.OSC_DATE AS TIMESTAMP), TRY_CAST(p.COMP_DATE AS TIMESTAMP))
                ELSE NULL END), 0) as avg_days,
            COUNT(DISTINCT p.IJ_CODE) as judge_count
        FROM {from_clause}
        LEFT JOIN tbllookupbasecity l ON p.BASE_CITY_CODE = l.BASE_CITY_CODE
        WHERE {where} AND l.BASE_CITY_NAME IS NOT NULL
        GROUP BY 1, 2
        HAVING COUNT(*) >= 100
        ORDER BY caseload DESC
    """
    court_df = run_query(court_sql)

    if not court_df.empty:
        st.markdown('<p class="section-header">Court Comparison</p>', unsafe_allow_html=True)
        display_court_df = court_df.drop(columns=["code"])
        st.dataframe(
            display_court_df,
            use_container_width=True,
            hide_index=True,
            height=min(len(display_court_df) * 35 + 38, 600),
            column_config={
                "court": st.column_config.TextColumn("Court"),
                "caseload": st.column_config.NumberColumn("Caseload", format="%d"),
                "grants": st.column_config.NumberColumn("Grants", format="%d"),
                "denials": st.column_config.NumberColumn("Denials", format="%d"),
                "grant_rate": st.column_config.ProgressColumn(
                    "Grant Rate %", min_value=0, max_value=100, format="%.1f%%"
                ),
                "avg_days": st.column_config.NumberColumn("Avg Days", format="%d"),
                "judge_count": st.column_config.NumberColumn("Judges", format="%d"),
            },
        )

        st.markdown("")

        # Treemap of caseload by court
        top_courts = court_df.head(30)
        fig = go.Figure(go.Treemap(
            labels=top_courts["court"],
            parents=[""] * len(top_courts),
            values=top_courts["caseload"],
            marker=dict(
                colors=top_courts["grant_rate"],
                colorscale=[[0, ACCENT_RED], [0.5, ACCENT_AMBER], [1, ACCENT_GREEN]],
                showscale=True,
                colorbar=dict(title="Grant Rate %", thickness=12, len=0.6),
            ),
            hovertemplate="<b>%{label}</b><br>Caseload: %{value:,.0f}<br>Grant Rate: %{color:.1f}%<extra></extra>",
            textinfo="label+value",
            textfont=dict(size=12),
        ))
        fig.update_layout(
            height=500, template=PLOTLY_TEMPLATE,
            title=dict(text="Caseload by Court (colored by grant rate)", font=dict(size=14)),
            margin=dict(l=8, r=8, t=48, b=8),
        )
        st.plotly_chart(fig, use_container_width=True, key="court_treemap")

    st.markdown("")

    # --- Court-level trend ---
    st.markdown('<p class="section-header">Court Trend Over Time</p>', unsafe_allow_html=True)
    if not court_df.empty:
        court_options = {row["code"]: row["court"] for _, row in court_df.iterrows()}
        selected_court_code = st.selectbox(
            "Select a court",
            options=list(court_options.keys()),
            format_func=lambda x: court_options.get(x, x),
            key="court_trend_select",
        )
        if selected_court_code:
            ct_trend_sql = f"""
                SELECT
                    DATE_TRUNC('quarter', TRY_CAST(p."{_proc_date}" AS TIMESTAMP)) as quarter,
                    COUNT(*) as cases,
                    ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate
                FROM {from_clause}
                WHERE {where} AND p.BASE_CITY_CODE = '{selected_court_code}'
                GROUP BY 1 ORDER BY 1
            """
            ct_trend_df = run_query(ct_trend_sql)
            if not ct_trend_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=ct_trend_df["quarter"], y=ct_trend_df["cases"],
                    name="Cases", marker=dict(color=ACCENT_BLUE, cornerradius=4, opacity=0.6),
                    hovertemplate="%{x|%b %Y}<br>Cases: <b>%{y:,.0f}</b><extra></extra>",
                    yaxis="y",
                ))
                fig.add_trace(go.Scatter(
                    x=ct_trend_df["quarter"], y=ct_trend_df["grant_rate"],
                    name="Grant Rate %", mode="lines+markers",
                    line=dict(color=ACCENT_GREEN, width=3),
                    marker=dict(size=6),
                    hovertemplate="%{x|%b %Y}<br>Grant Rate: <b>%{y:.1f}%</b><extra></extra>",
                    yaxis="y2",
                ))
                fig.update_layout(
                    height=380, template=PLOTLY_TEMPLATE,
                    title=dict(text=f"{court_options.get(selected_court_code, '')} — Cases & Grant Rate",
                               font=dict(size=14)),
                    yaxis=dict(title="Cases", side="left"),
                    yaxis2=dict(title="Grant Rate %", side="right", overlaying="y",
                                ticksuffix="%", showgrid=False),
                    barmode="overlay",
                )
                st.plotly_chart(fig, use_container_width=True, key="court_trend_chart")


# ===== TAB 4: Judge Analytics ================================================
with tab_judges:
    where, needs_join = build_where("p", case_alias="c", table_type="proc")
    from_clause = _proc_from("p", "c", needs_join)

    # --- Judge scorecard ---
    judge_sql = f"""
        SELECT
            j.JUDGE_NAME as judge,
            l.BASE_CITY_NAME as court,
            p.IJ_CODE as judge_code,
            COUNT(*) as caseload,
            SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) as grants,
            SUM(CASE WHEN p.DEC_CODE IN ('D','R','X') THEN 1 ELSE 0 END) as denials,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate,
            ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('D','R','X') THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as denial_rate,
            ROUND(AVG(CASE
                WHEN TRY_CAST(p.COMP_DATE AS TIMESTAMP) IS NOT NULL
                     AND TRY_CAST(p.OSC_DATE AS TIMESTAMP) IS NOT NULL
                THEN DATEDIFF('day', TRY_CAST(p.OSC_DATE AS TIMESTAMP), TRY_CAST(p.COMP_DATE AS TIMESTAMP))
                ELSE NULL END), 0) as avg_days
        FROM {from_clause}
        LEFT JOIN tbllookupjudge j ON p.IJ_CODE = j.JUDGE_CODE
        LEFT JOIN tbllookupbasecity l ON p.BASE_CITY_CODE = l.BASE_CITY_CODE
        WHERE {where}
            AND j.JUDGE_NAME IS NOT NULL AND j.JUDGE_NAME != '<All Judges>'
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 50
        ORDER BY caseload DESC
    """
    judge_df = run_query(judge_sql)

    if not judge_df.empty:
        st.markdown('<p class="section-header">Judge Scorecard</p>', unsafe_allow_html=True)
        display_judge_df = judge_df.drop(columns=["judge_code"])
        st.dataframe(
            display_judge_df,
            use_container_width=True,
            hide_index=True,
            height=min(len(display_judge_df) * 35 + 38, 600),
            column_config={
                "judge": st.column_config.TextColumn("Judge"),
                "court": st.column_config.TextColumn("Court"),
                "caseload": st.column_config.NumberColumn("Caseload", format="%d"),
                "grants": st.column_config.NumberColumn("Grants", format="%d"),
                "denials": st.column_config.NumberColumn("Denials", format="%d"),
                "grant_rate": st.column_config.ProgressColumn(
                    "Grant Rate %", min_value=0, max_value=100, format="%.1f%%"
                ),
                "denial_rate": st.column_config.ProgressColumn(
                    "Denial Rate %", min_value=0, max_value=100, format="%.1f%%"
                ),
                "avg_days": st.column_config.NumberColumn("Avg Days", format="%d"),
            },
        )

        st.markdown("")

        # Grant rate distribution histogram
        col_hist, col_topbot = st.columns(2)

        with col_hist:
            fig = go.Figure(go.Histogram(
                x=judge_df["grant_rate"].dropna(), nbinsx=25,
                marker=dict(color=ACCENT_PURPLE, cornerradius=4),
                hovertemplate="Grant Rate: %{x:.0f}%<br>Judges: <b>%{y}</b><extra></extra>",
            ))
            fig.update_layout(
                height=380, template=PLOTLY_TEMPLATE,
                title=dict(text="Grant Rate Distribution Across Judges", font=dict(size=14)),
                xaxis=dict(title="Grant Rate %", ticksuffix="%"),
                yaxis=dict(title="Number of Judges"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="judge_hist")

        with col_topbot:
            # Top 10 and bottom 10
            judge_sorted = judge_df.dropna(subset=["grant_rate"]).sort_values("grant_rate")
            bottom10 = judge_sorted.head(10).copy()
            top10 = judge_sorted.tail(10).copy()
            combined = pd.concat([top10, bottom10])
            combined = combined.sort_values("grant_rate")

            colors = [ACCENT_GREEN if r >= judge_sorted["grant_rate"].median() else ACCENT_RED
                      for r in combined["grant_rate"]]

            fig = go.Figure(go.Bar(
                x=combined["grant_rate"], y=combined["judge"], orientation="h",
                marker=dict(color=colors, cornerradius=4),
                hovertemplate="%{y}<br>Grant Rate: <b>%{x:.1f}%</b><extra></extra>",
            ))
            fig.update_layout(
                height=380, template=PLOTLY_TEMPLATE,
                title=dict(text="Top 10 & Bottom 10 by Grant Rate", font=dict(size=14)),
                xaxis=dict(ticksuffix="%"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="judge_topbot")

        # --- Individual judge trend ---
        st.markdown("")
        st.markdown('<p class="section-header">Individual Judge Trend</p>', unsafe_allow_html=True)
        judge_options = {row["judge_code"]: f"{row['judge']} ({row['court'] or 'Unknown'})"
                        for _, row in judge_df.iterrows()}
        selected_judge_code = st.selectbox(
            "Select a judge",
            options=list(judge_options.keys()),
            format_func=lambda x: judge_options.get(x, x),
            key="judge_trend_select",
        )
        if selected_judge_code:
            jt_sql = f"""
                SELECT
                    DATE_TRUNC('quarter', TRY_CAST(p."{_proc_date}" AS TIMESTAMP)) as quarter,
                    COUNT(*) as cases,
                    ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate
                FROM {from_clause}
                WHERE {where} AND p.IJ_CODE = '{selected_judge_code}'
                GROUP BY 1 ORDER BY 1
            """
            jt_df = run_query(jt_sql)
            if not jt_df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=jt_df["quarter"], y=jt_df["cases"],
                    name="Cases", marker=dict(color=ACCENT_BLUE, cornerradius=4, opacity=0.5),
                    yaxis="y",
                    hovertemplate="%{x|%b %Y}<br>Cases: <b>%{y:,.0f}</b><extra></extra>",
                ))
                fig.add_trace(go.Scatter(
                    x=jt_df["quarter"], y=jt_df["grant_rate"],
                    name="Grant Rate %", mode="lines+markers",
                    line=dict(color=ACCENT_GREEN, width=3),
                    marker=dict(size=6),
                    yaxis="y2",
                    hovertemplate="%{x|%b %Y}<br>Grant Rate: <b>%{y:.1f}%</b><extra></extra>",
                ))
                fig.update_layout(
                    height=380, template=PLOTLY_TEMPLATE,
                    title=dict(text=judge_options.get(selected_judge_code, ""),
                               font=dict(size=14)),
                    yaxis=dict(title="Cases", side="left"),
                    yaxis2=dict(title="Grant Rate %", side="right", overlaying="y",
                                ticksuffix="%", showgrid=False),
                    barmode="overlay",
                )
                st.plotly_chart(fig, use_container_width=True, key="judge_trend_chart")


# ===== TAB 5: Data Explorer ==================================================
with tab_explore:
    st.markdown('<p class="section-header">Custom SQL Explorer</p>', unsafe_allow_html=True)
    st.caption("Query the EOIR database directly. Results capped at 5,000 rows.")

    # Schema reference
    with st.expander("Schema Reference", expanded=False):
        tables = get_table_list()
        main_tables = ["a_tblcase", "b_tblproceeding", "tbl_schedule", "tbl_court_appln",
                       "tbl_court_motions", "b_tblproceedcharges", "tbl_repsassigned"]
        lookup_tables = sorted([t for t in tables if t.startswith("tbllookup") or t in ("tbladjournmentcodes", "tbllanguage", "tbldeccode")])

        st.markdown("**Main Tables**")
        for t in main_tables:
            try:
                cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
                col_names = ", ".join(c[0] for c in cols)
                st.markdown(f"`{t}`: {col_names}")
            except Exception:
                st.markdown(f"`{t}`")

        st.markdown("**Lookup Tables**")
        for t in lookup_tables:
            try:
                cols = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
                col_names = ", ".join(c[0] for c in cols)
                st.markdown(f"`{t}`: {col_names}")
            except Exception:
                st.markdown(f"`{t}`")

    # Example queries as buttons
    st.markdown("**Quick Queries**")
    example_queries = {
        "Grant Rate by Year": """SELECT
    DATE_TRUNC('year', TRY_CAST(p.COMP_DATE AS TIMESTAMP)) as year,
    COUNT(*) as total,
    SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) as grants,
    ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END) / COUNT(*), 1) as grant_rate
FROM b_tblproceeding p
WHERE TRY_CAST(p.COMP_DATE AS TIMESTAMP) >= '2015-01-01' AND p.COMP_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1""",
        "Top 20 Judges by Caseload": """SELECT j.JUDGE_NAME, l.BASE_CITY_NAME as court, COUNT(*) as cases,
    ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate
FROM b_tblproceeding p
LEFT JOIN tbllookupjudge j ON p.IJ_CODE = j.JUDGE_CODE
LEFT JOIN tbllookupbasecity l ON p.BASE_CITY_CODE = l.BASE_CITY_CODE
WHERE TRY_CAST(p.COMP_DATE AS TIMESTAMP) >= '2020-01-01' AND p.COMP_DATE IS NOT NULL
    AND j.JUDGE_NAME IS NOT NULL AND j.JUDGE_NAME != '<All Judges>'
GROUP BY 1, 2 HAVING COUNT(*) >= 100
ORDER BY cases DESC LIMIT 20""",
        "Asylum Cases by Nationality": """SELECT n.NAT_NAME as nationality, COUNT(*) as cases,
    ROUND(100.0 * SUM(CASE WHEN p.DEC_CODE IN ('G','A') THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN p.DEC_CODE IN ('G','A','D','R','X') THEN 1 ELSE 0 END), 0), 1) as grant_rate
FROM b_tblproceeding p
JOIN a_tblcase c ON TRY_CAST(p.IDNCASE AS BIGINT) = c.IDNCASE
LEFT JOIN tbllookupnationality n ON c.NAT = n.NAT_CODE
WHERE p.CASE_TYPE = 'ASY'
    AND TRY_CAST(p.COMP_DATE AS TIMESTAMP) >= '2020-01-01' AND p.COMP_DATE IS NOT NULL
    AND n.NAT_NAME IS NOT NULL
GROUP BY 1 ORDER BY cases DESC LIMIT 20""",
        "Court Hearing Volumes (Monthly)": """SELECT
    DATE_TRUNC('month', TRY_CAST(s.ADJ_DATE AS TIMESTAMP)) as month,
    COUNT(*) as hearings
FROM tbl_schedule s
WHERE TRY_CAST(s.ADJ_DATE AS TIMESTAMP) >= '2023-01-01'
    AND TRY_CAST(s.ADJ_DATE AS TIMESTAMP) <= '2024-12-31'
    AND s.ADJ_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1""",
    }

    btn_cols = st.columns(len(example_queries))
    for i, (name, query) in enumerate(example_queries.items()):
        with btn_cols[i]:
            if st.button(name, key=f"ex_{i}", use_container_width=True):
                st.session_state["explore_sql_input"] = query

    # SQL editor
    default_sql = st.session_state.get("explore_sql_input", list(example_queries.values())[0])
    sql = st.text_area("SQL Query", value=default_sql, height=200, key="explore_sql_area")

    run_col, resolve_col, _ = st.columns([1, 2, 3])
    with run_col:
        run_clicked = st.button("Run Query", type="primary", key="run_explore")
    with resolve_col:
        resolve_codes_on = st.checkbox("Resolve lookup codes", value=True, key="resolve_codes_toggle")

    def resolve_codes(df: pd.DataFrame) -> pd.DataFrame:
        """Replace code columns with human-readable names."""
        code_mappings = {
            "NAT": "nationality",
            "LANG": "language",
            "BASE_CITY_CODE": "base_city",
            "IJ_CODE": "judge",
            "DEC_CODE": "decision",
            "CASE_TYPE": "case_type",
            "CHARGE": "charge",
            "ADJ_RSN": "adjournment",
            "APPL_CODE": "application",
            "HEARING_LOC_CODE": "hearing_loc",
            "CUSTODY": "custody",
            "CAL_TYPE": "cal_type",
            "MOTION_TYPE": "motion_type",
        }
        df = df.copy()
        for col, lookup_key in code_mappings.items():
            if col in df.columns and lookup_key in LOOKUPS:
                df[col] = df[col].map(
                    lambda x, lk=lookup_key: LOOKUPS[lk].get(str(x).strip(), x) if pd.notna(x) else x
                )
        return df

    if run_clicked:
        # Add LIMIT if not present
        safe_sql = sql.strip().rstrip(";")
        if "limit" not in safe_sql.lower():
            safe_sql += "\nLIMIT 5000"

        with st.spinner("Executing..."):
            result = run_query(safe_sql)
            if not result.empty:
                display_result = resolve_codes(result) if resolve_codes_on else result
                st.dataframe(display_result, use_container_width=True, hide_index=True)
                st.caption(f"{len(result):,} rows returned")

                # Auto-chart
                date_cols = [c for c in result.columns if any(w in c.lower() for w in ("date", "year", "month", "quarter"))]
                num_cols = [c for c in result.columns if result[c].dtype in ("int64", "float64", "int32", "float32")]
                if date_cols and num_cols:
                    fig = go.Figure()
                    for i, nc in enumerate(num_cols[:3]):
                        fig.add_trace(go.Scatter(
                            x=result[date_cols[0]], y=result[nc],
                            name=nc, mode="lines+markers",
                            line=dict(width=2.5, color=CHART_COLORS[i % len(CHART_COLORS)]),
                        ))
                    fig.update_layout(height=380, template=PLOTLY_TEMPLATE)
                    st.plotly_chart(fig, use_container_width=True, key="explore_chart")
                elif len(result) <= 50 and num_cols:
                    str_cols = [c for c in result.columns if result[c].dtype == "object"]
                    if str_cols:
                        fig = go.Figure(go.Bar(
                            x=result[str_cols[0]], y=result[num_cols[0]],
                            marker=dict(color=ACCENT_BLUE, cornerradius=6),
                        ))
                        fig.update_layout(height=380, template=PLOTLY_TEMPLATE)
                        st.plotly_chart(fig, use_container_width=True, key="explore_bar")


# ===== TAB 6: AI Analyst =====================================================
with tab_ai:
    st.markdown('<p class="section-header">AI-Powered Analysis</p>', unsafe_allow_html=True)
    st.caption("Ask questions in plain English. The AI writes SQL, runs it, and explains the results.")

    # Initialize chat history
    if "ai_messages" not in st.session_state:
        st.session_state.ai_messages = []

    # Schema summary for AI
    @st.cache_data(ttl=3600)
    def get_schema_summary():
        tables = get_table_list()
        summary = []
        for t in tables[:25]:
            try:
                cols = con.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name='{t}'
                    ORDER BY ordinal_position
                """).fetchall()
                col_str = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
                summary.append(f"Table `{t}`: {col_str}")
            except Exception:
                pass
        return "\n".join(summary)

    schema = get_schema_summary()

    # Suggested questions
    suggestions = [
        "What is the asylum grant rate by year since 2015?",
        "Which courts have the highest denial rates?",
        "Top 10 nationalities with most cases in 2024",
        "Average wait time by court for completed cases",
        "In absentia rate trend by quarter since 2020",
        "Compare grant rates: New York vs Los Angeles",
    ]

    st.markdown("**Suggested Questions**")
    chip_cols = st.columns(3)
    for i, suggestion in enumerate(suggestions):
        with chip_cols[i % 3]:
            if st.button(suggestion, key=f"chip_{i}", use_container_width=True):
                st.session_state["ai_pending_question"] = suggestion

    st.markdown("")

    # Display chat history
    for msg in st.session_state.ai_messages:
        if msg["role"] == "user":
            st.markdown(f'<div class="chat-user">{msg["content"]}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="chat-ai">{msg["content"]}</div>', unsafe_allow_html=True)
            if "dataframe" in msg:
                st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)
            if "chart" in msg:
                st.plotly_chart(msg["chart"], use_container_width=True)

    # Chat input
    user_question = st.chat_input("Ask a question about EOIR data...")

    # Also check for chip-triggered question
    if "ai_pending_question" in st.session_state:
        user_question = st.session_state.pop("ai_pending_question")

    if user_question:
        st.session_state.ai_messages.append({"role": "user", "content": user_question})
        st.markdown(f'<div class="chat-user">{user_question}</div>', unsafe_allow_html=True)

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            err_msg = "ANTHROPIC_API_KEY not found in .env file."
            st.session_state.ai_messages.append({"role": "assistant", "content": err_msg})
            st.markdown(f'<div class="chat-ai">{err_msg}</div>', unsafe_allow_html=True)
        else:
            with st.spinner("Analyzing..."):
                try:
                    import anthropic
                    client = anthropic.Anthropic(api_key=api_key)

                    system_prompt = f"""You are a SQL analyst for immigration court (EOIR) data stored in DuckDB.

CRITICAL: All columns in the 6 main tables (a_tblcase, b_tblproceeding, tbl_schedule, tbl_court_appln, tbl_court_motions, b_tblproceedcharges) were imported as VARCHAR. You MUST use TRY_CAST() for date and number comparisons.

The user's current date type filter is: "{date_type}"
Date column mapping:
- "Completion Date" -> b_tblproceeding.COMP_DATE, tbl_schedule.ADJ_DATE, tbl_court_motions.COMP_DATE
- "Filing Date" -> b_tblproceeding.OSC_DATE, tbl_schedule.OSC_DATE, tbl_court_motions.OSC_DATE
- "Hearing Date" -> b_tblproceeding.HEARING_DATE, tbl_schedule.ADJ_DATE
Use the appropriate date column based on the user's selection when filtering by date.

Key tables and relationships:
- a_tblcase: Case master. PK: IDNCASE (BIGINT). Has NAT (nationality code), LANG (language code), CUSTODY, CASE_TYPE, C_BIRTHDATE (VARCHAR), Sex, DATE_OF_ENTRY (TIMESTAMP), LATEST_HEARING (TIMESTAMP), LPR.
- b_tblproceeding: Main proceedings table. PK: IDNPROCEEDING (VARCHAR). IDNCASE links to a_tblcase (VARCHAR — join with TRY_CAST(p.IDNCASE AS BIGINT) = c.IDNCASE). COMP_DATE (VARCHAR) is completion date, OSC_DATE (VARCHAR) is filing date, HEARING_DATE (VARCHAR). DEC_CODE (VARCHAR), IJ_CODE=judge, BASE_CITY_CODE=court, NAT, LANG, ABSENTIA, CASE_TYPE.
- tbl_schedule: Hearing schedule. IDNSCHEDULE PK. IDNPROCEEDING, IDNCASE (both VARCHAR). ADJ_DATE (VARCHAR)=hearing date, OSC_DATE (VARCHAR)=filing date, ADJ_RSN=adjournment reason, CAL_TYPE, IJ_CODE.
- tbl_court_appln: Applications filed. IDNPROCEEDING, IDNCASE. APPL_CODE=application type, APPL_DEC=decision.
- b_tblproceedcharges: Charges. IDNPROCEEDING, IDNCASE. CHARGE=charge code.
- tbl_court_motions: Motions. IDNPROCEEDING, IDNCASE. COMP_DATE, OSC_DATE, MOTION_RECD_DATE (all VARCHAR).
- tbl_repsassigned: Attorney assignments. IDNCASE.

Key lookup tables — ALWAYS JOIN these for human-readable output:
- tbllookupjudge: JUDGE_CODE -> JUDGE_NAME
- tbllookupbasecity: BASE_CITY_CODE -> BASE_CITY_NAME
- tbllookupnationality: NAT_CODE -> NAT_NAME
- tbllookupcourtdecision: strDecCode -> strDecDescription (also strCaseType for precise match)
- tbllanguage: strCode -> strDescription
- tbllookupcharges: strCode -> strCodeDescription
- tbladjournmentcodes: strcode -> strDesciption (typo in original data)
- tbllookup_appln: strcode -> strdescription
- tbllookuphloc: HEARING_LOC_CODE -> HEARING_LOC_NAME
- tbllookupcasetype: strCode -> strDescription
- tbllookupcustodystatus: strCode -> strDescription
- tbllookupcal_type: strCalTypeCode -> strCalTypeDescription
- tbllookupschedule_type: strCode -> strDescription
- tbllookupmotiontype: strMotionCode -> strMotionDesc
- tbllookupfiledby: strCode -> strDescription
- tbllookupappealtype: strApplCode -> strApplDescription
- tbllookupbiadecision: strCode -> strDescription
- tbllookupcourtappdecisions: strCourtApplnDecCode -> strCourtApplnDecDesc
- tbllookupnotice: Notice_Code -> Notice_Disp

Key relationships:
- IDNCASE links a_tblcase -> b_tblproceeding, tbl_schedule, etc.
- IDNPROCEEDING links b_tblproceeding -> tbl_schedule, tbl_court_motions, etc.
- To join b_tblproceeding to a_tblcase: TRY_CAST(p.IDNCASE AS BIGINT) = c.IDNCASE
- For grant rates, DEC_CODE IN ('G', 'A') for grants and ('D', 'R', 'X') for denials.

Full schema:
{schema}

Rules:
1. Write DuckDB-compatible SQL wrapped in ```sql ... ``` code blocks.
2. ALWAYS JOIN lookup tables for human-readable names — never show raw codes to the user. Use COALESCE(lookup.name, raw_code) as a fallback.
3. ALWAYS use TRY_CAST for date/number columns.
4. Keep results concise (LIMIT 1000 max).
5. After SQL, briefly explain what the query does and key findings to look for.
6. For decision lookups: p.DEC_CODE = d.strDecCode (optionally AND p.CASE_TYPE = d.strCaseType).
7. For nationality lookups: c.NAT = n.NAT_CODE (where c is a_tblcase)."""

                    # Build messages with history (last 10 messages for context)
                    history = st.session_state.ai_messages[-10:]
                    messages = [{"role": m["role"], "content": m["content"]} for m in history]

                    response = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=2000,
                        system=system_prompt,
                        messages=messages,
                    )

                    ai_text = response.content[0].text
                    msg_data = {"role": "assistant", "content": ai_text}

                    st.markdown(f'<div class="chat-ai">{ai_text}</div>', unsafe_allow_html=True)

                    # Extract and run SQL
                    sql_match = re.search(r"```sql\n(.*?)```", ai_text, re.DOTALL)
                    if sql_match:
                        extracted_sql = sql_match.group(1).strip()
                        # Cap at 1000 rows
                        if "limit" not in extracted_sql.lower():
                            extracted_sql += "\nLIMIT 1000"

                        with st.spinner("Running query..."):
                            result = run_query(extracted_sql)
                            if not result.empty:
                                st.dataframe(result, use_container_width=True, hide_index=True)
                                st.caption(f"{len(result):,} rows")
                                msg_data["dataframe"] = result

                                # Auto-chart
                                date_cols = [c for c in result.columns if any(w in c.lower() for w in ("date", "year", "month", "quarter"))]
                                num_cols = [c for c in result.columns if result[c].dtype in ("int64", "float64")]
                                if date_cols and num_cols:
                                    fig = go.Figure()
                                    for i, nc in enumerate(num_cols[:3]):
                                        fig.add_trace(go.Scatter(
                                            x=result[date_cols[0]], y=result[nc],
                                            name=nc, mode="lines+markers",
                                            line=dict(width=2.5, color=CHART_COLORS[i % len(CHART_COLORS)]),
                                        ))
                                    fig.update_layout(height=380, template=PLOTLY_TEMPLATE)
                                    st.plotly_chart(fig, use_container_width=True)
                                    msg_data["chart"] = fig
                                elif len(result) <= 30 and num_cols:
                                    str_cols = [c for c in result.columns if result[c].dtype == "object"]
                                    if str_cols:
                                        fig = go.Figure(go.Bar(
                                            x=result[str_cols[0]], y=result[num_cols[0]],
                                            marker=dict(color=ACCENT_BLUE, cornerradius=6),
                                        ))
                                        fig.update_layout(height=380, template=PLOTLY_TEMPLATE)
                                        st.plotly_chart(fig, use_container_width=True)
                                        msg_data["chart"] = fig

                    st.session_state.ai_messages.append(msg_data)

                except Exception as e:
                    err_msg = f"Error: {e}"
                    st.session_state.ai_messages.append({"role": "assistant", "content": err_msg})
                    st.error(err_msg)

    # Clear chat button
    if st.session_state.ai_messages:
        if st.button("Clear Conversation", key="clear_chat"):
            st.session_state.ai_messages = []
            st.rerun()
