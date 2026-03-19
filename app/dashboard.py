"""EOIR Analytics Platform — Immigration Court Intelligence.

Port 8519. Investor-grade data visualization platform over 160M+ rows
of EOIR immigration court data in DuckDB.

New UX: sidebar table/field navigator with popout filters in main area.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, timedelta
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

/* -- Hide Streamlit chrome -- */
#MainMenu, footer, div[data-testid="stToolbar"],
header[data-testid="stHeader"] {{ display: none !important; }}

/* -- Global -- */
.stApp {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: {BG};
}}

/* -- Dark sidebar -- */
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

/* -- KPI metric cards -- */
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

/* -- Tab styling -- */
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

/* -- Card wrapper -- */
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

/* -- Hero header -- */
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

/* -- Section headers -- */
.section-header {{
    font-size: 1.1rem;
    font-weight: 700;
    color: {TEXT_PRIMARY};
    margin: 8px 0 16px 0;
    letter-spacing: -0.01em;
}}

/* -- Data tables -- */
.stDataFrame {{
    border-radius: 12px !important;
    overflow: hidden !important;
    border: 1px solid #E2E8F0 !important;
}}

/* -- Buttons -- */
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

/* -- Chat messages (AI tab) -- */
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

/* -- Plotly chart containers -- */
.stPlotlyChart {{
    border-radius: 12px;
    overflow: hidden;
}}

/* -- Sidebar filter/field buttons -- */
section[data-testid="stSidebar"] .stButton > button {{
    background: rgba(255,255,255,0.06) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 8px !important;
    color: #CBD5E1 !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    transition: all 0.15s ease !important;
}}
section[data-testid="stSidebar"] .stButton > button:hover {{
    background: rgba(255,255,255,0.12) !important;
    border-color: rgba(255,255,255,0.2) !important;
}}

/* -- Reduce default spacing -- */
.block-container {{
    padding-top: 1rem !important;
    padding-bottom: 1rem !important;
    max-width: 100% !important;
}}

/* -- Filter popout card -- */
.filter-popout {{
    position: relative;
    background: white;
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 16px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.12);
    border: 1px solid #E2E8F0;
    animation: slideDown 0.2s ease;
}}
@keyframes slideDown {{
    from {{ opacity: 0; transform: translateY(-8px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}

/* -- Active filter pills -- */
.filter-pills {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 12px 0;
    align-items: center;
}}
.filter-pill {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #EFF6FF;
    border: 1px solid #BFDBFE;
    color: #1E40AF;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 500;
}}
.filter-pill-remove {{
    cursor: pointer;
    opacity: 0.7;
    font-size: 0.9rem;
}}
.filter-pill-remove:hover {{ opacity: 1; }}
.filter-pills-label {{
    font-size: 0.75rem;
    font-weight: 600;
    color: {TEXT_SECONDARY};
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-right: 4px;
}}

/* -- Sidebar table & field tree styling -- */
.sidebar-section-label {{
    font-size: 0.68rem;
    color: #94A3B8;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 12px 0 8px 0;
}}
.sidebar-table-row {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 2px 0;
}}
.sidebar-table-label {{
    font-size: 0.85rem;
    font-weight: 600;
    color: #E2E8F0;
}}
.sidebar-table-count {{
    font-size: 0.68rem;
    color: #64748B;
    font-weight: 500;
}}
.sidebar-field-row {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1px 0 1px 16px;
    font-size: 0.78rem;
    color: #94A3B8;
}}
.sidebar-field-name {{
    font-weight: 400;
}}
.sidebar-field-icon {{
    font-size: 0.72rem;
    opacity: 0.7;
}}

/* -- Admin mode hidden field strikethrough -- */
.field-hidden {{
    text-decoration: line-through;
    opacity: 0.4;
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


@st.cache_data(ttl=3600)
def get_table_row_count(table_name: str) -> int:
    """Get approximate row count for a table."""
    con = get_db()
    if con is None:
        return 0
    try:
        result = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
        return result[0] if result else 0
    except Exception:
        return 0


@st.cache_data(ttl=3600)
def get_table_columns(table_name: str) -> list[dict]:
    """Get column names and types for a table."""
    con = get_db()
    if con is None:
        return []
    try:
        rows = con.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name='{table_name}' ORDER BY ordinal_position"
        ).fetchall()
        return [{"name": r[0], "type": r[1]} for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=600)
def get_field_value_counts(table_name: str, field_name: str, lookup_key: str | None = None) -> list[tuple[str, str, int]]:
    """Get (code, display_name, count) for a field, ordered by count desc. Max 500."""
    con = get_db()
    if con is None:
        return []
    try:
        rows = con.execute(
            f'SELECT "{field_name}", COUNT(*) as cnt '
            f'FROM "{table_name}" '
            f'WHERE "{field_name}" IS NOT NULL '
            f'GROUP BY "{field_name}" '
            f'ORDER BY cnt DESC LIMIT 500'
        ).fetchall()
        lookup = LOOKUPS.get(lookup_key, {}) if lookup_key else {}
        result = []
        for r in rows:
            code = str(r[0]).strip()
            display = lookup.get(code, code) if lookup else code
            result.append((code, display, r[1]))
        return result
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Data Model Configuration
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "dashboard_config.json"

# Table metadata — the core data model definition
TABLE_META = {
    "a_tblcase": {
        "label": "Cases",
        "description": "Case master records with demographics, nationality, language, custody",
        "alias": "c",
    },
    "b_tblproceeding": {
        "label": "Proceedings",
        "description": "Court proceedings, decisions, judges, completion dates",
        "alias": "p",
    },
    "tbl_schedule": {
        "label": "Hearings",
        "description": "Scheduled hearings, adjournments, calendar types",
        "alias": "s",
    },
    "tbl_court_appln": {
        "label": "Applications",
        "description": "Filed applications (asylum, withholding, CAT, etc.)",
        "alias": "ap",
    },
    "b_tblproceedcharges": {
        "label": "Charges",
        "description": "Immigration charges filed against respondents",
        "alias": "ch",
    },
    "tbl_court_motions": {
        "label": "Motions",
        "description": "Court motions filed and decided",
        "alias": "m",
    },
    "d_tblassociatedbond": {
        "label": "Bonds",
        "description": "Bond hearings and decisions",
        "alias": "b",
    },
    "tbl_repsassigned": {
        "label": "Attorneys",
        "description": "Attorney/representative assignments",
        "alias": "r",
    },
    "tbl_custodyhistory": {
        "label": "Custody History",
        "description": "Detention and release history",
        "alias": "cu",
    },
    "tblappeal": {
        "label": "Appeals",
        "description": "BIA appeals filed and decided",
        "alias": "a",
    },
}

# Comprehensive field metadata for every main table
FIELD_META = {
    "a_tblcase": {
        "NAT": {"label": "Nationality", "type": "lookup", "lookup": "nationality",
                "lookup_table": "tbllookupnationality", "code_col": "NAT_CODE", "desc_col": "NAT_NAME",
                "filter_candidate": True},
        "LANG": {"label": "Language", "type": "lookup", "lookup": "language",
                 "lookup_table": "tbllanguage", "code_col": "strCode", "desc_col": "strDescription",
                 "filter_candidate": True},
        "CUSTODY": {"label": "Custody Status", "type": "lookup", "lookup": "custody",
                    "lookup_table": "tbllookupcustodystatus", "code_col": "strCode", "desc_col": "strDescription",
                    "filter_candidate": True},
        "Sex": {"label": "Gender", "type": "text", "filter_candidate": True},
        "CASE_TYPE": {"label": "Case Type", "type": "lookup", "lookup": "case_type",
                      "lookup_table": "tbllookupcasetype", "code_col": "strCode", "desc_col": "strDescription",
                      "filter_candidate": True},
        "LPR": {"label": "LPR Status", "type": "boolean", "filter_candidate": True},
        "ALIEN_STATE": {"label": "Respondent State", "type": "text", "filter_candidate": True},
        "ALIEN_CITY": {"label": "Respondent City", "type": "text", "filter_candidate": False},
        "SITE_TYPE": {"label": "Site Type", "type": "text", "filter_candidate": True},
        "DATE_OF_ENTRY": {"label": "Date of US Entry", "type": "date", "filter_candidate": False},
        "C_BIRTHDATE": {"label": "Date of Birth", "type": "date", "filter_candidate": False},
        "E_28_DATE": {"label": "E-28 Date", "type": "date", "filter_candidate": False},
        "CASEPRIORITY_CODE": {"label": "Case Priority", "type": "text", "filter_candidate": True},
        "CORRECTIONAL_FAC": {"label": "Correctional Facility", "type": "text", "filter_candidate": False},
    },
    "b_tblproceeding": {
        "BASE_CITY_CODE": {"label": "Immigration Court", "type": "lookup", "lookup": "base_city",
                           "lookup_table": "tbllookupbasecity", "code_col": "BASE_CITY_CODE", "desc_col": "BASE_CITY_NAME",
                           "filter_candidate": True},
        "HEARING_LOC_CODE": {"label": "Hearing Location", "type": "lookup", "lookup": "hearing_loc",
                             "lookup_table": "tbllookuphloc", "code_col": "HEARING_LOC_CODE", "desc_col": "HEARING_LOC_NAME",
                             "filter_candidate": True},
        "IJ_CODE": {"label": "Immigration Judge", "type": "lookup", "lookup": "judge",
                     "lookup_table": "tbllookupjudge", "code_col": "JUDGE_CODE", "desc_col": "JUDGE_NAME",
                     "filter_candidate": True},
        "DEC_CODE": {"label": "Decision", "type": "lookup", "lookup": "decision",
                     "lookup_table": "tbllookupcourtdecision", "code_col": "strDecCode", "desc_col": "strDecDescription",
                     "filter_candidate": True},
        "DEC_TYPE": {"label": "Decision Type", "type": "text", "filter_candidate": True},
        "CASE_TYPE": {"label": "Case Type", "type": "lookup", "lookup": "case_type",
                      "lookup_table": "tbllookupcasetype", "code_col": "strCode", "desc_col": "strDescription",
                      "filter_candidate": True},
        "NAT": {"label": "Nationality", "type": "lookup", "lookup": "nationality",
                "lookup_table": "tbllookupnationality", "code_col": "NAT_CODE", "desc_col": "NAT_NAME",
                "filter_candidate": True},
        "LANG": {"label": "Language", "type": "lookup", "lookup": "language",
                 "lookup_table": "tbllanguage", "code_col": "strCode", "desc_col": "strDescription",
                 "filter_candidate": True},
        "CUSTODY": {"label": "Custody", "type": "lookup", "lookup": "custody",
                    "lookup_table": "tbllookupcustodystatus", "code_col": "strCode", "desc_col": "strDescription",
                    "filter_candidate": True},
        "ABSENTIA": {"label": "In Absentia", "type": "boolean", "filter_candidate": True},
        "CRIM_IND": {"label": "Criminal Indicator", "type": "boolean", "filter_candidate": True},
        "IHP": {"label": "Institutional Hearing", "type": "boolean", "filter_candidate": True},
        "AGGRAVATE_FELON": {"label": "Aggravated Felon", "type": "boolean", "filter_candidate": True},
        "COMP_DATE": {"label": "Completion Date", "type": "date", "filter_candidate": True},
        "OSC_DATE": {"label": "Filing Date (OSC)", "type": "date", "filter_candidate": True},
        "HEARING_DATE": {"label": "Last Hearing Date", "type": "date", "filter_candidate": True},
    },
    "tbl_schedule": {
        "ADJ_DATE": {"label": "Hearing Date", "type": "date", "filter_candidate": True},
        "ADJ_RSN": {"label": "Adjournment Reason", "type": "lookup", "lookup": "adjournment",
                    "lookup_table": "tbladjournmentcodes", "code_col": "strcode", "desc_col": "strDesciption",
                    "filter_candidate": True},
        "CAL_TYPE": {"label": "Calendar Type", "type": "lookup", "lookup": "cal_type",
                     "lookup_table": "tbllookupcal_type", "code_col": "strCalTypeCode", "desc_col": "strCalTypeDescription",
                     "filter_candidate": True},
        "SCHEDULE_TYPE": {"label": "Schedule Type", "type": "lookup", "lookup": "schedule_type",
                          "lookup_table": "tbllookupschedule_type", "code_col": "strCode", "desc_col": "strDescription",
                          "filter_candidate": True},
        "IJ_CODE": {"label": "Judge", "type": "lookup", "lookup": "judge",
                     "lookup_table": "tbllookupjudge", "code_col": "JUDGE_CODE", "desc_col": "JUDGE_NAME",
                     "filter_candidate": True},
        "BASE_CITY_CODE": {"label": "Court", "type": "lookup", "lookup": "base_city",
                           "lookup_table": "tbllookupbasecity", "code_col": "BASE_CITY_CODE", "desc_col": "BASE_CITY_NAME",
                           "filter_candidate": True},
        "HEARING_LOC_CODE": {"label": "Hearing Location", "type": "lookup", "lookup": "hearing_loc",
                             "lookup_table": "tbllookuphloc", "code_col": "HEARING_LOC_CODE", "desc_col": "HEARING_LOC_NAME",
                             "filter_candidate": True},
        "NOTICE_CODE": {"label": "Notice Code", "type": "text", "filter_candidate": True},
        "ADJ_MEDIUM": {"label": "Hearing Medium", "type": "text", "filter_candidate": True},
    },
    "tbl_court_appln": {
        "APPL_CODE": {"label": "Application Type", "type": "lookup", "lookup": "application",
                      "lookup_table": "tbllookup_appln", "code_col": "strcode", "desc_col": "strdescription",
                      "filter_candidate": True},
        "APPL_DEC": {"label": "Application Decision", "type": "lookup", "lookup": "court_app_dec",
                     "lookup_table": "tbllookupcourtappdecisions", "code_col": "strCourtApplnDecCode", "desc_col": "strCourtApplnDecDesc",
                     "filter_candidate": True},
        "APPL_RECD_DATE": {"label": "Application Received Date", "type": "date", "filter_candidate": False},
    },
    "b_tblproceedcharges": {
        "CHARGE": {"label": "Charge", "type": "lookup", "lookup": "charge",
                   "lookup_table": "tbllookupcharges", "code_col": "strCode", "desc_col": "strCodeDescription",
                   "filter_candidate": True},
        "CHG_STATUS": {"label": "Charge Status", "type": "text", "filter_candidate": True},
    },
    "tbl_court_motions": {
        "COMP_DATE": {"label": "Motion Decision Date", "type": "date", "filter_candidate": False},
        "MOTION_RECD_DATE": {"label": "Motion Filed Date", "type": "date", "filter_candidate": False},
        "DEC": {"label": "Motion Decision", "type": "text", "filter_candidate": True},
        "STRFILINGPARTY": {"label": "Filed By", "type": "lookup", "lookup": "filed_by",
                           "lookup_table": "tbllookupfiledby", "code_col": "strCode", "desc_col": "strDescription",
                           "filter_candidate": True},
        "STRFILINGMETHOD": {"label": "Filing Method", "type": "text", "filter_candidate": True},
    },
    "d_tblassociatedbond": {
        "DEC": {"label": "Bond Decision", "type": "text", "filter_candidate": True},
        "INITIAL_BOND": {"label": "Initial Bond Amount", "type": "number", "filter_candidate": False},
        "BASE_CITY_CODE": {"label": "Court", "type": "lookup", "lookup": "base_city",
                           "lookup_table": "tbllookupbasecity", "code_col": "BASE_CITY_CODE", "desc_col": "BASE_CITY_NAME",
                           "filter_candidate": True},
        "IJ_CODE": {"label": "Judge", "type": "lookup", "lookup": "judge",
                     "lookup_table": "tbllookupjudge", "code_col": "JUDGE_CODE", "desc_col": "JUDGE_NAME",
                     "filter_candidate": True},
        "COMP_DATE": {"label": "Bond Decision Date", "type": "date", "filter_candidate": False},
    },
    "tbl_repsassigned": {
        "STRATTYLEVEL": {"label": "Attorney Level", "type": "text", "filter_candidate": True},
        "STRATTYTYPE": {"label": "Attorney Type", "type": "text", "filter_candidate": True},
        "BASE_CITY_CODE": {"label": "Court", "type": "lookup", "lookup": "base_city",
                           "lookup_table": "tbllookupbasecity", "code_col": "BASE_CITY_CODE", "desc_col": "BASE_CITY_NAME",
                           "filter_candidate": True},
        "E_27_DATE": {"label": "E-27 Date", "type": "date", "filter_candidate": False},
        "E_28_DATE": {"label": "E-28 Date", "type": "date", "filter_candidate": False},
    },
    "tbl_custodyhistory": {
        # Minimal fields — table details TBD
    },
    "tblappeal": {
        "strAppealType": {"label": "Appeal Type", "type": "text", "filter_candidate": True},
        "strBIADecision": {"label": "BIA Decision", "type": "lookup", "lookup": "bia_decision",
                           "lookup_table": "tbllookupbiadecision", "code_col": "strCode", "desc_col": "strDescription",
                           "filter_candidate": True},
        "strBIADecisionType": {"label": "BIA Decision Type", "type": "text", "filter_candidate": True},
        "datAppealFiled": {"label": "Appeal Filed Date", "type": "date", "filter_candidate": False},
        "datBIADecision": {"label": "BIA Decision Date", "type": "date", "filter_candidate": False},
        "strFiledBy": {"label": "Filed By", "type": "text", "filter_candidate": True},
    },
}

# JOIN relationships: how each table joins to b_tblproceeding (base)
TABLE_JOINS = {
    "a_tblcase": "JOIN a_tblcase {alias} ON TRY_CAST({base}.IDNCASE AS BIGINT) = {alias}.IDNCASE",
    "tbl_schedule": "JOIN tbl_schedule {alias} ON {base}.IDNPROCEEDING = {alias}.IDNPROCEEDING AND {base}.IDNCASE = {alias}.IDNCASE",
    "tbl_court_appln": "JOIN tbl_court_appln {alias} ON {base}.IDNPROCEEDING = {alias}.IDNPROCEEDING AND {base}.IDNCASE = {alias}.IDNCASE",
    "b_tblproceedcharges": "JOIN b_tblproceedcharges {alias} ON {base}.IDNPROCEEDING = {alias}.IDNPROCEEDING AND {base}.IDNCASE = {alias}.IDNCASE",
    "tbl_court_motions": "JOIN tbl_court_motions {alias} ON {base}.IDNPROCEEDING = {alias}.IDNPROCEEDING AND {base}.IDNCASE = {alias}.IDNCASE",
    "d_tblassociatedbond": "JOIN d_tblassociatedbond {alias} ON {base}.IDNPROCEEDING = {alias}.IDNPROCEEDING AND {base}.IDNCASE = {alias}.IDNCASE",
    "tbl_repsassigned": "JOIN tbl_repsassigned {alias} ON {base}.IDNCASE = {alias}.IDNCASE",
    "tbl_custodyhistory": "JOIN tbl_custodyhistory {alias} ON {base}.IDNCASE = {alias}.IDNCASE",
    "tblappeal": "JOIN tblappeal {alias} ON {base}.IDNPROCEEDING = {alias}.idnProceeding AND {base}.IDNCASE = {alias}.idncase",
}

# Default configuration
_DEFAULT_CONFIG = {
    "active_tables": ["a_tblcase", "b_tblproceeding"],
    "hidden_fields": {},
}


def _load_config() -> dict:
    """Load dashboard config from JSON, creating defaults if missing."""
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                cfg = json.load(f)
                if "active_tables" in cfg:
                    # Migrate from old format if needed
                    if "hidden_fields" not in cfg:
                        cfg["hidden_fields"] = {}
                    return cfg
        except (json.JSONDecodeError, KeyError):
            pass
    # Write defaults
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(_DEFAULT_CONFIG, f, indent=2)
    return _DEFAULT_CONFIG.copy()


def _save_config(config: dict):
    """Persist dashboard config to JSON."""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


# ---------------------------------------------------------------------------
# Session state initialization
# ---------------------------------------------------------------------------

if "dashboard_config" not in st.session_state:
    st.session_state.dashboard_config = _load_config()

if "active_tables" not in st.session_state:
    st.session_state.active_tables = list(st.session_state.dashboard_config.get(
        "active_tables", _DEFAULT_CONFIG["active_tables"]))

if "active_popout" not in st.session_state:
    st.session_state.active_popout = None  # e.g. "a_tblcase.NAT"

if "filters" not in st.session_state:
    st.session_state.filters = {}  # e.g. {"a_tblcase.NAT": ["MX","GT"], "b_tblproceeding.COMP_DATE": {"from":"2020-01-01","to":"2026-03-18"}}

if "admin_mode" not in st.session_state:
    st.session_state.admin_mode = False


def _get_hidden_fields(table_name: str) -> list[str]:
    return st.session_state.dashboard_config.get("hidden_fields", {}).get(table_name, [])


def _is_field_hidden(table_name: str, field_name: str) -> bool:
    return field_name in _get_hidden_fields(table_name)


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


def _table_required_message(table_label: str) -> None:
    """Show a message when a required table is not active."""
    st.info(
        f"Enable the **{table_label}** table in the sidebar to see this analysis.",
        icon="\u2139\ufe0f",
    )


def _is_table_active(table_name: str) -> bool:
    return table_name in st.session_state.active_tables


# ---------------------------------------------------------------------------
# Database check
# ---------------------------------------------------------------------------
con = get_db()
if con is None:
    st.error("Database not found. Run the import script first.")
    st.stop()


# ---------------------------------------------------------------------------
# Sidebar — Table & Field Navigator
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

st.sidebar.markdown('<div class="sidebar-section-label">DATA SOURCES</div>', unsafe_allow_html=True)

# Render each table with checkbox and fields
_row_count_cache = {}
for table_name, meta in TABLE_META.items():
    if table_name not in _row_count_cache:
        _row_count_cache[table_name] = get_table_row_count(table_name)
    row_count = _row_count_cache[table_name]

    is_active = table_name in st.session_state.active_tables
    label_text = f"{meta['label']} -- {_fmt_number(row_count)} rows"

    new_active = st.sidebar.checkbox(
        label_text,
        value=is_active,
        key=f"sb_table_{table_name}",
    )

    # Handle table toggle change
    if new_active != is_active:
        if new_active:
            st.session_state.active_tables.append(table_name)
        else:
            st.session_state.active_tables.remove(table_name)
            # Close any popout from this table
            if st.session_state.active_popout and st.session_state.active_popout.startswith(f"{table_name}."):
                st.session_state.active_popout = None
            # Remove filters for this table
            keys_to_remove = [k for k in st.session_state.filters if k.startswith(f"{table_name}.")]
            for k in keys_to_remove:
                del st.session_state.filters[k]
        # Persist active tables to config
        st.session_state.dashboard_config["active_tables"] = list(st.session_state.active_tables)
        _save_config(st.session_state.dashboard_config)
        st.rerun()

    # Show fields if table is active
    if new_active:
        fields = FIELD_META.get(table_name, {})
        hidden_fields = _get_hidden_fields(table_name)

        for field_name, field_meta in fields.items():
            # Skip hidden fields (unless admin mode)
            if field_name in hidden_fields and not st.session_state.admin_mode:
                continue

            ftype = field_meta.get("type", "text")
            label = field_meta.get("label", field_name)
            is_filterable = field_meta.get("filter_candidate", False)
            is_date = ftype == "date" and is_filterable

            # Build the sidebar row
            filter_key = f"{table_name}.{field_name}"
            has_active_filter = filter_key in st.session_state.filters

            # Admin mode: show hide button
            if st.session_state.admin_mode:
                is_hidden = field_name in hidden_fields
                admin_cols = st.sidebar.columns([5, 1])
                with admin_cols[0]:
                    if is_filterable or is_date:
                        icon = "📅" if is_date else "🔍"
                        btn_label = f"  {icon} {label}" + (" *" if has_active_filter else "")
                        if is_hidden:
                            btn_label = f"  ~{label}~ (hidden)"
                        if st.button(btn_label, key=f"sb_field_{table_name}_{field_name}",
                                     use_container_width=True):
                            if not is_hidden:
                                st.session_state.active_popout = filter_key
                                st.rerun()
                    else:
                        display_label = f"  {label}" + (" (hidden)" if is_hidden else "")
                        st.sidebar.markdown(
                            f'<div class="sidebar-field-row{"  field-hidden" if is_hidden else ""}">'
                            f'<span class="sidebar-field-name">{display_label}</span></div>',
                            unsafe_allow_html=True)
                with admin_cols[1]:
                    if is_hidden:
                        if st.button("+", key=f"sb_unhide_{table_name}_{field_name}"):
                            hidden_fields.remove(field_name)
                            st.session_state.dashboard_config.setdefault("hidden_fields", {})[table_name] = hidden_fields
                            _save_config(st.session_state.dashboard_config)
                            st.rerun()
                    else:
                        if st.button("x", key=f"sb_hide_{table_name}_{field_name}"):
                            hidden_fields.append(field_name)
                            st.session_state.dashboard_config.setdefault("hidden_fields", {})[table_name] = hidden_fields
                            _save_config(st.session_state.dashboard_config)
                            st.rerun()
            else:
                # Normal mode
                if is_filterable or is_date:
                    icon = "📅" if is_date else "🔍"
                    btn_label = f"  {icon} {label}" + (" *" if has_active_filter else "")
                    if st.sidebar.button(btn_label, key=f"sb_field_{table_name}_{field_name}",
                                         use_container_width=True):
                        # Toggle popout
                        if st.session_state.active_popout == filter_key:
                            st.session_state.active_popout = None
                        else:
                            st.session_state.active_popout = filter_key
                        st.rerun()
                else:
                    st.sidebar.markdown(
                        f'<div class="sidebar-field-row">'
                        f'<span class="sidebar-field-name">  {label}</span></div>',
                        unsafe_allow_html=True)


st.sidebar.divider()

# -- Admin toggle at bottom of sidebar --
admin_on = st.sidebar.toggle("Admin Mode", value=st.session_state.admin_mode, key="sb_admin_toggle")
if admin_on != st.session_state.admin_mode:
    st.session_state.admin_mode = admin_on
    st.rerun()

# -- Filter summary in sidebar --
active_filter_count = len(st.session_state.filters)
st.sidebar.markdown(
    '<div style="text-align:center; font-size:0.65rem; color:#475569; padding:8px 0;">'
    f'Active filters: {active_filter_count}<br>'
    '160M+ records &middot; 89 tables &middot; DuckDB'
    '</div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Dynamic WHERE clause + FROM builder
# ---------------------------------------------------------------------------

# Map date_type to the correct column per table
_DATE_COL_MAP = {
    "Completion Date": {"proc": "COMP_DATE", "sched": "ADJ_DATE", "motion": "COMP_DATE"},
    "Filing Date": {"proc": "OSC_DATE", "sched": "OSC_DATE", "motion": "OSC_DATE"},
    "Hearing Date": {"proc": "HEARING_DATE", "sched": "ADJ_DATE", "motion": "COMP_DATE"},
}


def build_where(
    base_alias: str = "p",
    date_col: str | None = None,
    table_type: str = "proc",
) -> tuple[str, set[str]]:
    """Build SQL WHERE clause from all active filter selections in session state.

    table_type: "proc" | "sched" | "motion" -- selects the right date column.
    Returns (where_clause, needs_tables) where needs_tables is a set of
    extra table names that must be JOINed.
    """
    conditions: list[str] = []
    needs_tables: set[str] = set()

    # Check if there's a date filter that should act as the main date constraint
    # Find date filters from session state
    date_constraints_applied = False

    for filter_key, filter_value in st.session_state.filters.items():
        table_name, field_name = filter_key.split(".", 1)
        field_meta = FIELD_META.get(table_name, {}).get(field_name, {})

        if field_meta.get("type") == "date" and isinstance(filter_value, dict):
            # Date range filter
            alias_map = {t: m["alias"] for t, m in TABLE_META.items()}
            if table_name == "b_tblproceeding":
                alias = base_alias if table_type == "proc" else alias_map.get(table_name, base_alias)
            elif table_name == "tbl_schedule":
                alias = base_alias if table_type == "sched" else alias_map.get(table_name, base_alias)
            else:
                alias = alias_map.get(table_name, base_alias)

            # Only add join if the table isn't the base
            base_table_map = {"proc": "b_tblproceeding", "sched": "tbl_schedule", "motion": "tbl_court_motions"}
            if table_name != base_table_map.get(table_type):
                needs_tables.add(table_name)

            date_from = filter_value.get("from", "2000-01-01")
            date_to = filter_value.get("to", date.today().strftime("%Y-%m-%d"))
            conditions.append(f'TRY_CAST({alias}."{field_name}" AS TIMESTAMP) >= \'{date_from}\'')
            conditions.append(f'TRY_CAST({alias}."{field_name}" AS TIMESTAMP) <= \'{date_to}\'')
            conditions.append(f'{alias}."{field_name}" IS NOT NULL')
            date_constraints_applied = True

        elif field_meta.get("type") != "date" and isinstance(filter_value, list) and filter_value:
            # Categorical filter
            alias_map = {t: m["alias"] for t, m in TABLE_META.items()}
            base_table_map = {"proc": "b_tblproceeding", "sched": "tbl_schedule", "motion": "tbl_court_motions"}
            if table_name == base_table_map.get(table_type):
                alias = base_alias
            elif table_name in alias_map:
                needs_tables.add(table_name)
                alias = alias_map[table_name]
            else:
                alias = base_alias

            quoted = ", ".join(f"'{v}'" for v in filter_value)
            conditions.append(f'{alias}."{field_name}" IN ({quoted})')

    # If no date filter was explicitly applied, add a default date constraint
    if not date_constraints_applied:
        if date_col is None:
            date_col = _DATE_COL_MAP["Completion Date"].get(table_type, "COMP_DATE")
        # Default: last 5 years
        default_from = (date.today() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        default_to = date.today().strftime("%Y-%m-%d")
        conditions.append(f'TRY_CAST({base_alias}."{date_col}" AS TIMESTAMP) >= \'{default_from}\'')
        conditions.append(f'TRY_CAST({base_alias}."{date_col}" AS TIMESTAMP) <= \'{default_to}\'')
        conditions.append(f'{base_alias}."{date_col}" IS NOT NULL')

    if not conditions:
        return "1=1", needs_tables
    return " AND ".join(conditions), needs_tables


def build_from(
    base_table: str,
    base_alias: str,
    needs_tables: set[str],
) -> str:
    """Build FROM clause with necessary JOINs for the active filters."""
    sql = f"{base_table} {base_alias}"
    for table_name in needs_tables:
        if table_name == base_table:
            continue
        join_template = TABLE_JOINS.get(table_name)
        if join_template:
            alias = TABLE_META[table_name]["alias"]
            sql += "\n        " + join_template.format(alias=alias, base=base_alias)
    return sql


def _proc_from(base_alias: str = "p", case_alias: str = "c", needs_tables: set[str] | None = None) -> str:
    """Convenience: build FROM for b_tblproceeding queries."""
    tables = needs_tables or set()
    return build_from("b_tblproceeding", base_alias, tables)


def _sched_from(base_alias: str = "s", case_alias: str = "c", needs_tables: set[str] | None = None) -> str:
    """Convenience: build FROM for tbl_schedule queries."""
    tables = needs_tables or set()
    return build_from("tbl_schedule", base_alias, tables)


# Derive the active date column for proceeding queries (used in charts)
# Find the first date filter on b_tblproceeding, or default to COMP_DATE
_proc_date = "COMP_DATE"
for _fk, _fv in st.session_state.filters.items():
    if _fk.startswith("b_tblproceeding.") and isinstance(_fv, dict):
        _proc_date = _fk.split(".", 1)[1]
        break

_sched_date = "ADJ_DATE"
for _fk, _fv in st.session_state.filters.items():
    if _fk.startswith("tbl_schedule.") and isinstance(_fv, dict):
        _sched_date = _fk.split(".", 1)[1]
        break


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
# Filter Popout (above tabs, in main area)
# ---------------------------------------------------------------------------

def _render_filter_popout():
    """Render the filter popout card if one is active."""
    popout_key = st.session_state.active_popout
    if not popout_key:
        return

    parts = popout_key.split(".", 1)
    if len(parts) != 2:
        return
    table_name, field_name = parts
    field_meta = FIELD_META.get(table_name, {}).get(field_name, {})
    if not field_meta:
        st.session_state.active_popout = None
        return

    label = field_meta.get("label", field_name)
    ftype = field_meta.get("type", "text")
    is_date = ftype == "date"

    st.markdown('<div class="filter-popout">', unsafe_allow_html=True)

    if is_date:
        # ----- DATE RANGE POPOUT -----
        header_cols = st.columns([6, 1])
        with header_cols[0]:
            st.markdown(f"**📅 {label}**")
        with header_cols[1]:
            if st.button("Close", key="popout_close"):
                st.session_state.active_popout = None
                st.rerun()

        # Get current date filter values
        current = st.session_state.filters.get(popout_key, {})
        current_from = current.get("from", "2020-01-01") if isinstance(current, dict) else "2020-01-01"
        current_to = current.get("to", date.today().strftime("%Y-%m-%d")) if isinstance(current, dict) else date.today().strftime("%Y-%m-%d")

        try:
            from_val = datetime.strptime(current_from, "%Y-%m-%d").date()
        except ValueError:
            from_val = date(2020, 1, 1)
        try:
            to_val = datetime.strptime(current_to, "%Y-%m-%d").date()
        except ValueError:
            to_val = date.today()

        d_cols = st.columns([2, 2, 3])
        with d_cols[0]:
            new_from = st.date_input("From", value=from_val, key=f"popout_date_from_{popout_key}")
        with d_cols[1]:
            new_to = st.date_input("To", value=to_val, key=f"popout_date_to_{popout_key}")

        # Quick presets
        with d_cols[2]:
            st.markdown("**Quick Range**")
            qc1, qc2, qc3 = st.columns(3)
            with qc1:
                if st.button("Last Year", key=f"popout_q1_{popout_key}"):
                    new_from = date(date.today().year - 1, 1, 1)
                    new_to = date.today()
            with qc2:
                if st.button("Last 5 Years", key=f"popout_q5_{popout_key}"):
                    new_from = date(date.today().year - 5, 1, 1)
                    new_to = date.today()
            with qc3:
                if st.button("All Time", key=f"popout_qall_{popout_key}"):
                    new_from = date(2000, 1, 1)
                    new_to = date.today()

        apply_cols = st.columns([4, 1])
        with apply_cols[1]:
            if st.button("Apply Date Range", key=f"popout_apply_{popout_key}", type="primary"):
                st.session_state.filters[popout_key] = {
                    "from": new_from.strftime("%Y-%m-%d"),
                    "to": new_to.strftime("%Y-%m-%d"),
                }
                st.session_state.active_popout = None
                st.rerun()

    else:
        # ----- LOOKUP / TEXT / BOOLEAN POPOUT -----
        header_cols = st.columns([6, 1])
        with header_cols[0]:
            st.markdown(f"**🔍 {label}**")
        with header_cols[1]:
            if st.button("Close", key="popout_close"):
                st.session_state.active_popout = None
                st.rerun()

        # Get value counts
        lookup_key = field_meta.get("lookup")
        value_counts = get_field_value_counts(table_name, field_name, lookup_key)

        # Current selections
        current_selections = set(st.session_state.filters.get(popout_key, []))

        # Search box
        search = st.text_input("Search...", key=f"popout_search_{popout_key}", placeholder="Type to filter options...")

        # Filter by search
        if search:
            search_lower = search.lower()
            value_counts = [(c, d, n) for c, d, n in value_counts
                            if search_lower in d.lower() or search_lower in c.lower()]

        total_options = len(value_counts)

        # Select All / Clear All
        btn_cols = st.columns([1, 1, 4])
        with btn_cols[0]:
            if st.button("Select All", key=f"popout_selall_{popout_key}"):
                current_selections = {c for c, d, n in value_counts}
        with btn_cols[1]:
            if st.button("Clear All", key=f"popout_clrall_{popout_key}"):
                current_selections = set()

        # Checkbox grid — 2 columns
        col1, col2 = st.columns(2)
        new_selections = set()
        for i, (code, display, count) in enumerate(value_counts):
            count_str = f"{count:,}"
            target_col = col1 if i % 2 == 0 else col2
            with target_col:
                checked = st.checkbox(
                    f"{display} ({count_str})",
                    value=code in current_selections,
                    key=f"popout_cb_{popout_key}_{code}",
                )
                if checked:
                    new_selections.add(code)

        if total_options == 500:
            st.caption(f"Showing first 500 of many values. Use search to narrow down.")
        else:
            st.caption(f"{total_options} total options")

        # Apply button
        apply_cols = st.columns([4, 1])
        with apply_cols[1]:
            if st.button("Apply Filter", key=f"popout_apply_{popout_key}", type="primary"):
                if new_selections:
                    st.session_state.filters[popout_key] = sorted(new_selections)
                else:
                    st.session_state.filters.pop(popout_key, None)
                st.session_state.active_popout = None
                st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)


# Render popout
_render_filter_popout()


# ---------------------------------------------------------------------------
# Active Filters Bar
# ---------------------------------------------------------------------------

def _render_active_filters_bar():
    """Render dismissible filter pills above the tabs."""
    if not st.session_state.filters:
        return

    pills_html = '<div class="filter-pills"><span class="filter-pills-label">Active Filters:</span>'

    for filter_key, filter_value in st.session_state.filters.items():
        table_name, field_name = filter_key.split(".", 1)
        field_meta = FIELD_META.get(table_name, {}).get(field_name, {})
        label = field_meta.get("label", field_name)
        lookup_key = field_meta.get("lookup")
        lookup = LOOKUPS.get(lookup_key, {}) if lookup_key else {}

        if isinstance(filter_value, dict):
            # Date range
            display = f"{filter_value.get('from','')} to {filter_value.get('to','')}"
        elif isinstance(filter_value, list):
            # Categorical — show up to 3 values
            display_vals = [lookup.get(v, v) for v in filter_value[:3]]
            display = ", ".join(display_vals)
            if len(filter_value) > 3:
                display += f" +{len(filter_value) - 3} more"
        else:
            display = str(filter_value)

        pills_html += (
            f'<span class="filter-pill">'
            f'{label}: {display}'
            f'</span>'
        )

    pills_html += '</div>'
    st.markdown(pills_html, unsafe_allow_html=True)

    # Render remove buttons using Streamlit (can't use HTML onclick)
    filter_keys = list(st.session_state.filters.keys())
    if filter_keys:
        btn_cols = st.columns(len(filter_keys) + 1)
        for i, fk in enumerate(filter_keys):
            table_name, field_name = fk.split(".", 1)
            field_meta = FIELD_META.get(table_name, {}).get(field_name, {})
            label = field_meta.get("label", field_name)
            with btn_cols[i]:
                if st.button(f"Remove {label}", key=f"remove_filter_{fk}"):
                    del st.session_state.filters[fk]
                    st.rerun()
        with btn_cols[-1]:
            if st.button("Clear All", key="clear_all_filters"):
                st.session_state.filters = {}
                st.rerun()


_render_active_filters_bar()


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_exec, tab_outcomes, tab_courts, tab_judges, tab_hearings, tab_explore, tab_ai = st.tabs([
    "Executive Summary",
    "Case Outcomes",
    "Court Performance",
    "Judge Analytics",
    "Hearings",
    "Data Explorer",
    "AI Analyst",
])


# ===== TAB 1: Executive Summary =============================================
with tab_exec:
    if not _is_table_active("b_tblproceeding"):
        _table_required_message("Proceedings")
    else:
        where, needs_tables = build_where("p", table_type="proc")
        from_clause = _proc_from("p", "c", needs_tables)

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
        where_nat, needs_tables_nat = build_where("p", table_type="proc")
        needs_tables_nat.add("a_tblcase")
        from_nat = _proc_from("p", "c", needs_tables_nat)

        with col_left:
            if _is_table_active("a_tblcase"):
                nat_sql = f"""
                    SELECT n.NAT_NAME as nationality, COUNT(*) as cases
                    FROM {from_nat}
                    LEFT JOIN tbllookupnationality n ON c.NAT = n.NAT_CODE
                    WHERE {where_nat} AND n.NAT_NAME IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                """
                nat_df = run_query(nat_sql)
                if not nat_df.empty:
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
            else:
                _table_required_message("Cases")

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
    if not _is_table_active("b_tblproceeding"):
        _table_required_message("Proceedings")
    else:
        where, needs_tables = build_where("p", table_type="proc")
        from_clause = _proc_from("p", "c", needs_tables)

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
                mode="lines", fill="tozeroy",
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
        if _is_table_active("a_tblcase"):
            where_nat_out, needs_tables_nat_out = build_where("p", table_type="proc")
            needs_tables_nat_out.add("a_tblcase")
            from_nat_out = _proc_from("p", "c", needs_tables_nat_out)

            # Derive date constraints for subquery
            _date_from_str = "2020-01-01"
            _date_to_str = date.today().strftime("%Y-%m-%d")
            for _fk2, _fv2 in st.session_state.filters.items():
                if _fk2.startswith("b_tblproceeding.") and isinstance(_fv2, dict):
                    _date_from_str = _fv2.get("from", _date_from_str)
                    _date_to_str = _fv2.get("to", _date_to_str)
                    break

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
                    WHERE TRY_CAST(p2."{_proc_date}" AS TIMESTAMP) >= '{_date_from_str}'
                        AND TRY_CAST(p2."{_proc_date}" AS TIMESTAMP) <= '{_date_to_str}'
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
    if not _is_table_active("b_tblproceeding"):
        _table_required_message("Proceedings")
    else:
        where, needs_tables = build_where("p", table_type="proc")
        from_clause = _proc_from("p", "c", needs_tables)

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
                        title=dict(text=f"{court_options.get(selected_court_code, '')} -- Cases & Grant Rate",
                                   font=dict(size=14)),
                        yaxis=dict(title="Cases", side="left"),
                        yaxis2=dict(title="Grant Rate %", side="right", overlaying="y",
                                    ticksuffix="%", showgrid=False),
                        barmode="overlay",
                    )
                    st.plotly_chart(fig, use_container_width=True, key="court_trend_chart")


# ===== TAB 4: Judge Analytics ================================================
with tab_judges:
    if not _is_table_active("b_tblproceeding"):
        _table_required_message("Proceedings")
    else:
        where, needs_tables = build_where("p", table_type="proc")
        from_clause = _proc_from("p", "c", needs_tables)

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


# ===== TAB 5: Hearings ======================================================
with tab_hearings:
    if not _is_table_active("tbl_schedule"):
        _table_required_message("Hearings")
    else:
        where_s, needs_tables_s = build_where("s", table_type="sched")
        from_sched = _sched_from("s", "c", needs_tables_s)

        # --- Hearing volume over time ---
        hvol_sql = f"""
            SELECT
                DATE_TRUNC('month', TRY_CAST(s."{_sched_date}" AS TIMESTAMP)) as month,
                COUNT(*) as hearings
            FROM {from_sched}
            WHERE {where_s}
            GROUP BY 1 ORDER BY 1
        """
        hvol_df = run_query(hvol_sql)
        if not hvol_df.empty:
            fig = _make_area_chart(hvol_df, "month", "hearings", ACCENT_PURPLE, "Hearing Volume Over Time", 340)
            st.plotly_chart(fig, use_container_width=True, key="hear_vol")

        st.markdown("")

        col_left, col_right = st.columns(2)

        with col_left:
            # Calendar type breakdown
            cal_sql = f"""
                SELECT
                    COALESCE(ct.strCalTypeDescription, s.CAL_TYPE) as calendar_type,
                    COUNT(*) as hearings
                FROM {from_sched}
                LEFT JOIN tbllookupcal_type ct ON s.CAL_TYPE = ct.strCalTypeCode
                WHERE {where_s} AND s.CAL_TYPE IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """
            cal_df = run_query(cal_sql)
            if not cal_df.empty:
                fig = _make_donut(cal_df, "hearings", "calendar_type",
                                  "Calendar Type Breakdown", 400,
                                  center_text=_fmt_number(cal_df["hearings"].sum()))
                st.plotly_chart(fig, use_container_width=True, key="hear_cal")

        with col_right:
            # Top adjournment reasons
            adj_sql = f"""
                SELECT
                    COALESCE(ac.strDesciption, s.ADJ_RSN) as reason,
                    COUNT(*) as hearings
                FROM {from_sched}
                LEFT JOIN tbladjournmentcodes ac ON s.ADJ_RSN = ac.strcode
                WHERE {where_s} AND s.ADJ_RSN IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """
            adj_df = run_query(adj_sql)
            if not adj_df.empty:
                n = len(adj_df)
                bar_colors = [f"rgba(139,92,246,{0.4 + 0.6 * (n - i) / n})" for i in range(n)]
                fig = go.Figure(go.Bar(
                    x=adj_df["hearings"], y=adj_df["reason"], orientation="h",
                    marker=dict(color=bar_colors, cornerradius=6),
                    hovertemplate="%{y}<br><b>%{x:,.0f} hearings</b><extra></extra>",
                ))
                fig.update_layout(
                    height=400, template=PLOTLY_TEMPLATE,
                    title=dict(text="Top 10 Adjournment Reasons", font=dict(size=14, color=TEXT_PRIMARY)),
                    yaxis=dict(autorange="reversed"), showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, key="hear_adj")

        # --- Hearing medium breakdown ---
        med_sql = f"""
            SELECT
                COALESCE(s.ADJ_MEDIUM, 'Unknown') as medium,
                COUNT(*) as hearings
            FROM {from_sched}
            WHERE {where_s}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
        med_df = run_query(med_sql)
        if not med_df.empty and len(med_df) > 1:
            st.markdown("")
            fig = _make_bar_chart(med_df, "medium", "hearings", ACCENT_GREEN,
                                  title="Hearings by Medium", height=350)
            st.plotly_chart(fig, use_container_width=True, key="hear_medium")


# ===== TAB 6: Data Explorer ==================================================
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


# ===== TAB 7: AI Analyst =====================================================
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

Key tables and relationships:
- a_tblcase: Case master. PK: IDNCASE (BIGINT). Has NAT (nationality code), LANG (language code), CUSTODY, CASE_TYPE, C_BIRTHDATE (VARCHAR), Sex, DATE_OF_ENTRY (TIMESTAMP), LATEST_HEARING (TIMESTAMP), LPR.
- b_tblproceeding: Main proceedings table. PK: IDNPROCEEDING (VARCHAR). IDNCASE links to a_tblcase (VARCHAR -- join with TRY_CAST(p.IDNCASE AS BIGINT) = c.IDNCASE). COMP_DATE (VARCHAR) is completion date, OSC_DATE (VARCHAR) is filing date, HEARING_DATE (VARCHAR). DEC_CODE (VARCHAR), IJ_CODE=judge, BASE_CITY_CODE=court, NAT, LANG, ABSENTIA, CASE_TYPE.
- tbl_schedule: Hearing schedule. IDNSCHEDULE PK. IDNPROCEEDING, IDNCASE (both VARCHAR). ADJ_DATE (VARCHAR)=hearing date, OSC_DATE (VARCHAR)=filing date, ADJ_RSN=adjournment reason, CAL_TYPE, IJ_CODE.
- tbl_court_appln: Applications filed. IDNPROCEEDING, IDNCASE. APPL_CODE=application type, APPL_DEC=decision.
- b_tblproceedcharges: Charges. IDNPROCEEDING, IDNCASE. CHARGE=charge code.
- tbl_court_motions: Motions. IDNPROCEEDING, IDNCASE. COMP_DATE, OSC_DATE, MOTION_RECD_DATE (all VARCHAR).
- tbl_repsassigned: Attorney assignments. IDNCASE.

Key lookup tables -- ALWAYS JOIN these for human-readable output:
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
2. ALWAYS JOIN lookup tables for human-readable names -- never show raw codes to the user. Use COALESCE(lookup.name, raw_code) as a fallback.
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
