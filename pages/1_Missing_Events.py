"""
Missing Events Checker — compare official league schedules against SG + TE inventory.
"""
from __future__ import annotations

import io
import logging
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from src.auth.client import AuthenticatedClient
from src.comparison.checker import find_missing_games
from src.config import load_config, load_credentials
from src.leagues import SUPPORTED_LEAGUES, MLB_TEAMS, NHL_TEAMS, NBA_TEAMS, NFL_TEAMS

_LEAGUE_TEAM_LISTS: dict[str, list[str]] = {
    "MLB": sorted(MLB_TEAMS),
    "NHL": sorted(NHL_TEAMS),
    "NBA": sorted(NBA_TEAMS),
    "NFL": sorted(NFL_TEAMS),
}
from src.models import Event, ScheduledGame
from src.scrapers.sg import SGScraper
from src.scrapers.te import TEScraper
from src.schedules.registry import get_schedule_provider

logging.basicConfig(level=logging.WARNING)

st.markdown(
    """
    <style>
    [data-testid="metric-container"] { padding: 0.6rem 1rem; }
    hr { border-color: #2e2e2e; }
    [data-testid="stDataFrameResizable"] th { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────────────
if "results" not in st.session_state:
    st.session_state.results: dict = {}
if "last_run" not in st.session_state:
    st.session_state.last_run: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_schedule(league: str, from_iso: str, to_iso: str) -> list[ScheduledGame]:
    provider = get_schedule_provider(league)
    return provider.get_games(date.fromisoformat(from_iso), date.fromisoformat(to_iso))


def _season_end(cfg: dict, league: str) -> date:
    raw = cfg.get("schedules", {}).get(league.lower(), {}).get("season_end")
    try:
        return date.fromisoformat(raw) if raw else date(date.today().year, 12, 31)
    except ValueError:
        return date(date.today().year, 12, 31)


def _build_client(cfg: dict, creds: dict, tool: str) -> AuthenticatedClient:
    return AuthenticatedClient(
        tool_name=tool.upper(),
        base_url=cfg["tools"][tool]["base_url"],
        login_url=cfg["tools"][tool]["login_url"],
        username=creds[tool]["username"],
        password=creds[tool]["password"],
        tool_config=cfg["tools"][tool],
    )


def _missing_to_df(games: list[ScheduledGame]) -> pd.DataFrame:
    if not games:
        return pd.DataFrame(columns=["Date", "Away", "Home", "Venue"])
    return pd.DataFrame(
        [{"Date": g.game_date, "Away": g.away_team, "Home": g.home_team, "Venue": g.venue}
         for g in sorted(games, key=lambda g: g.game_date)]
    )


def _inventory_to_df(events: list[Event]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["Date", "Event", "Home", "Away", "Venue", "Tool"])
    return pd.DataFrame(
        [{"Date": e.event_date, "Event": e.raw_title, "Home": e.home_team,
          "Away": e.away_team, "Venue": e.venue, "Tool": e.source}
         for e in sorted(events, key=lambda e: e.event_date)]
    )


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Config / credentials
# ─────────────────────────────────────────────────────────────────────────────
try:
    _cfg = load_config()
    _creds = load_credentials()
    _config_ok = True
except (FileNotFoundError, EnvironmentError) as _cfg_err:
    _config_ok = False


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📋 Missing Events")
    st.caption("Missing game detector · SG + TE tools")
    st.divider()

    league = st.radio("Select league", SUPPORTED_LEAGUES, horizontal=False)

    st.divider()

    st.markdown("**Filter by tool**")
    tools_filter = st.multiselect(
        "Include inventory from",
        options=["SG", "TE"],
        default=["SG", "TE"],
        label_visibility="collapsed",
    )
    if not tools_filter:
        st.warning("Select at least one tool.")

    st.divider()

    st.markdown("**Filter by date range**")
    today = date.today()
    default_end = _season_end(_cfg, league) if _config_ok else date(today.year, 12, 31)

    date_range = st.date_input(
        "Date range",
        value=(today, default_end),
        min_value=today - timedelta(days=1),
        max_value=date(2027, 12, 31),
        label_visibility="collapsed",
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        filter_from, filter_to = date_range
    else:
        filter_from = date_range if isinstance(date_range, date) else today
        filter_to = default_end

    st.divider()

    st.markdown("**Filter by team**")
    if league in st.session_state.results:
        _sched = st.session_state.results[league]["schedule"]
        _team_options = sorted({t for g in _sched for t in (g.home_team, g.away_team)})
    else:
        _team_options = _LEAGUE_TEAM_LISTS[league]

    teams_filter = st.multiselect(
        "teams_multiselect",
        options=_team_options,
        default=[],
        placeholder="All teams (leave empty for no filter)",
        label_visibility="collapsed",
    )

    st.divider()

    run_btn = st.button("▶  Run Check", type="primary", use_container_width=True)

    if league in st.session_state.last_run:
        ts = st.session_state.last_run[league]
        st.caption(f"Last scraped: {ts.strftime('%b %d  %I:%M %p')}")
        st.caption("Filters apply without re-running.")

    st.divider()

    with st.expander("🔍 Discover page structure"):
        st.caption("Inspect login forms and table headers to confirm config.yaml selectors.")
        disc_btn = st.button("Run Discover", use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main header
# ─────────────────────────────────────────────────────────────────────────────
if not _config_ok:
    st.error(
        f"**Configuration error:** {_cfg_err}\n\n"
        "Copy `.env.example` → `.env` and fill in all four credentials, then refresh."
    )
    st.stop()

tool_label = " + ".join(tools_filter) if tools_filter else "no tools"
team_label = ", ".join(teams_filter) if teams_filter else "all teams"
st.title(f"{league} — Missing Game Check")
st.caption(
    f"Schedule: **{filter_from.strftime('%b %d, %Y')}** → **{filter_to.strftime('%b %d, %Y')}**  ·  "
    f"Tools: **{tool_label}**  ·  "
    f"Teams: **{team_label}**"
)
st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# RUN CHECK
# ─────────────────────────────────────────────────────────────────────────────
if run_btn and tools_filter:
    with st.status("Running check…", expanded=True) as status_box:

        st.write("🔐  Logging in to SG…")
        sg_client = _build_client(_cfg, _creds, "sg")
        try:
            sg_client.login()
        except RuntimeError as exc:
            status_box.update(label="Login failed", state="error")
            st.error(f"SG login failed: {exc}")
            st.stop()

        st.write("🔐  Logging in to TE…")
        te_client = _build_client(_cfg, _creds, "te")
        try:
            te_client.login()
        except RuntimeError as exc:
            status_box.update(label="Login failed", state="error")
            st.error(f"TE login failed: {exc}")
            st.stop()

        st.write("📋  Scraping SG events…")
        sg_events_raw = SGScraper(sg_client, _cfg["tools"]["sg"]).fetch_events()
        if not sg_events_raw:
            st.warning("⚠️ SG returned 0 events — check selectors via Discover.")

        st.write("📋  Scraping TE events…")
        te_events_raw = TEScraper(te_client, _cfg["tools"]["te"]).fetch_events()
        if not te_events_raw:
            st.warning("⚠️ TE returned 0 events — check selectors via Discover.")

        season_end = _season_end(_cfg, league)
        st.write(f"📅  Fetching {league} schedule ({today} → {season_end})…")
        try:
            full_schedule = _fetch_schedule(league, today.isoformat(), season_end.isoformat())
        except Exception as exc:
            status_box.update(label="Schedule fetch failed", state="error")
            st.error(f"Could not retrieve {league} schedule: {exc}")
            st.stop()

        st.session_state.results[league] = {
            "sg_events": [e for e in sg_events_raw if e.league == league],
            "te_events": [e for e in te_events_raw if e.league == league],
            "sg_total": len(sg_events_raw),
            "te_total": len(te_events_raw),
            "schedule": full_schedule,
        }
        st.session_state.last_run[league] = datetime.now()
        status_box.update(label="Check complete ✓", state="complete", expanded=False)


# ─────────────────────────────────────────────────────────────────────────────
# RESULTS
# ─────────────────────────────────────────────────────────────────────────────
if league in st.session_state.results and tools_filter:
    r = st.session_state.results[league]

    active_events: list[Event] = []
    if "SG" in tools_filter:
        active_events += r["sg_events"]
    if "TE" in tools_filter:
        active_events += r["te_events"]

    filtered_schedule: list[ScheduledGame] = [
        g for g in r["schedule"]
        if filter_from <= g.game_date <= filter_to
    ]

    if teams_filter:
        filtered_schedule = [
            g for g in filtered_schedule
            if g.home_team in teams_filter or g.away_team in teams_filter
        ]

    min_word = _cfg.get("matching", {}).get("min_word_len", 4)
    missing_games = find_missing_games(active_events, filtered_schedule, league, min_word)
    covered = len(filtered_schedule) - len(missing_games)

    sg_shown = sum(1 for e in active_events if e.source == "SG")
    te_shown = sum(1 for e in active_events if e.source == "TE")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Scheduled", len(filtered_schedule))
    c2.metric("In Tools", covered)
    c3.metric(
        "Missing",
        len(missing_games),
        delta=f"-{len(missing_games)}" if missing_games else None,
        delta_color="inverse",
    )
    c4.metric("SG Events", sg_shown)
    c5.metric("TE Events", te_shown)

    st.divider()

    if not missing_games:
        st.success(f"✅  All {league} games in the selected range are accounted for.")
    else:
        st.warning(
            f"⚠️  **{len(missing_games)} {league} game(s)** are in the official schedule "
            f"but not found in **{tool_label}**."
        )

    st.subheader(f"Missing {league} Games")
    missing_df = _missing_to_df(missing_games)

    if missing_df.empty:
        st.info("No missing games for the selected filters.")
    else:
        st.dataframe(
            missing_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Date": st.column_config.DateColumn("Date", format="MMM DD, YYYY"),
                "Away":  st.column_config.TextColumn("Away Team"),
                "Home":  st.column_config.TextColumn("Home Team"),
                "Venue": st.column_config.TextColumn("Venue"),
            },
        )
        ts_str = st.session_state.last_run[league].strftime("%Y%m%d_%H%M%S")
        filter_tag = f"{'_'.join(t.lower() for t in tools_filter)}_{filter_from}_{filter_to}"
        st.download_button(
            label="⬇  Download missing games CSV",
            data=_df_to_csv_bytes(missing_df),
            file_name=f"{league.lower()}_missing_{filter_tag}_{ts_str}.csv",
            mime="text/csv",
        )

    st.divider()

    inv_events = [e for e in active_events if filter_from <= e.event_date <= filter_to]
    if teams_filter:
        inv_events = [
            e for e in inv_events
            if any(team.lower() in e.raw_title.lower() for team in teams_filter)
        ]
    with st.expander(f"📦  Inventory in tools for selected range ({len(inv_events)} events)"):
        inv_df = _inventory_to_df(inv_events)
        if inv_df.empty:
            st.info("No events found for the selected filters.")
        else:
            st.dataframe(
                inv_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Date": st.column_config.DateColumn("Date", format="MMM DD, YYYY"),
                    "Tool": st.column_config.TextColumn("Tool"),
                },
            )
            ts_str = st.session_state.last_run[league].strftime("%Y%m%d_%H%M%S")
            filter_tag = f"{'_'.join(t.lower() for t in tools_filter)}_{filter_from}_{filter_to}"
            st.download_button(
                label="⬇  Download inventory CSV",
                data=_df_to_csv_bytes(inv_df),
                file_name=f"{league.lower()}_inventory_{filter_tag}_{ts_str}.csv",
                mime="text/csv",
                key=f"inv_dl_{league}_{filter_tag}",
            )

elif league not in st.session_state.results:
    st.info(
        "Select filters in the sidebar then press **▶ Run Check** to start.",
        icon="👈",
    )


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVER
# ─────────────────────────────────────────────────────────────────────────────
if disc_btn:
    st.divider()
    st.subheader("🔍 Page Structure Discovery")

    for tool_key, label in [("sg", "SG"), ("te", "TE")]:
        with st.expander(f"{label} — login form & tables", expanded=True):
            client = _build_client(_cfg, _creds, tool_key)

            st.markdown("**Step 1 — Login form fields**")
            form_info = client.discover()
            if "error" in form_info:
                st.error(form_info["error"])
            else:
                st.write(f"Form action: `{form_info['form_action']}`  |  Method: `{form_info['form_method']}`")
                st.write(f"Input fields found: `{list(form_info.get('fields', {}).keys())}`")
                st.write(
                    f"config.yaml expects → "
                    f"username_field: `{form_info['config_username_field']}`  |  "
                    f"password_field: `{form_info['config_password_field']}`"
                )
                if form_info.get("mismatches"):
                    for m in form_info["mismatches"]:
                        st.error(f"⚠️ Mismatch: {m}")
                    st.warning("Update config.yaml field names to match, then re-run.")
                else:
                    st.success("✅ Field names match config.yaml")

            st.markdown("**Step 2 — Page tables (after login)**")
            if form_info.get("mismatches"):
                st.info("Fix field name mismatches above before attempting login.")
            else:
                try:
                    client.login()
                    st.success("✅ Login succeeded")
                    scraper_cls = SGScraper if tool_key == "sg" else TEScraper
                    scraper = scraper_cls(client, _cfg["tools"][tool_key])
                    tables = scraper.discover_tables(_cfg["tools"][tool_key]["events_url"])
                    if not tables:
                        st.warning("No tables found on the page.")
                    for tbl in tables:
                        st.markdown(
                            f"`Table #{tbl['table_index']}` — "
                            f"**{tbl['row_count']} rows**  |  "
                            f"Headers: `{tbl['headers']}`"
                        )
                        if tbl["sample_rows"]:
                            n_cols = max(len(r) for r in tbl["sample_rows"])
                            padded = [r + [""] * (n_cols - len(r)) for r in tbl["sample_rows"]]
                            st.dataframe(
                                pd.DataFrame(padded, columns=[f"col_{i}" for i in range(n_cols)]),
                                hide_index=True,
                                use_container_width=True,
                            )
                except RuntimeError as exc:
                    st.error(f"❌ {exc}")
