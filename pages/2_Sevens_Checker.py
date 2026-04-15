"""
7's Checker — scans event pricing pages for all-$7.00 prices (wrong market map).

When every ticket's "Current buy price" on a pricing page shows $7.00, it means
the market map is configured incorrectly and needs to be updated.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from src.auth.client import AuthenticatedClient
from src.config import load_config, load_credentials
from src.scrapers.sevens import SevensScraper

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
if "sevens_results" not in st.session_state:
    st.session_state.sevens_results: dict = {}   # keyed by tool key ("sg" / "te")
if "sevens_last_run" not in st.session_state:
    st.session_state.sevens_last_run: dict = {}

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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _build_client(tool: str) -> AuthenticatedClient:
    return AuthenticatedClient(
        tool_name=tool.upper(),
        base_url=_cfg["tools"][tool]["base_url"],
        login_url=_cfg["tools"][tool]["login_url"],
        username=_creds[tool]["username"],
        password=_creds[tool]["password"],
        tool_config=_cfg["tools"][tool],
    )


_TIERS: list[tuple[float, str]] = [
    (0.0,  "Perfect! LFG!"),
    (25.0, "Minor Issue"),
    (50.0, "Moderate Issue"),
    (75.0, "Map Concern"),
    (100.0, "7 SZN IT IS OVER"),
]


def _tier_label(pct: float) -> str:
    """Return the tier label for a given percentage of $7 sections."""
    if pct == 0.0:
        return "Perfect! LFG!"
    elif pct <= 25.0:
        return "Minor Issue"
    elif pct <= 50.0:
        return "Moderate Issue"
    elif pct <= 75.0:
        return "Map Concern"
    else:
        return "7 SZN IT IS OVER"


def _results_to_df(results: list[dict], diagnostic: bool = False) -> pd.DataFrame:
    rows = []
    for r in results:
        total = r.get("total_rows", 0)
        seven_count = r.get("seven_count", 0)
        pct = r.get("seven_pct", round(seven_count / total * 100, 1) if total else 0.0)
        row = {
            "Event": r.get("event", ""),
            "Date": r.get("date", ""),
            "Status": _tier_label(pct),
            "$7 Sections": seven_count,
            "Total Sections": total,
            "% at $7": pct,
        }
        if diagnostic:
            fetch_ok = r.get("fetch_ok", True)
            col_found = r.get("column_found", True)
            prices = r.get("prices", [])
            if not fetch_ok:
                diag = f"Fetch failed: {r.get('error', '?')}"
            elif not col_found:
                hdrs = r.get("all_table_headers", [])
                diag = f"No buy-price col — headers: {hdrs}"
            elif not prices:
                diag = f"Column '{r.get('column_header', '?')}' found but 0 rows"
            else:
                sample = prices[:5]
                diag = f"Col: '{r.get('column_header', '?')}' · Sample: {sample}{'…' if len(prices) > 5 else ''}"
            row["Diagnostic"] = diag
        row["Pricing URL"] = r.get("pricing_url", "")
        rows.append(row)
    return pd.DataFrame(rows)


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("💰 7's Checker")
    st.caption("Wrong-map detector · pricing page scanner")
    st.divider()

    tool_label = st.radio("Select tool", ["SG", "TE"], horizontal=True)
    tool_key = tool_label.lower()

    st.divider()

    run_btn = st.button("▶  Run 7's Check", type="primary", use_container_width=True)

    if tool_key in st.session_state.sevens_last_run:
        ts = st.session_state.sevens_last_run[tool_key]
        st.caption(f"Last run: {ts.strftime('%b %d  %I:%M %p')}")

    st.divider()

    with st.expander("🔍 Discover pricing links"):
        st.caption(
            "Shows every link found in each event row on the overview page. "
            "Use this to verify which link leads to the pricing page."
        )
        disc_btn = st.button("Run Discover", use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Guard
# ─────────────────────────────────────────────────────────────────────────────
if not _config_ok:
    st.error(
        f"**Configuration error:** {_cfg_err}\n\n"
        "Copy `.env.example` → `.env` and fill in credentials, then refresh."
    )
    st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
st.title(f"💰 7's Checker — {tool_label}")
st.caption(
    "Scans every event's pricing page. When **75% or more** of sections have a "
    "current buy price of **$7.00**, the market map is likely wrong and needs to be corrected."
)
st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# RUN CHECK
# ─────────────────────────────────────────────────────────────────────────────
if run_btn:
    with st.status(f"Running 7's check on {tool_label}…", expanded=True) as status_box:

        st.write(f"🔐  Logging in to {tool_label}…")
        try:
            client = _build_client(tool_key)
            client.login()
        except RuntimeError as exc:
            status_box.update(label="Login failed", state="error")
            st.error(f"{tool_label} login failed: {exc}")
            st.stop()

        scraper = SevensScraper(client, _cfg["tools"][tool_key])

        st.write("📋  Loading event list from overview page…")
        event_links = scraper._extract_event_links()

        if not event_links:
            status_box.update(label="No events found", state="error")
            st.error(
                "No events with pricing links found on the overview page.  \n"
                "Run **Discover** (sidebar) to inspect what links are available."
            )
            st.stop()

        st.write(f"🔍  Checking {len(event_links)} events for all-$7.00 pricing…")
        progress_bar = st.progress(0, text="Checking events…")

        results: list[dict] = []

        def _on_progress(done: int, total: int) -> None:
            pct = done / total
            progress_bar.progress(pct, text=f"Checked {done} / {total} events…")

        results = scraper.run_check(progress_cb=_on_progress)

        st.session_state.sevens_results[tool_key] = results
        st.session_state.sevens_last_run[tool_key] = datetime.now()
        status_box.update(label="Check complete ✓", state="complete", expanded=False)


# ─────────────────────────────────────────────────────────────────────────────
# RESULTS
# ─────────────────────────────────────────────────────────────────────────────
if tool_key in st.session_state.sevens_results:
    results = st.session_state.sevens_results[tool_key]
    ts_str = st.session_state.sevens_last_run[tool_key].strftime("%Y%m%d_%H%M%S")

    def _tier_events(label: str) -> list[dict]:
        return [
            r for r in results
            if _tier_label(r.get("seven_pct", 0.0)) == label
        ]

    perfect   = _tier_events("Perfect! LFG!")
    minor     = _tier_events("Minor Issue")
    moderate  = _tier_events("Moderate Issue")
    concern   = _tier_events("Map Concern")
    szn       = _tier_events("7 SZN IT IS OVER")

    # ── Tier metrics ─────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Perfect! LFG!", len(perfect))
    c2.metric("Minor Issue", len(minor))
    c3.metric("Moderate Issue", len(moderate))
    c4.metric("Map Concern", len(concern))
    c5.metric("7 SZN IT IS OVER", len(szn))

    st.divider()

    _COL_CFG = {
        "% at $7": st.column_config.NumberColumn("% at $7", format="%.1f%%"),
        "Pricing URL": st.column_config.LinkColumn("Pricing URL"),
    }
    _DIAG_COL_CFG = {
        **_COL_CFG,
        "Diagnostic": st.column_config.TextColumn("Diagnostic", width="large"),
    }

    # ── Banner ────────────────────────────────────────────────────────────────
    if szn:
        st.error(f"7 SZN IT IS OVER — **{len(szn)} event(s)** have ≥75% of sections at $7.00.")
    elif concern:
        st.warning(f"Map Concern — **{len(concern)} event(s)** have 50–75% of sections at $7.00.")
    elif moderate:
        st.warning(f"Moderate Issue — **{len(moderate)} event(s)** have 25–50% of sections at $7.00.")
    elif minor:
        st.info(f"Minor Issue — **{len(minor)} event(s)** have 1–25% of sections at $7.00.")
    else:
        st.success("Perfect! LFG! — All events have correct pricing.")

    # ── Per-tier tables ───────────────────────────────────────────────────────
    for tier_label, tier_list, expanded in [
        ("7 SZN IT IS OVER",  szn,      True),
        ("Map Concern",       concern,   True),
        ("Moderate Issue",    moderate,  False),
        ("Minor Issue",       minor,     False),
    ]:
        if not tier_list:
            continue
        with st.expander(f"**{tier_label}** — {len(tier_list)} event(s)", expanded=expanded):
            df = _results_to_df(tier_list)
            st.dataframe(df, use_container_width=True, hide_index=True, column_config=_COL_CFG)
            st.download_button(
                label=f"⬇  Download {tier_label} CSV",
                data=_to_csv_bytes(df),
                file_name=f"{tool_key}_{tier_label.lower().replace(' ', '_')}_{ts_str}.csv",
                mime="text/csv",
                key=f"dl_{tier_label}",
            )

    st.divider()

    # ── All events (with diagnostics) ─────────────────────────────────────────
    with st.expander(f"📦  All {len(results)} events (with diagnostics)"):
        all_df = _results_to_df(results, diagnostic=True)
        st.dataframe(
            all_df, use_container_width=True, hide_index=True,
            column_config=_DIAG_COL_CFG,
        )
        st.download_button(
            label="⬇  Download full results CSV",
            data=_to_csv_bytes(all_df),
            file_name=f"{tool_key}_sevens_full_{ts_str}.csv",
            mime="text/csv",
            key="dl_full",
        )

    # ── Diagnostic when nothing is flagged ────────────────────────────────────
    if not szn and not concern and not moderate and not minor:
        st.info(
            "All events are Perfect! LFG!  If you expected issues, expand the "
            "diagnostics table above to see the sample prices fetched per event.",
            icon="🔎",
        )

else:
    st.info(
        f"Select a tool and press **▶ Run 7's Check** to scan for wrong maps.",
        icon="👈",
    )


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVER
# ─────────────────────────────────────────────────────────────────────────────
if disc_btn:
    st.divider()
    st.subheader("🔍 Pricing Link Discovery")
    st.caption(
        f"Showing all links found in each event row on the **{tool_label}** overview page. "
        "The checker picks the link whose icon/URL suggests pricing. "
        "Use this to verify the correct link is being followed."
    )

    with st.spinner(f"Logging in to {tool_label} and loading overview…"):
        try:
            client = _build_client(tool_key)
            client.login()
        except RuntimeError as exc:
            st.error(f"Login failed: {exc}")
            st.stop()

        scraper = SevensScraper(client, _cfg["tools"][tool_key])
        discovered = scraper.discover_event_links()

    if not discovered:
        st.warning(
            "No event rows found. Check `config.yaml` → `table_index` and `table_selector`."
        )
    else:
        st.info(f"Found {len(discovered)} event rows. Showing first 15.")
        for item in discovered[:15]:
            with st.expander(f"**{item['event'][:80]}**  ·  {item.get('date', '')}"):
                links = item.get("links", [])
                if not links:
                    st.info("No links found in this row.")
                else:
                    rows = []
                    for lnk in links:
                        rows.append({
                            "URL": lnk["url"],
                            "Text": lnk["text"] or "—",
                            "Title": lnk["title"] or "—",
                            "Icon classes": " ".join(lnk.get("icon_classes", [])) or "—",
                            "Money icon?": "✅" if lnk.get("is_money_icon") else "—",
                        })
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
