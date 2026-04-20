"""
Event Replicator — batch build and send event payloads to the TE API.
"""

import json
import os
from collections import defaultdict

import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Event Replicator",
    page_icon="🎟️",
    layout="wide",
)

st.title("🎟️ Event Replicator")
st.caption("Build and send event payloads to the TE API.")

# ---------------------------------------------------------------------------
# Go service base URL — reads from Streamlit secrets, falls back to env var
# ---------------------------------------------------------------------------

def get_base_url() -> str:
    try:
        return st.secrets["event_replicator"]["base_url"].rstrip("/")
    except (KeyError, FileNotFoundError):
        return os.environ.get("EVENT_REPLICATOR_BASE_URL", "http://localhost:8080").rstrip("/")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mp_field(marketplaces, name, marketplace_name=None):
    if not marketplaces:
        return ""
    for entry in marketplaces:
        if entry.get("name") == name:
            if marketplace_name is None or entry.get("marketplace_name") == marketplace_name:
                return entry.get("item_id", "") or ""
    return ""


def parse_event_name(start_date_est, event_name):
    import re
    from datetime import datetime
    raw = (start_date_est or "").strip()
    raw = re.sub(r"^\w+\s+", "", raw, count=1)
    for fmt in ["%B %d, %Y %I:%M %p", "%B %d, %Y %I:%M%p", "%B %d, %Y"]:
        try:
            dt = datetime.strptime(raw, fmt)
            return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} {event_name}"
        except ValueError:
            pass
    return f"{raw} {event_name}"


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_payload(event_rows):
    first = event_rows[0]
    mp = first["marketplaces"] or []

    payload = {
        "ProductionID":        mp_field(mp, "tradedesk"),
        "eventName":           parse_event_name(first["start_date_est"], first["rtk_event_name"]),
        "ZHEventID":           mp_field(mp, "zerohero"),
        "shEventID":           mp_field(mp, "stubhub"),
        "sgEventID":           mp_field(mp, "seatgeek"),
        "vividEventID":        mp_field(mp, "vividseats"),
        "shVenueID":           str(first["sh_venue_id"]) if first["sh_venue_id"] else "",
        "shParkingEventID":    mp_field(mp, "PARKING", "stubhub"),
        "sgParkingEventID":    mp_field(mp, "PARKING", "seatgeek"),
        "vividParkingEventID": mp_field(mp, "PARKING", "vividseats"),
        "taxRate":             0,
        "profitPercentage":    0.08,
        "variants":            [],
    }

    for r in event_rows:
        is_parking = r["is_parking"]
        payload["variants"].append({
            "variantTitle":       r["variant_title"] or "",
            "variantType":        3 if is_parking else 1,
            "maxBuyPrice":        7,
            "minQty":             1 if is_parking else 2,
            "exactQty":           False,
            "ignoreTerms":        "",
            "mustHave":           r["must_have"] or "",
            "mustHaveRows":       "",
            "ignoreRows":         False,
            "exactMatchSections": True,
            "excludeWheelchair":  True,
            "excludePiggyback":   True,
            "excludeObstructed":  True,
            "excludeStudent":     True,
            "autoBuy":            False,
            "autoBuyPercentage":  8.0,
            "shMustHaveSection":  "",
            "sgMustHaveSection":  r["sg_must_have_section"] or "",
            "sendAlerts":         False,
        })

    return payload


# ---------------------------------------------------------------------------
# Fetch from Go service
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60, show_spinner=False)
def fetch_events(event_ids: tuple):
    base_url = get_base_url()
    url = f"{base_url}/venueMapping/v1/event-replicator/batch"
    resp = requests.get(
        url,
        params={"event_ids": ",".join(str(i) for i in event_ids)},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["rows"]


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

API_URL = "https://tradeinternet.net/roman/te/insert-event-api.php"

# Input
st.subheader("Event IDs")
raw_input = st.text_area(
    "Paste one or more event IDs (one per line, or space/comma separated)",
    height=120,
    placeholder="51237\n54999\n55159",
)

col1, col2 = st.columns([1, 5])
preview_btn = col1.button("🔍 Preview", use_container_width=True)
send_btn    = col2.button("🚀 Send to API", type="primary", use_container_width=True)

# Parse IDs
def parse_ids(raw):
    import re
    tokens = re.split(r"[\s,]+", raw.strip())
    ids, errors = [], []
    for t in tokens:
        if not t:
            continue
        try:
            ids.append(int(t))
        except ValueError:
            errors.append(t)
    return ids, errors

# Main logic
if preview_btn or send_btn:
    event_ids, bad = parse_ids(raw_input)

    if bad:
        st.warning(f"Skipping non-integer values: {', '.join(bad)}")
    if not event_ids:
        st.error("No valid event IDs found.")
        st.stop()

    with st.spinner(f"Fetching {len(event_ids)} event(s) from service..."):
        try:
            all_rows = fetch_events(tuple(sorted(event_ids)))
        except Exception as e:
            st.error(f"Service error: {e}")
            st.stop()

    if not all_rows:
        st.error("No events found for the given IDs.")
        st.stop()

    # Group by event
    by_event = defaultdict(list)
    for row in all_rows:
        by_event[row["rtk_event_id"]].append(row)

    missing = set(event_ids) - set(by_event.keys())
    if missing:
        st.warning(f"No data found for event ID(s): {sorted(missing)}")

    st.success(f"Found **{len(by_event)}** event(s) — **{len(all_rows)}** total variant rows")

    # Build all payloads
    payloads = {eid: build_payload(rows) for eid, rows in by_event.items()}

    # Preview section
    st.divider()
    for event_id in event_ids:
        if event_id not in payloads:
            continue

        payload = payloads[event_id]
        tickets  = [v for v in payload["variants"] if v["variantType"] == 1]
        parking  = [v for v in payload["variants"] if v["variantType"] == 3]

        with st.expander(f"**{payload['eventName']}** — {len(payload['variants'])} variants", expanded=False):
            m = st.columns(4)
            m[0].metric("ProductionID",  payload["ProductionID"] or "—")
            m[1].metric("SH Event",      payload["shEventID"] or "—")
            m[2].metric("SG Event",      payload["sgEventID"] or "—")
            m[3].metric("Vivid Event",   payload["vividEventID"] or "—")

            if tickets:
                st.markdown("**🎫 Ticket Variants**")
                st.dataframe(
                    [{"Zone": v["variantTitle"], "mustHave": v["mustHave"], "sgMustHave": v["sgMustHaveSection"]} for v in tickets],
                    use_container_width=True, hide_index=True,
                )
            if parking:
                st.markdown("**🚗 Parking Variants**")
                st.dataframe(
                    [{"Zone": v["variantTitle"], "mustHave": v["mustHave"], "sgMustHave": v["sgMustHaveSection"]} for v in parking],
                    use_container_width=True, hide_index=True,
                )

            # JSON download
            st.download_button(
                label="⬇️ Download payload JSON",
                data=json.dumps(payload, indent=2),
                file_name=f"event_{payload['ProductionID'] or event_id}_payload.json",
                mime="application/json",
            )

    # Send section
    if send_btn:
        st.divider()
        st.subheader("API Results")

        summary = {"success": [], "already_exists": [], "error": []}

        for event_id in event_ids:
            if event_id not in payloads:
                continue

            payload = payloads[event_id]
            event_name = payload["eventName"]

            try:
                response = requests.post(
                    API_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )

                if response.status_code == 200:
                    st.success(f"✅ **{event_name}** — {response.json().get('processed', '?')} variants processed")
                    summary["success"].append(event_id)
                elif response.status_code == 400:
                    st.warning(f"⚠️ **{event_name}** — Event already exists in the web service")
                    summary["already_exists"].append(event_id)
                else:
                    st.error(f"❌ **{event_name}** — HTTP {response.status_code}: {response.text}")
                    summary["error"].append(event_id)

            except requests.RequestException as e:
                st.error(f"❌ **{event_name}** — Request failed: {e}")
                summary["error"].append(event_id)

        # Summary
        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("✅ Sent",            len(summary["success"]))
        c2.metric("⚠️ Already Existed", len(summary["already_exists"]))
        c3.metric("❌ Errors",           len(summary["error"]))
