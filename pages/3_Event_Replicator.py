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
# SG payload builders
#
# SG differs from TE:
#   - Parking is a SEPARATE event with its own ProductionID (PARKING+seatgeek
#     marketplace entry). So each nexus ID produces TWO payloads.
#   - `mustHave` uses SG section names (sg_must_have_section) instead of TM.
#   - Adds `teEventID` (tradedesk cross-ref) and `venueVariantID` fields.
#   - Drops shParking/sgParking/vividParking event IDs on the main payload.
# ---------------------------------------------------------------------------

def is_usable(r):
    """Skip cohorts whose generic has no section data ('- DELETED' ghosts)."""
    return bool(r.get("variant_title")) and bool(r.get("sg_must_have_section"))


def sg_variant(r):
    is_parking = r["is_parking"]
    return {
        "variantTitle":       r.get("variant_title") or "",
        "venueVariantID":     0,  # hardcoded per spec — tool requires field but we don't use it
        "variantType":        3 if is_parking else 1,
        "maxBuyPrice":        7,
        "minQty":             1 if is_parking else 2,
        "exactQty":           False,
        "ignoreTerms":        "",
        "mustHave":           r.get("sg_must_have_section") or "",
        "mustHaveRows":       "",
        "ignoreRows":         False,
        "exactMatchSections": True,
        "excludeWheelchair":  True,
        "excludePiggyback":   True,
        "excludeObstructed":  True,
        "excludeStudent":     True,
        "autoBuy":            False,
        "autoBuyPercentage":  8.0,
        "sendAlerts":         False,
    }


def build_sg_main_payload(event_rows):
    first = event_rows[0]
    mp = first["marketplaces"] or []
    ticket_rows = [r for r in event_rows if not r["is_parking"] and is_usable(r)]
    return {
        "ProductionID":     mp_field(mp, "seatgeek"),
        "teEventID":        mp_field(mp, "tradedesk"),
        "eventName":        parse_event_name(first["start_date_est"], first["rtk_event_name"]),
        "ZHEventID":        mp_field(mp, "zerohero"),
        "shEventID":        mp_field(mp, "stubhub"),
        "sgEventID":        mp_field(mp, "seatgeek"),
        "vividEventID":     mp_field(mp, "vividseats"),
        "shVenueID":        str(first["sh_venue_id"]) if first["sh_venue_id"] else "",
        "taxRate":          0,
        "profitPercentage": 0.08,
        "variants":         [sg_variant(r) for r in ticket_rows],
    }


def build_sg_parking_payload(event_rows):
    """Returns None if the event has no SG parking production ID."""
    first = event_rows[0]
    mp = first["marketplaces"] or []
    parking_rows = [r for r in event_rows if r["is_parking"] and is_usable(r)]

    if not parking_rows:
        return None
    sg_parking_prod = mp_field(mp, "PARKING", "seatgeek")
    if not sg_parking_prod:
        return None

    return {
        "ProductionID":     sg_parking_prod,
        "teEventID":        mp_field(mp, "tradedesk"),  # fallback to main TE (TE has one ID for event+parking)
        "eventName":        parse_event_name(first["start_date_est"], first["rtk_event_name"]) + " - PARKING",
        "ZHEventID":        mp_field(mp, "PARKING", "zerohero"),
        "shEventID":        mp_field(mp, "PARKING", "stubhub"),
        "sgEventID":        mp_field(mp, "PARKING", "seatgeek"),
        "vividEventID":     mp_field(mp, "PARKING", "vividseats"),
        "shVenueID":        str(first["sh_venue_id"]) if first["sh_venue_id"] else "",
        "taxRate":          0,
        "profitPercentage": 0.08,
        "variants":         [sg_variant(r) for r in parking_rows],
    }


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

TE_API_URL = "https://tradeinternet.net/roman/te/insert-event-api.php"
SG_API_URL = "https://tradeinternet.net/roman/sg/insert-event-api.php"

# Input — separate boxes per destination tool so it's obvious which
# nexus IDs get routed where. Each side processes independently.
st.subheader("Nexus Event IDs")

te_col, sg_col = st.columns(2)

with te_col:
    st.markdown("### 🟦 TE Tool")
    st.caption(f"Posts to `{TE_API_URL}`")
    te_raw_input = st.text_area(
        "Nexus IDs for TE event adds",
        height=140,
        placeholder="51237\n54999\n55159",
        key="te_event_ids",
        help="One per line, or space/comma separated. Each ID produces one TE payload.",
    )

with sg_col:
    st.markdown("### 🟩 SG Tool")
    st.caption(f"Posts to `{SG_API_URL}`")
    sg_raw_input = st.text_area(
        "Nexus IDs for SG event adds",
        height=140,
        placeholder="41735\n...",
        key="sg_event_ids",
        help=(
            "One per line, or space/comma separated. Each ID produces TWO SG "
            "payloads: one for the main event and one for the parking event "
            "(separate SG production IDs)."
        ),
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

def post_payload(url, payload):
    """POST a single payload, return (ok_bool, status_code, text, processed)."""
    try:
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=60)
        try:
            js = resp.json()
            processed = js.get("processed", "?")
        except Exception:
            processed = "?"
        return resp.status_code, resp.text, processed
    except requests.RequestException as e:
        return None, str(e), "?"


def group_rows(all_rows, event_ids):
    by_event = defaultdict(list)
    for row in all_rows:
        by_event[row["rtk_event_id"]].append(row)
    missing = set(event_ids) - set(by_event.keys())
    return by_event, missing


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

if preview_btn or send_btn:
    te_event_ids, te_bad = parse_ids(te_raw_input)
    sg_event_ids, sg_bad = parse_ids(sg_raw_input)

    if te_bad:
        st.warning(f"TE input — skipping non-integer values: {', '.join(te_bad)}")
    if sg_bad:
        st.warning(f"SG input — skipping non-integer values: {', '.join(sg_bad)}")

    if not te_event_ids and not sg_event_ids:
        st.error("No valid event IDs found in either the TE or SG box.")
        st.stop()

    # Fetch every unique ID once (cached), then slice per tool.
    all_ids = sorted(set(te_event_ids) | set(sg_event_ids))
    with st.spinner(f"Fetching {len(all_ids)} event(s) from service..."):
        try:
            all_rows = fetch_events(tuple(all_ids))
        except Exception as e:
            st.error(f"Service error: {e}")
            st.stop()

    if not all_rows:
        st.error("No events found for the given IDs.")
        st.stop()

    by_event, global_missing = group_rows(all_rows, set(all_ids))

    # -----------------------------------------------------------------------
    # TE tool
    # -----------------------------------------------------------------------
    if te_event_ids:
        st.divider()
        st.header("🟦 TE Tool")

        te_missing = set(te_event_ids) & global_missing
        if te_missing:
            st.warning(f"TE — no data for event ID(s): {sorted(te_missing)}")

        te_payloads = {eid: build_payload(by_event[eid]) for eid in te_event_ids if eid in by_event}
        st.success(f"Built {len(te_payloads)} TE payload(s)")

        for event_id in te_event_ids:
            payload = te_payloads.get(event_id)
            if not payload:
                continue
            tickets = [v for v in payload["variants"] if v["variantType"] == 1]
            parking = [v for v in payload["variants"] if v["variantType"] == 3]

            with st.expander(f"**{payload['eventName']}** — {len(payload['variants'])} variants", expanded=False):
                m = st.columns(4)
                m[0].metric("ProductionID", payload["ProductionID"] or "—")
                m[1].metric("SH Event",     payload["shEventID"] or "—")
                m[2].metric("SG Event",     payload["sgEventID"] or "—")
                m[3].metric("Vivid Event",  payload["vividEventID"] or "—")

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

                st.download_button(
                    label="⬇️ Download TE payload JSON",
                    data=json.dumps(payload, indent=2),
                    file_name=f"event_{payload['ProductionID'] or event_id}_te_payload.json",
                    mime="application/json",
                    key=f"te_dl_{event_id}",
                )

        if send_btn and te_payloads:
            st.subheader("TE API Results")
            summary = {"success": 0, "already_exists": 0, "error": 0}
            for event_id in te_event_ids:
                payload = te_payloads.get(event_id)
                if not payload:
                    continue
                status, body, processed = post_payload(TE_API_URL, payload)
                event_name = payload["eventName"]
                if status == 200:
                    st.success(f"✅ **{event_name}** — {processed} variants processed")
                    summary["success"] += 1
                elif status == 400:
                    st.warning(f"⚠️ **{event_name}** — already exists")
                    summary["already_exists"] += 1
                else:
                    st.error(f"❌ **{event_name}** — HTTP {status}: {body}")
                    summary["error"] += 1
            c1, c2, c3 = st.columns(3)
            c1.metric("✅ Sent",            summary["success"])
            c2.metric("⚠️ Already Existed", summary["already_exists"])
            c3.metric("❌ Errors",           summary["error"])

    # -----------------------------------------------------------------------
    # SG tool — two payloads per event (main + parking)
    # -----------------------------------------------------------------------
    if sg_event_ids:
        st.divider()
        st.header("🟩 SG Tool")

        sg_missing = set(sg_event_ids) & global_missing
        if sg_missing:
            st.warning(f"SG — no data for event ID(s): {sorted(sg_missing)}")

        sg_payloads = {}  # eid -> {"main": {...}, "parking": {...} or None}
        for eid in sg_event_ids:
            if eid not in by_event:
                continue
            rows = by_event[eid]
            sg_payloads[eid] = {
                "main":    build_sg_main_payload(rows),
                "parking": build_sg_parking_payload(rows),
            }

        built_parking = sum(1 for p in sg_payloads.values() if p["parking"])
        st.success(
            f"Built {len(sg_payloads)} SG main payload(s) and "
            f"{built_parking} parking payload(s)"
        )

        for event_id in sg_event_ids:
            bundle = sg_payloads.get(event_id)
            if not bundle:
                continue
            main_p = bundle["main"]
            parking_p = bundle["parking"]

            label = f"**{main_p['eventName']}** — {len(main_p['variants'])} main"
            if parking_p:
                label += f" + {len(parking_p['variants'])} parking"
            else:
                label += " (no parking payload)"

            with st.expander(label, expanded=False):
                m = st.columns(4)
                m[0].metric("SG ProductionID", main_p["ProductionID"] or "—")
                m[1].metric("teEventID",       main_p["teEventID"] or "—")
                m[2].metric("SH Event",        main_p["shEventID"] or "—")
                m[3].metric("Vivid Event",     main_p["vividEventID"] or "—")

                if main_p["variants"]:
                    st.markdown("**🎫 Main Variants**")
                    st.dataframe(
                        [{"Zone": v["variantTitle"], "type": v["variantType"], "mustHave (SG)": v["mustHave"]} for v in main_p["variants"]],
                        use_container_width=True, hide_index=True,
                    )

                st.download_button(
                    label="⬇️ Download SG main payload JSON",
                    data=json.dumps(main_p, indent=2),
                    file_name=f"event_{event_id}_sg_main_payload.json",
                    mime="application/json",
                    key=f"sg_main_dl_{event_id}",
                )

                if parking_p:
                    st.markdown("---")
                    mp2 = st.columns(3)
                    mp2[0].metric("SG Parking ProductionID", parking_p["ProductionID"] or "—")
                    mp2[1].metric("SH Parking Event",        parking_p["shEventID"] or "—")
                    mp2[2].metric("SG Parking Event",        parking_p["sgEventID"] or "—")

                    st.markdown("**🚗 Parking Variants**")
                    st.dataframe(
                        [{"Zone": v["variantTitle"], "type": v["variantType"], "mustHave (SG)": v["mustHave"]} for v in parking_p["variants"]],
                        use_container_width=True, hide_index=True,
                    )
                    st.download_button(
                        label="⬇️ Download SG parking payload JSON",
                        data=json.dumps(parking_p, indent=2),
                        file_name=f"event_{event_id}_sg_parking_payload.json",
                        mime="application/json",
                        key=f"sg_park_dl_{event_id}",
                    )
                else:
                    st.info(
                        "No parking payload built for this event — either there are no "
                        "parking cohorts or the event lacks a PARKING+seatgeek "
                        "marketplace ID."
                    )

        if send_btn and sg_payloads:
            st.subheader("SG API Results")
            summary = {"success": 0, "already_exists": 0, "error": 0, "skipped": 0}

            for event_id in sg_event_ids:
                bundle = sg_payloads.get(event_id)
                if not bundle:
                    continue

                for kind, payload in (("main", bundle["main"]), ("parking", bundle["parking"])):
                    if payload is None:
                        st.info(f"⏭️ Event {event_id} — no {kind} payload to send")
                        summary["skipped"] += 1
                        continue
                    status, body, processed = post_payload(SG_API_URL, payload)
                    label = f"{payload['eventName']} [{kind.upper()}]"
                    if status == 200:
                        st.success(f"✅ **{label}** — {processed} variants processed")
                        summary["success"] += 1
                    elif status == 400:
                        st.warning(f"⚠️ **{label}** — already exists")
                        summary["already_exists"] += 1
                    else:
                        st.error(f"❌ **{label}** — HTTP {status}: {body}")
                        summary["error"] += 1

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("✅ Sent",            summary["success"])
            c2.metric("⚠️ Already Existed", summary["already_exists"])
            c3.metric("❌ Errors",           summary["error"])
            c4.metric("⏭️ Skipped",          summary["skipped"])
