"""
Event Replicator — batch build and send event payloads to the TE API.
"""

import json
from collections import defaultdict

import requests
import streamlit as st

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    st.error("psycopg2 not installed. Add psycopg2-binary to requirements.txt.")
    st.stop()

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
# DB connection — reads from Streamlit secrets, falls back to env vars
# ---------------------------------------------------------------------------

def get_conn():
    try:
        cfg = st.secrets["postgres"]
        return psycopg2.connect(
            host=cfg["host"],
            port=int(cfg.get("port", 5432)),
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"],
        )
    except (KeyError, FileNotFoundError):
        import os
        return psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", ""),
        )


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
# Batch SQL
# ---------------------------------------------------------------------------

BATCH_QUERY = """
WITH
target_events AS (
    SELECT
        e.id                          AS rtk_event_id,
        e.name                        AS rtk_event_name,
        e.start_date_est,
        e.venue_id,
        e.marketplaces,
        v.sh_venue_id
    FROM public.rtk_events_prod e
    LEFT JOIN public.rtk_venues_prod v ON v.id = e.venue_id
    WHERE e.id = ANY(%(event_ids)s)
      AND e.deleted IS NOT TRUE
),
active_generic_ids AS (
    SELECT DISTINCT generic_cohort_id AS id
    FROM public.rtk_cohorts_prod
    WHERE needs_deleted IS NOT TRUE AND generic_cohort_id IS NOT NULL
),
cohorts AS (
    SELECT
        c.cohort_id, c.cohort_name, c.event_id, c.is_parking,
        c.pack_size, c.status, c.generic_cohort_id
    FROM public.rtk_cohorts_prod c
    JOIN target_events te ON te.rtk_event_id = c.event_id
    WHERE c.needs_deleted IS NOT TRUE
      AND (
        (c.is_parking = true  AND c.pack_size = 1) OR
        (c.is_parking = false AND c.pack_size = 2)
      )
),
variant_sections AS (
    SELECT
        c.cohort_id,
        g.name                                        AS variant_title,
        string_agg(DISTINCT tm->>'name', ',')         AS must_have,
        string_agg(DISTINCT sg->>'name', ',')         AS sg_must_have_section
    FROM cohorts c
    JOIN public.rtk_cohorts_generic_prod g ON g.id = c.generic_cohort_id
    JOIN public.rtk_cohort_sections_prod cs ON cs.id = ANY(g.section_ids)
    ,    jsonb_array_elements(cs.ticketmaster_sections) AS tm
    ,    jsonb_array_elements(cs.seatgeek_sections) AS sg
    WHERE cs.deleted_at IS NULL AND c.is_parking = false
    GROUP BY c.cohort_id, g.name

    UNION ALL

    SELECT
        c.cohort_id,
        g.name                                        AS variant_title,
        string_agg(DISTINCT tm->>'name', ',')         AS must_have,
        string_agg(DISTINCT sg->>'name', ',')         AS sg_must_have_section
    FROM cohorts c
    JOIN target_events te ON te.rtk_event_id = c.event_id
    JOIN public.rtk_cohorts_generic_prod g
        ON g.venue_id = te.venue_id
        AND lower(g.name) = lower(c.cohort_name)
        AND g.id IN (SELECT id FROM active_generic_ids)
    JOIN public.rtk_cohort_sections_prod cs ON cs.id = ANY(g.section_ids)
    ,    jsonb_array_elements(cs.ticketmaster_sections) AS tm
    ,    jsonb_array_elements(cs.seatgeek_sections) AS sg
    WHERE cs.deleted_at IS NULL AND c.is_parking = true
    GROUP BY c.cohort_id, g.name
)
SELECT
    te.rtk_event_id,
    te.rtk_event_name,
    te.start_date_est,
    te.marketplaces,
    te.sh_venue_id,
    c.cohort_id,
    c.cohort_name,
    c.is_parking,
    c.pack_size,
    c.status,
    vs.variant_title,
    vs.must_have,
    vs.sg_must_have_section
FROM target_events te
JOIN cohorts c ON c.event_id = te.rtk_event_id
LEFT JOIN variant_sections vs ON vs.cohort_id = c.cohort_id
ORDER BY te.rtk_event_id, c.cohort_id;
"""


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

# Fetch from DB
@st.cache_data(ttl=60, show_spinner=False)
def fetch_events(event_ids: tuple):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(BATCH_QUERY, {"event_ids": list(event_ids)})
        rows = cur.fetchall()
    conn.close()
    return rows

# Main logic
if preview_btn or send_btn:
    event_ids, bad = parse_ids(raw_input)

    if bad:
        st.warning(f"Skipping non-integer values: {', '.join(bad)}")
    if not event_ids:
        st.error("No valid event IDs found.")
        st.stop()

    with st.spinner(f"Querying database for {len(event_ids)} event(s)..."):
        try:
            all_rows = fetch_events(tuple(sorted(event_ids)))
        except Exception as e:
            st.error(f"Database error: {e}")
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
