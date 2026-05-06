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


# Marker appended to every event name we create. Lets the team identify
# entries that came from this page when looking at the destination tool.
ER_SUFFIX = " - ER"


def format_event_name(start_date_est, event_name, parking=False):
    """Format the event name as `YYYY-MM-DD HH:MMAM/PM <name>[ - PARKING] - ER`.

    The DB returns dates like 'Wed April 26, 2026 7:30 PM'. Strip the
    leading weekday and parse with several known shapes. Date is rendered
    in 12-hour form with no space before the meridiem suffix
    (e.g. '2026-05-11 10:30PM') to match the format the destination tools
    expect. ER suffix is always last so it stays visible regardless of
    whether the parking marker is present."""
    import re
    from datetime import datetime

    raw = (start_date_est or "").strip()
    raw = re.sub(r"^\w+\s+", "", raw, count=1)

    formatted_date = None
    for fmt in ["%B %d, %Y %I:%M %p", "%B %d, %Y %I:%M%p", "%B %d, %Y"]:
        try:
            dt = datetime.strptime(raw, fmt)
            formatted_date = dt.strftime("%Y-%m-%d %I:%M%p")
            break
        except ValueError:
            pass

    head = f"{formatted_date} {event_name}" if formatted_date else f"{raw} {event_name}"
    if parking:
        head += " - PARKING"
    return head + ER_SUFFIX


# Backwards-compatible alias — old name retained so any external scripts
# importing parse_event_name keep working. New code should call
# format_event_name directly.
def parse_event_name(start_date_est, event_name):
    return format_event_name(start_date_est, event_name, parking=False)


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def sg_section_list(raw, is_parking=False):
    """SG section slugs in the database use hyphens where spaces belong
    for ticket sections (e.g. 'rate-club-a' -> 'rate club a'). Both TE
    and SG tools expect the human-readable form with spaces for ticket
    variants.

    Parking variants (variantType=3) keep the raw hyphenated slug
    (e.g. 'parking-lot-b' stays as-is) — that's the format the SG/TE
    parking matchers expect."""
    if not raw:
        return ""
    if is_parking:
        return raw
    return ",".join(s.replace("-", " ") for s in raw.split(","))


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
            # Internal-only key (stripped before POST). Used as a stable
            # identifier for per-variant resolution widgets in the UI.
            "_cohort_id":         r.get("cohort_id"),
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
            "sgMustHaveSection":  sg_section_list(r["sg_must_have_section"], is_parking=is_parking),
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
        "mustHave":           sg_section_list(r.get("sg_must_have_section"), is_parking=is_parking),
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
        "eventName":        format_event_name(first["start_date_est"], first["rtk_event_name"], parking=True),
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

# ---------------------------------------------------------------------------
# Preflight validation
#
# Each tool requires a non-empty ProductionID before we POST. Without one,
# the destination tool can silently accept the payload and create or attach
# garbage records (we hit this on event 55360 — no tradedesk marketplace
# entry, so ProductionID was empty and the TE tool stamped an unrelated
# production ID onto the alerts).
# ---------------------------------------------------------------------------

def validate_te(payload):
    """Return list of blocking-error strings for a TE payload (empty == OK)."""
    errors = []
    if not payload.get("ProductionID"):
        errors.append(
            "missing tradedesk marketplace ID (TE ProductionID is empty) — "
            "verify the TE production ID is attached to this event in the "
            "alerts system before retrying"
        )
    return errors


def variant_has_empty_tm(v):
    """A variant is in the 'empty TM alert' state when mustHave (TM) is
    empty but sgMustHaveSection is populated — the alert that gets created
    has no TM filter, which is almost always wrong."""
    return (not v.get("mustHave")) and bool(v.get("sgMustHaveSection"))


def strip_internal_fields(payload):
    """Return a copy of the payload with internal-only keys (prefixed `_`)
    removed from variants so it can be POSTed cleanly."""
    cleaned = dict(payload)
    cleaned["variants"] = [
        {k: val for k, val in var.items() if not k.startswith("_")}
        for var in payload["variants"]
    ]
    return cleaned


DEFAULT_ACTION = "Send as-is (creates empty TE alert)"
ACTION_OPTIONS = [
    DEFAULT_ACTION,
    "Provide TM sections manually",
    "Omit this variant",
]


def apply_te_resolutions(payload, event_id):
    """Apply user-selected resolutions (stored in session_state under
    `te_action_<eid>_<cohort>` and `te_tm_input_<eid>_<cohort>` keys) to
    a fresh payload. Pure function — no UI rendering. Always safe to call:
    if no resolutions have been submitted, all variants stay as-is.

    Returns (resolved_payload, n_unresolved). `n_unresolved` counts
    variants still in the empty-TM state after resolutions (i.e. user
    chose "Send as-is", or chose "Provide" but didn't type anything)."""
    new_variants = []
    unresolved = 0
    for v in payload["variants"]:
        if not variant_has_empty_tm(v):
            new_variants.append(v)
            continue
        cohort_id = v.get("_cohort_id") or "unknown"
        action = st.session_state.get(
            f"te_action_{event_id}_{cohort_id}", DEFAULT_ACTION
        )
        if action == "Omit this variant":
            continue
        if action == "Provide TM sections manually":
            user_sections = (
                st.session_state.get(f"te_tm_input_{event_id}_{cohort_id}") or ""
            ).strip()
            if user_sections:
                v_resolved = dict(v)
                v_resolved["mustHave"] = user_sections
                new_variants.append(v_resolved)
            else:
                new_variants.append(v)
                unresolved += 1
            continue
        # Send as-is (default)
        new_variants.append(v)
        unresolved += 1

    resolved = dict(payload)
    resolved["variants"] = new_variants
    return resolved, unresolved


def render_te_resolution_form(payload, event_id):
    """Render the per-variant resolution form for a TE event with empty-TM
    warnings. Returns True iff the user clicked the form's submit button
    on this run (the trigger for POSTing this event).

    The form wraps every radio + text input so picks don't trigger reruns.
    Streamlit only reruns when the form's submit button is clicked, at
    which point all batched values are committed to session_state and
    `apply_te_resolutions()` will pick them up on the next render."""
    affected = [v for v in payload["variants"] if variant_has_empty_tm(v)]
    if not affected:
        return False

    with st.form(key=f"resolve_form_{event_id}", clear_on_submit=False):
        st.markdown(
            f"##### {len(affected)} variant(s) need attention — "
            "pick an action and click **Apply & Send** below"
        )
        for v in affected:
            cohort_id = v.get("_cohort_id") or "unknown"
            zone = v.get("variantTitle") or "(untitled)"
            kind = "parking" if v.get("variantType") == 3 else "ticket"
            sg_sections = v.get("sgMustHaveSection") or ""

            st.markdown(
                f"⚠️ **Empty TM `mustHave` for {kind} variant `{zone}`** "
                f"(cohort `{cohort_id}`)"
            )
            st.caption(f"SG sections present: `{sg_sections}`")

            st.radio(
                "How should this variant be handled?",
                options=ACTION_OPTIONS,
                key=f"te_action_{event_id}_{cohort_id}",
                horizontal=True,
            )
            st.text_input(
                "TM sections (only used if you picked 'Provide TM sections manually')",
                key=f"te_tm_input_{event_id}_{cohort_id}",
                placeholder="100,101,102",
            )
            st.markdown("---")

        return st.form_submit_button(
            "✅ Apply & Send to API",
            use_container_width=True,
            type="primary",
        )


def te_soft_warnings(payload):
    """Return list of soft-warning strings for a TE payload — non-blocking,
    surfaced for review.

    The big one: a variant where TM `mustHave` is empty but the SG section
    field is populated. The TE tool builds an alert keyed on the TM field,
    so this creates an "empty alert" — broad / unfilterable, picks up
    inventory the cohort never intended to cover. Source of the problem
    is usually a generic cohort with empty `ticketmaster_sections` upstream
    in venue-mapping; user can fix there or remove the cohort.

    Format: one warning per affected variant, with cohort/zone name so
    it's actionable."""
    warnings = []
    for v in payload.get("variants", []):
        if variant_has_empty_tm(v):
            kind = "parking" if v.get("variantType") == 3 else "ticket"
            zone = v.get("variantTitle") or "(untitled)"
            warnings.append(
                f"empty TM `mustHave` for {kind} variant '{zone}' — SG sections "
                f"are present ({v['sgMustHaveSection']}) but no TM sections."
            )
    return warnings


def validate_sg_main(payload):
    errors = []
    if not payload.get("ProductionID"):
        errors.append(
            "missing seatgeek marketplace ID (SG main ProductionID is empty) — "
            "verify the SG event has a seatgeek production ID attached"
        )
    return errors


def validate_sg_parking(payload):
    errors = []
    if not payload.get("ProductionID"):
        errors.append(
            "missing PARKING+seatgeek marketplace ID — parking event cannot "
            "be created without an SG parking production ID"
        )
    return errors


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
# Workflow state — persists across reruns so form submits don't lose the
# rendered context.
#
# Streamlit reruns the entire script every time the user interacts with a
# widget. The Preview/Send buttons return True only on the rerun where they
# were clicked, so we need to remember which one launched the workflow to
# keep rendering on subsequent reruns (e.g. when a per-event "Apply & Send"
# form is submitted).
#
# Cached POST results live under `te_posted_<eid>` / `sg_posted_<eid>_<kind>`
# so each event posts at most once per workflow.
# ---------------------------------------------------------------------------

if preview_btn or send_btn:
    # Fresh workflow — clear any cached POST results from previous runs
    # so the user sees current data.
    for k in list(st.session_state.keys()):
        if k.startswith("te_posted_") or k.startswith("sg_posted_"):
            del st.session_state[k]
    st.session_state["last_action"] = "send" if send_btn else "preview"

last_action = st.session_state.get("last_action")
# Whether to auto-POST events that have no soft warnings to resolve.
# Send button → True. Preview button → False (preview-only for healthy).
trigger_send_for_healthy = (last_action == "send")


def _show_post_result(res):
    """Render a POST result line (success / already-exists / error)."""
    name = res.get("name", "")
    status = res.get("status")
    body = res.get("body", "")
    processed = res.get("processed", "?")
    if status == 200:
        st.success(f"✅ **{name}** — {processed} variants processed")
    elif status == 400:
        st.warning(f"⚠️ **{name}** — already exists in destination tool")
    else:
        st.error(f"❌ **{name}** — HTTP {status}: {body}")


if last_action:
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
        # Validate every payload up front so the user sees blocking errors
        # before they click Send. Errors-by-event keyed for fast lookup.
        te_errors = {eid: validate_te(p) for eid, p in te_payloads.items()}
        te_blocked = {eid for eid, errs in te_errors.items() if errs}
        # Soft warnings — non-blocking, but surfaced prominently so the user
        # can decide whether to push, fix upstream, or drop variants.
        te_warnings = {eid: te_soft_warnings(p) for eid, p in te_payloads.items()}
        te_warn_count = sum(len(w) for w in te_warnings.values())

        ok_count = len(te_payloads) - len(te_blocked)
        if te_blocked:
            st.error(
                f"⛔ {len(te_blocked)} of {len(te_payloads)} TE payload(s) "
                f"have blocking errors and will NOT be sent: "
                f"{sorted(te_blocked)}"
            )
        if te_warn_count:
            warned_events = sorted({eid for eid, w in te_warnings.items() if w})
            st.warning(
                f"⚠️ {te_warn_count} soft warning(s) across {len(warned_events)} "
                f"event(s) — review before sending: {warned_events}"
            )
        st.success(f"Built {len(te_payloads)} TE payload(s) — {ok_count} ready to send")

        # -------------------------------------------------------------------
        # Per-event preview + POST.
        # Logic:
        #   - Blocked: show error, no preview, no POST.
        #   - Warned: always render preview + resolution form. POST happens
        #             when the user submits the form ("Apply & Send").
        #   - Healthy: under SEND, POST immediately (no preview tables).
        #              Under PREVIEW, render preview only (no POST).
        # -------------------------------------------------------------------
        for event_id in te_event_ids:
            payload = te_payloads.get(event_id)
            if not payload:
                continue
            errs  = te_errors.get(event_id, [])
            warns = te_warnings.get(event_id, [])
            posted_key = f"te_posted_{event_id}"
            already_posted = posted_key in st.session_state

            # ---- Blocked ----
            if errs:
                with st.expander(f"⛔ BLOCKED **{payload['eventName']}** — {len(payload['variants'])} variants", expanded=True):
                    for e in errs:
                        st.error(f"❌ Event {event_id}: {e}")
                continue

            # Apply resolutions (no-op if there are none) so tables and the
            # final POST always reflect the user's choices.
            resolved_payload, unresolved = apply_te_resolutions(payload, event_id)
            te_payloads[event_id] = resolved_payload  # keep dict in sync

            # ---- Healthy (no warnings) under SEND: POST immediately, no preview ----
            if not warns and trigger_send_for_healthy:
                if not already_posted:
                    wire = strip_internal_fields(resolved_payload)
                    with st.spinner(f"Posting event {event_id} to TE API..."):
                        status, body, processed = post_payload(TE_API_URL, wire)
                    st.session_state[posted_key] = {
                        "status": status, "body": body,
                        "processed": processed, "name": resolved_payload["eventName"],
                    }
                _show_post_result(st.session_state[posted_key])
                continue

            # ---- Warned, OR healthy under PREVIEW: render preview UI ----
            badge = ""
            if already_posted:
                res = st.session_state[posted_key]
                if res.get("status") == 200:
                    badge = "✅ SENT "
                elif res.get("status") == 400:
                    badge = "⚠️ ALREADY EXISTS "
                else:
                    badge = "❌ ERROR "
            elif warns:
                badge = "⚠️ REVIEW "

            with st.expander(
                f"{badge}**{resolved_payload['eventName']}** — {len(resolved_payload['variants'])} variants",
                expanded=bool(warns and not already_posted),
            ):
                # Render the resolution form for warned events that haven't
                # been posted yet. The form's submit button is the POST
                # trigger — clicking it commits resolutions AND fires the POST.
                submitted = False
                if warns and not already_posted:
                    submitted = render_te_resolution_form(payload, event_id)
                    if unresolved == 0:
                        st.success(f"All {len(warns)} warning(s) resolved.")
                    else:
                        st.warning(
                            f"{unresolved} variant(s) still in the empty-TM state. "
                            "Sending now will create unfiltered alerts for those."
                        )

                # The form_submit reruns the script. On THAT rerun, submitted
                # is True and resolutions are now in session_state — so
                # apply_te_resolutions() above already produced the final
                # resolved_payload reflecting the user's choices.
                if submitted and not already_posted:
                    wire = strip_internal_fields(resolved_payload)
                    with st.spinner(f"Posting event {event_id} to TE API..."):
                        status, body, processed = post_payload(TE_API_URL, wire)
                    st.session_state[posted_key] = {
                        "status": status, "body": body,
                        "processed": processed, "name": resolved_payload["eventName"],
                    }
                    already_posted = True

                if already_posted:
                    _show_post_result(st.session_state[posted_key])

                m = st.columns(4)
                m[0].metric("ProductionID", resolved_payload["ProductionID"] or "—")
                m[1].metric("SH Event",     resolved_payload["shEventID"] or "—")
                m[2].metric("SG Event",     resolved_payload["sgEventID"] or "—")
                m[3].metric("Vivid Event",  resolved_payload["vividEventID"] or "—")

                tickets = [v for v in resolved_payload["variants"] if v["variantType"] == 1]
                parking = [v for v in resolved_payload["variants"] if v["variantType"] == 3]
                if tickets:
                    st.markdown(f"**🎫 Ticket Variants** ({len(tickets)})")
                    st.dataframe(
                        [{"Zone": v["variantTitle"], "mustHave": v["mustHave"], "sgMustHave": v["sgMustHaveSection"]} for v in tickets],
                        use_container_width=True, hide_index=True,
                    )
                if parking:
                    st.markdown(f"**🚗 Parking Variants** ({len(parking)})")
                    st.dataframe(
                        [{"Zone": v["variantTitle"], "mustHave": v["mustHave"], "sgMustHave": v["sgMustHaveSection"]} for v in parking],
                        use_container_width=True, hide_index=True,
                    )

                st.download_button(
                    label="⬇️ Download TE payload JSON",
                    data=json.dumps(strip_internal_fields(resolved_payload), indent=2),
                    file_name=f"event_{resolved_payload['ProductionID'] or event_id}_te_payload.json",
                    mime="application/json",
                    key=f"te_dl_{event_id}",
                )

        # -------------------------------------------------------------------
        # TE summary metrics — derived from cached POST results so they stay
        # accurate across reruns.
        # -------------------------------------------------------------------
        te_summary = {"success": 0, "already_exists": 0, "error": 0, "blocked": 0, "pending": 0}
        for event_id in te_event_ids:
            if event_id in te_blocked:
                te_summary["blocked"] += 1
                continue
            res = st.session_state.get(f"te_posted_{event_id}")
            if res is None:
                te_summary["pending"] += 1
            elif res["status"] == 200:
                te_summary["success"] += 1
            elif res["status"] == 400:
                te_summary["already_exists"] += 1
            else:
                te_summary["error"] += 1

        cols = st.columns(5)
        cols[0].metric("✅ Sent",            te_summary["success"])
        cols[1].metric("⚠️ Already Existed", te_summary["already_exists"])
        cols[2].metric("❌ Errors",           te_summary["error"])
        cols[3].metric("⛔ Blocked",          te_summary["blocked"])
        cols[4].metric("⏳ Pending",          te_summary["pending"])

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
        sg_errors   = {}  # eid -> {"main": [...], "parking": [...]}
        for eid in sg_event_ids:
            if eid not in by_event:
                continue
            rows = by_event[eid]
            main_p    = build_sg_main_payload(rows)
            parking_p = build_sg_parking_payload(rows)
            sg_payloads[eid] = {"main": main_p, "parking": parking_p}
            sg_errors[eid] = {
                "main":    validate_sg_main(main_p),
                "parking": validate_sg_parking(parking_p) if parking_p else [],
            }

        sg_blocked_main    = {eid for eid, errs in sg_errors.items() if errs["main"]}
        sg_blocked_parking = {eid for eid, errs in sg_errors.items() if sg_payloads[eid]["parking"] and errs["parking"]}
        built_parking = sum(1 for p in sg_payloads.values() if p["parking"])

        if sg_blocked_main or sg_blocked_parking:
            blocks = []
            if sg_blocked_main:    blocks.append(f"main: {sorted(sg_blocked_main)}")
            if sg_blocked_parking: blocks.append(f"parking: {sorted(sg_blocked_parking)}")
            st.error(f"⛔ Blocked SG payload(s): " + " | ".join(blocks))

        st.success(
            f"Built {len(sg_payloads)} SG main payload(s) and "
            f"{built_parking} parking payload(s)"
        )

        # -------------------------------------------------------------------
        # Per-event preview + POST. SG has no soft warnings/forms, so the
        # workflow is simpler than TE:
        #   - Blocked (per kind): show error, skip POST.
        #   - Healthy under SEND: POST main+parking immediately (no preview).
        #   - Healthy under PREVIEW: show preview tables only.
        # -------------------------------------------------------------------
        for event_id in sg_event_ids:
            bundle = sg_payloads.get(event_id)
            if not bundle:
                continue
            main_p    = bundle["main"]
            parking_p = bundle["parking"]
            errs_main    = sg_errors[event_id]["main"]
            errs_parking = sg_errors[event_id]["parking"]
            any_block = bool(errs_main) or bool(errs_parking)

            posted_main_key    = f"sg_posted_{event_id}_main"
            posted_parking_key = f"sg_posted_{event_id}_parking"

            # ---- Healthy under SEND: POST inline, skip preview entirely ----
            if not any_block and trigger_send_for_healthy:
                # Main
                if main_p and posted_main_key not in st.session_state:
                    with st.spinner(f"Posting event {event_id} (main) to SG API..."):
                        status, body, processed = post_payload(SG_API_URL, main_p)
                    st.session_state[posted_main_key] = {
                        "status": status, "body": body, "processed": processed,
                        "name": f"{main_p['eventName']} [MAIN]",
                    }
                if posted_main_key in st.session_state:
                    _show_post_result(st.session_state[posted_main_key])

                # Parking
                if parking_p and posted_parking_key not in st.session_state:
                    with st.spinner(f"Posting event {event_id} (parking) to SG API..."):
                        status, body, processed = post_payload(SG_API_URL, parking_p)
                    st.session_state[posted_parking_key] = {
                        "status": status, "body": body, "processed": processed,
                        "name": f"{parking_p['eventName']} [PARKING]",
                    }
                if posted_parking_key in st.session_state:
                    _show_post_result(st.session_state[posted_parking_key])
                elif not parking_p:
                    st.info(f"⏭️ Event {event_id} — no parking payload to send")
                continue

            # ---- Otherwise: render preview UI ----
            label = f"**{main_p['eventName']}** — {len(main_p['variants'])} main"
            if parking_p:
                label += f" + {len(parking_p['variants'])} parking"
            else:
                label += " (no parking payload)"
            if any_block:
                label = "⛔ BLOCKED " + label

            with st.expander(label, expanded=any_block):
                if errs_main:
                    for e in errs_main:
                        st.error(f"❌ Event {event_id} [MAIN]: {e}")
                if errs_parking:
                    for e in errs_parking:
                        st.error(f"❌ Event {event_id} [PARKING]: {e}")

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

        # -------------------------------------------------------------------
        # SG summary metrics — derived from cached POST results.
        # -------------------------------------------------------------------
        sg_summary = {"success": 0, "already_exists": 0, "error": 0, "blocked": 0, "pending": 0}
        for event_id in sg_event_ids:
            for kind, key in (("main", f"sg_posted_{event_id}_main"),
                              ("parking", f"sg_posted_{event_id}_parking")):
                bundle = sg_payloads.get(event_id)
                if bundle is None:
                    continue
                # Skip parking metric when there's no parking payload for this event.
                if kind == "parking" and bundle["parking"] is None:
                    continue
                if sg_errors[event_id][kind]:
                    sg_summary["blocked"] += 1
                    continue
                res = st.session_state.get(key)
                if res is None:
                    sg_summary["pending"] += 1
                elif res["status"] == 200:
                    sg_summary["success"] += 1
                elif res["status"] == 400:
                    sg_summary["already_exists"] += 1
                else:
                    sg_summary["error"] += 1

        cols = st.columns(5)
        cols[0].metric("✅ Sent",            sg_summary["success"])
        cols[1].metric("⚠️ Already Existed", sg_summary["already_exists"])
        cols[2].metric("❌ Errors",           sg_summary["error"])
        cols[3].metric("⛔ Blocked",          sg_summary["blocked"])
        cols[4].metric("⏳ Pending",          sg_summary["pending"])
