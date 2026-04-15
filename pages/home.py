"""
Home page — Cal's Tool Inventory Tracking System
"""
from __future__ import annotations

import streamlit as st
from src.config import load_config, load_credentials

st.title("Cal's Tool Inventory Tracking System")
st.caption("Select a tool from the sidebar or use the cards below.")

# ── Credential status ─────────────────────────────────────────────────────────
try:
    load_config()
    load_credentials()
    st.success("Credentials loaded — ready to run.")
except (FileNotFoundError, EnvironmentError) as e:
    st.error(f"Configuration error: {e}")

st.divider()

# ── Tool cards ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown("### 📋 Missing Events Checker")
    st.markdown(
        "Compare official league schedules — **MLB, NHL, NBA, NFL** — against your "
        "SG and TE tool inventory.  \n"
        "Quickly find any games that are in the schedule but not yet loaded in your tools."
    )
    st.page_link("pages/1_Missing_Events.py", label="Open Missing Events Checker", icon="📋")

with col2:
    st.markdown("### 💰 7's Checker")
    st.markdown(
        "Scan event pricing pages for sections priced at **\\$7.00**. "
        "Tiered from *Perfect! LFG!* all the way to *7 SZN IT IS OVER* "
        "so you know exactly where each event stands."
    )
    st.page_link("pages/2_Sevens_Checker.py", label="Open 7's Checker", icon="💰")
