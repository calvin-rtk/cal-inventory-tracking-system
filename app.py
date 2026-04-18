"""
Cal's Tool Inventory Tracking System — navigation router.
"""
import streamlit as st

st.set_page_config(
    page_title="Cal's Tool Inventory Tracking System",
    page_icon="🏟",
    layout="wide",
    initial_sidebar_state="expanded",
)

pg = st.navigation([
    st.Page("pages/home.py",                title="Home",             icon="🏠", default=True),
    st.Page("pages/1_Missing_Events.py",    title="Missing Events",   icon="📋"),
    st.Page("pages/2_Sevens_Checker.py",    title="7's Checker",      icon="💰"),
    st.Page("pages/3_Event_Replicator.py",  title="Event Replicator", icon="🎟️"),
])
pg.run()
