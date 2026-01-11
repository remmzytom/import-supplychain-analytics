"""
Minimal test dashboard to diagnose Streamlit Cloud issues
"""
import streamlit as st

# Page config
try:
    st.set_page_config(
        page_title="Test Dashboard",
        page_icon=None,
        layout="wide"
    )
except Exception:
    pass

st.title("Test Dashboard")
st.write("If you see this, basic Streamlit is working!")

# Test imports
try:
    import pandas as pd
    st.success("✅ pandas imported")
except Exception as e:
    st.error(f"❌ pandas import failed: {e}")

try:
    import plotly.express as px
    st.success("✅ plotly imported")
except Exception as e:
    st.error(f"❌ plotly import failed: {e}")

try:
    from google.cloud import storage
    st.success("✅ google.cloud.storage imported")
except Exception as e:
    st.warning(f"⚠️ google.cloud.storage import failed: {e}")

try:
    from commodity_code_mapping import map_commodity_code_to_sitc_industry
    st.success("✅ commodity_code_mapping imported")
except Exception as e:
    st.warning(f"⚠️ commodity_code_mapping import failed: {e}")

# Test secrets
try:
    if 'gcp' in st.secrets:
        st.success("✅ GCP secrets found")
    else:
        st.info("ℹ️ GCP secrets not configured")
except Exception as e:
    st.warning(f"⚠️ Secrets access failed: {e}")

st.write("---")
st.write("**If all tests pass, the issue is in the main dashboard code.**")
st.write("**If any test fails, that's the root cause.**")

