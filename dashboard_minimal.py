"""
Minimal test dashboard to verify Streamlit Cloud deployment works
"""
import streamlit as st

# Page config
try:
    st.set_page_config(
        page_title="Test Dashboard",
        layout="wide"
    )
except:
    pass

# Simple test
st.title("Dashboard Test")
st.write("If you see this, Streamlit is working!")

# Test imports
try:
    import pandas as pd
    st.success("✓ pandas imported")
except Exception as e:
    st.error(f"✗ pandas failed: {e}")

try:
    import plotly.express as px
    st.success("✓ plotly imported")
except Exception as e:
    st.error(f"✗ plotly failed: {e}")

try:
    from google.cloud import storage
    st.success("✓ google.cloud.storage imported")
except Exception as e:
    st.warning(f"⚠ google.cloud.storage not available: {e}")

try:
    from commodity_code_mapping import map_commodity_code_to_sitc_industry
    st.success("✓ commodity_code_mapping imported")
except Exception as e:
    st.warning(f"⚠ commodity_code_mapping not available: {e}")

st.info("This is a minimal test. If this works, we can debug the main dashboard.")

# Show timestamp if pandas is available
try:
    import pandas as pd
    st.write(f"**Deployment Test:** This dashboard was updated at {pd.Timestamp.now()}")
except:
    from datetime import datetime
    st.write(f"**Deployment Test:** This dashboard was updated at {datetime.now()}")

