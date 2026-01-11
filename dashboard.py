"""
Streamlit Dashboard for Freight Import Data Analysis
Interactive dashboard displaying all visualizations from the analysis notebook
Updated: 2025-01-10
"""

# Wrap all imports in try-except to prevent silent crashes
try:
    import streamlit as st
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import warnings
    from datetime import datetime
    import sys
    import os
    import tempfile
    import json
    
    # Try to import Google Cloud Storage (optional - for Streamlit Cloud)
    try:
        from google.cloud import storage
        GCS_AVAILABLE = True
    except ImportError:
        GCS_AVAILABLE = False
    
    # Add current directory to path for imports
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # Import commodity mapping (optional - provide fallback)
    try:
        from commodity_code_mapping import map_commodity_code_to_sitc_industry
        COMMODITY_MAPPING_AVAILABLE = True
    except ImportError:
        COMMODITY_MAPPING_AVAILABLE = False
        # Fallback function if mapping is not available
        def map_commodity_code_to_sitc_industry(code):
            return "Unknown"
    
    warnings.filterwarnings('ignore')
except Exception as e:
    # If imports fail, we can't use st.error, so print to stderr
    import sys
    print(f"CRITICAL: Import failed: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    # Try to import streamlit at least to show error
    try:
        import streamlit as st
        st.error(f"Failed to import required modules: {e}")
        st.stop()
    except:
        raise

# Light Theme Function for Plotly Charts
def apply_light_theme(fig):
    """Apply light theme to Plotly charts"""
    fig.update_layout(
        template='plotly_white',
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(color='#262730'),
        title=dict(font=dict(color='#262730')),
        xaxis=dict(gridcolor='#e0e0e0', linecolor='#b0b0b0'),
        yaxis=dict(gridcolor='#e0e0e0', linecolor='#b0b0b0'),
    )
    return fig

# Page configuration (wrap in try-except to prevent crashes)
try:
    st.set_page_config(
        page_title="Freight Import Data Dashboard",
        page_icon=None,
        layout="wide",
        initial_sidebar_state="expanded"
    )
except Exception:
    # Page config already set, ignore
    pass

# Custom CSS (wrap in try-except to prevent crashes)
try:
    st.markdown("""
    <style>
    /* Light Background */
    .main {
        background-color: #ffffff;
    }
    .stApp {
        background-color: #ffffff;
    }
    .block-container {
        background-color: #ffffff;
    }
    body {
        background-color: #ffffff !important;
    }
    html {
        background-color: #ffffff !important;
    }
    
    /* Main Header */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    
    /* Section Headers */
    .section-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 3px solid #1f77b4;
        padding-bottom: 0.5rem;
    }
    
    /* Text Colors */
    p, span, div, label, h1, h2, h3, h4, h5, h6 {
        color: #262730 !important;
    }
    .stMarkdown {
        color: #262730;
    }
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid rgba(31, 119, 180, 0.3);
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
        margin-bottom: 1rem;
        width: 100%;
        height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        box-sizing: border-box;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
        border-color: rgba(31, 119, 180, 0.5);
    }
    .metric-card-title {
        font-size: 0.9rem;
        font-weight: 600;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.5rem;
        line-height: 1.2;
    }
    .metric-card-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1f77b4;
        margin: 0;
        line-height: 1.2;
        word-wrap: break-word;
        overflow-wrap: break-word;
    }
    div[data-testid="stMetricContainer"] {
        background: linear-gradient(135deg, #1e2130 0%, #2d3748 100%) !important;
        padding: 1.5rem !important;
        border-radius: 12px !important;
        border: 1px solid rgba(31, 119, 180, 0.3) !important;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3) !important;
    }
    div[data-testid="stMetricContainer"]:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.4) !important;
        border-color: rgba(31, 119, 180, 0.5) !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700 !important;
        color: #ffffff !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        color: #a0aec0 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Navigation Button */
    .nav-button {
        background-color: #1f77b4;
        color: white;
        padding: 0.5rem 1.5rem;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 1rem;
        margin: 0.5rem;
    }
    .nav-button:hover {
        background-color: #155a8a;
    }
    
    /* Navigation Container */
    .nav-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 1rem;
        background-color: #f8f9fa;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
    }
    [data-testid="stSidebar"] .stMarkdown {
        color: #262730;
    }
    [data-testid="stSidebar"] label {
        color: #262730 !important;
    }
    
    /* Info/Error/Success/Warning Boxes */
    .stInfo {
        background-color: #e7f3ff;
        border-left: 4px solid #1f77b4;
        color: #262730;
    }
    .stError {
        background-color: #ffe7e7;
        border-left: 4px solid #e53e3e;
        color: #262730;
    }
    .stSuccess {
        background-color: #e7f5e7;
        border-left: 4px solid #38a169;
        color: #262730;
    }
    .stWarning {
        background-color: #fff4e7;
        border-left: 4px solid #d69e2e;
        color: #262730;
    }
    
    /* Selectbox and Input Fields */
    [data-baseweb="select"] {
        background-color: #ffffff;
        color: #262730;
    }
    input, textarea, select {
        background-color: #ffffff !important;
        color: #262730 !important;
    }
    
    /* Dataframe */
    .dataframe {
        background-color: #ffffff;
        color: #262730;
    }
    
    /* Horizontal Rule */
    hr {
        border-color: #e0e0e0;
    }
    </style>
""", unsafe_allow_html=True)
except Exception:
    # CSS failed to load, continue without it
    pass

def _load_data_from_gcs_internal(show_progress=False):
    """Internal function to load data from GCS without Streamlit widgets
    This can be called from cached functions
    """
    try:
        # Get GCS configuration from Streamlit secrets
        if 'gcp' not in st.secrets:
            if show_progress:
                st.error("GCP configuration not found in Streamlit secrets. Please configure GCS access.")
            return None
        
        gcp_config = st.secrets['gcp']
        bucket_name = gcp_config.get('bucket_name', 'freight-import-data')
        file_name = gcp_config.get('file_name', 'imports_2024_2025_cleaned.csv')
        
        # Get credentials from secrets
        if 'credentials' not in gcp_config:
            if show_progress:
                st.error("GCP credentials not found in Streamlit secrets.")
            return None
        
        # Create credentials dict from secrets
        credentials_dict = dict(gcp_config['credentials'])
        
        # Create temporary file for credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as creds_file:
            json.dump(credentials_dict, creds_file)
            creds_path = creds_file.name
        
        try:
            # Initialize GCS client with credentials
            if show_progress:
                st.info(f"Authenticating with Google Cloud Storage...")
            client = storage.Client.from_service_account_json(creds_path)
            
            if show_progress:
                st.info(f"Accessing bucket: `{bucket_name}`")
            bucket = client.bucket(bucket_name)
            
            if show_progress:
                st.info(f"Looking for file: `{file_name}`")
            blob = bucket.blob(file_name)
            
            # Check if file exists
            if not blob.exists():
                if show_progress:
                    st.error(f"File `{file_name}` not found in bucket `{bucket_name}`")
                    st.info("""
                    **Possible solutions:**
                    - Verify the file name in Streamlit secrets matches the file in GCS
                    - Check if the automation has uploaded the file successfully
                    - Verify the bucket name is correct
                    """)
                return None
            
            # Show loading progress
            file_size = blob.size
            if show_progress:
                if file_size is not None:
                    st.info(f"File size: {file_size / (1024*1024):.2f} MB")
                else:
                    st.info("File size: Unknown")
            
            # Download to temporary file
            if show_progress:
                progress_bar = st.progress(0)
                st.info(f"Downloading `{file_name}` from Google Cloud Storage...")
            
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                blob.download_to_filename(tmp_file.name)
                tmp_path = tmp_file.name
            
            if show_progress:
                progress_bar.progress(100)
                st.success("File downloaded successfully!")
            
            # Load data from temporary file
            if show_progress:
                st.info("Loading data into memory...")
            df = load_data_from_file(tmp_path)
            
            if show_progress:
                st.success(f"Data loaded successfully! ({len(df):,} rows)")
            
            return df
        finally:
            # Clean up credentials file
            if os.path.exists(creds_path):
                os.unlink(creds_path)
            # Clean up data file after loading
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
                
    except Exception as e:
        if show_progress:
            import traceback
            error_details = traceback.format_exc()
            st.error("Error loading data from Google Cloud Storage")
            st.error(f"Error: {str(e)}")
            st.error("Error Type: " + type(e).__name__)
            
            # Show more details in expander
            with st.expander("Click to see detailed error information"):
                st.code(error_details)
            
            st.info("""
            **Troubleshooting Steps:**
            
            1. **Check Streamlit Secrets:**
               - Go to Streamlit Cloud → Settings → Secrets
               - Verify `gcp.bucket_name` and `gcp.file_name` are set correctly
               - Verify `gcp.credentials` contains valid service account JSON
            
            2. **Verify GCS Bucket:**
               - Bucket name: `freight-import-data`
               - File name: `imports_2024_2025_cleaned.csv` (or as configured)
               - File should exist in the bucket
            
            3. **Check Service Account Permissions:**
               - Service account needs `Storage Object Viewer` role to read files
               - Go to GCS → Bucket → Permissions → Verify service account has access
            
            4. **Verify Credentials Format:**
               - Credentials should be a JSON object (not a string)
               - Should include: `type`, `project_id`, `private_key_id`, `private_key`, `client_email`, etc.
            """)
        
        return None

def load_data_from_gcs():
    """Load data from Google Cloud Storage (with progress indicators)"""
    return _load_data_from_gcs_internal(show_progress=True)

def load_data_from_file(file_path):
    """Load and process data from a CSV file"""
    chunk_size = 100000
    chunks = []
    total_rows = 0
    batch_size = 5
    
    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), 1):
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if len(chunks) >= batch_size:
                if 'df' not in locals():
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.concat([df] + chunks, ignore_index=True)
                chunks = []
                import gc
                gc.collect()
        
        # Combine remaining chunks
        if chunks:
            if 'df' not in locals():
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.concat([df] + chunks, ignore_index=True)
        
        # Add industry sector if not present
        if 'industry_sector' not in df.columns:
            df['industry_sector'] = df['commodity_code'].apply(map_commodity_code_to_sitc_industry)
        
        # Create date column for time series
        df['date'] = pd.to_datetime(
            df['year'].astype(str) + '-' + 
            df['month_number'].astype(str).str.zfill(2) + '-01'
        )
        
        return df
    
    except Exception as e:
        st.error(f"Error loading data from file: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None

@st.cache_data
def load_data():
    """Load and cache the cleaned import data
    Tries local file first, then Google Cloud Storage if available
    NOTE: This function cannot contain widget commands (like st.file_uploader)
    NOTE: Cannot access st.secrets directly in cached functions - must pass as parameter
    """
    import sys
    print("load_data() called", file=sys.stderr)
    
    # Try local file first (for local development)
    data_path = 'data/imports_2024_2025_cleaned.csv'
    print(f"Checking for local file: {data_path}", file=sys.stderr)
    
    try:
        if os.path.exists(data_path):
            print(f"Local file found, loading...", file=sys.stderr)
            return load_data_from_file(data_path)
        else:
            print(f"Local file not found", file=sys.stderr)
    except Exception as e:
        print(f"ERROR loading local file: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Silently fail and try GCS
        pass
    
    # If local file not found, try Google Cloud Storage
    # NOTE: Cannot access st.secrets in cached function - return None and let non-cached function handle it
    print(f"GCS_AVAILABLE: {GCS_AVAILABLE}", file=sys.stderr)
    print("Cannot access st.secrets in cached function - returning None", file=sys.stderr)
    
    # If both fail, return None (main() will handle the error and file uploader)
    return None

def load_data_with_fallback():
    """Load data with fallback to file uploader
    This function handles widgets and should not be cached
    Can access st.secrets here since it's not cached
    """
    import sys
    print("load_data_with_fallback() called", file=sys.stderr)
    
    # Try to load data (cached function - only checks local file)
    try:
        print("Calling load_data()...", file=sys.stderr)
        df = load_data()
        print(f"load_data() returned: {df is not None}, length={len(df) if df is not None else 0}", file=sys.stderr)
        
        # If data loaded successfully, return it
        if df is not None and len(df) > 0:
            print("Data loaded successfully from local file, returning dataframe", file=sys.stderr)
            return df
        else:
            print("Data is None or empty from load_data()", file=sys.stderr)
    except Exception as e:
        print(f"ERROR in load_data(): {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        st.warning(f"Error loading data: {str(e)}")
    
    # Try GCS (can access secrets here since this function is not cached)
    print("Attempting to load from GCS...", file=sys.stderr)
    if GCS_AVAILABLE:
        try:
            print("Checking for GCS secrets...", file=sys.stderr)
            # Can access secrets here since this function is not cached
            if 'gcp' in st.secrets:
                print("GCP secrets found, calling _load_data_from_gcs_internal()...", file=sys.stderr)
                gcs_data = _load_data_from_gcs_internal(show_progress=False)
                print(f"_load_data_from_gcs_internal() returned: {gcs_data is not None}", file=sys.stderr)
                if gcs_data is not None and len(gcs_data) > 0:
                    print(f"GCS data loaded successfully, length: {len(gcs_data)}", file=sys.stderr)
                    return gcs_data
            else:
                print("GCP secrets not found", file=sys.stderr)
        except Exception as e:
            print(f"ERROR loading from GCS: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
    
    # If data not found, show error and offer file uploader
    print("No data found, showing file uploader", file=sys.stderr)
    st.error("**Data file not found**")
    st.info("""
    **For local development:**
    - Ensure `data/imports_2024_2025_cleaned.csv` exists in your project directory
    
    **For Streamlit Cloud:**
    - Configure Google Cloud Storage in Streamlit secrets (see GCS_SETUP_GUIDE.md)
    - Or upload the data file using the file uploader below
    """)
    
    # Offer file uploader as fallback (this is outside cached function)
    uploaded_file = st.file_uploader("Upload cleaned data file (CSV)", type=['csv'])
    if uploaded_file is not None:
        try:
            print("User uploaded file, loading...", file=sys.stderr)
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name
            return load_data_from_file(tmp_path)
        except Exception as e:
            print(f"ERROR loading uploaded file: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            st.error(f"Error loading uploaded file: {str(e)}")
            return None
    
    return None

def main():
    """Main dashboard application"""
    import sys
    
    print("main() function called", file=sys.stderr)
    
    # Header
    try:
        print("Displaying header...", file=sys.stderr)
        st.markdown('<h1 class="main-header">Freight Import Data Dashboard</h1>', unsafe_allow_html=True)
        st.markdown("---")
        print("Header displayed successfully", file=sys.stderr)
    except Exception as e:
        print(f"ERROR displaying header: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        raise
    
    # Load data with error handling
    try:
        print("Calling load_data_with_fallback()...", file=sys.stderr)
        df = load_data_with_fallback()
        print(f"load_data_with_fallback() returned: df is {df is not None}, length={len(df) if df is not None else 0}", file=sys.stderr)
        
        # Check if data was loaded successfully
        if df is None or len(df) == 0:
            print("Data is None or empty, showing warning", file=sys.stderr)
            st.warning("⚠️ No data available. Please configure data source or upload a file.")
            return
        
        print(f"Data loaded: {len(df)} rows, columns: {list(df.columns)[:5]}...", file=sys.stderr)
        
        # Validate required columns exist
        print("Validating required columns...", file=sys.stderr)
        required_columns = ['year', 'month', 'country_description', 'valuecif', 'valuefob', 'weight']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing columns: {missing_columns}", file=sys.stderr)
            st.error(f"⚠️ Missing required columns: {', '.join(missing_columns)}")
            st.info("Please ensure your data file contains all required columns.")
            return
        
        print("Column validation passed", file=sys.stderr)
            
    except Exception as e:
        print(f"ERROR in data loading/validation: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        st.error(f"⚠️ Error loading or processing data: {str(e)}")
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Year filter
    try:
        available_years = sorted(df['year'].unique())
    except Exception as e:
        st.error(f"Error accessing year column: {str(e)}")
        return
    selected_years = st.sidebar.multiselect(
        "Select Years",
        options=available_years,
        default=available_years
    )
    
    # Month filter
    available_months = sorted(df['month'].unique())
    selected_months = st.sidebar.multiselect(
        "Select Months",
        options=available_months,
        default=available_months
    )
    
    # Country filter
    top_countries = df.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(20).index.tolist()
    selected_countries = st.sidebar.multiselect(
        "Select Countries (Top 20)",
        options=top_countries,
        default=[]
    )
    
    # Apply filters
    df_filtered = df.copy()
    if selected_years:
        df_filtered = df_filtered[df_filtered['year'].isin(selected_years)]
    if selected_months:
        df_filtered = df_filtered[df_filtered['month'].isin(selected_months)]
    if selected_countries:
        df_filtered = df_filtered[df_filtered['country_description'].isin(selected_countries)]
    
    # Table of Contents for quick navigation
    st.markdown("---")
    st.markdown("### Table of Contents")
    toc_cols = st.columns(4)
    
    sections_list = [
        ("Overview", "overview"),
        ("Time Series", "time-series"),
        ("Geographic", "geographic"),
        ("Commodity", "commodity"),
        ("Value vs Volume", "value-volume"),
        ("Risk Analysis", "risk"),
        ("Transport Mode", "transport"),
        ("Key Insights", "insights")
    ]
    
    for idx, (name, anchor) in enumerate(sections_list):
        with toc_cols[idx % 4]:
            st.markdown(f"[{name}](#{anchor})")
    
    st.markdown("---")
    
    # Display all sections on the same page
    
    # 1. Overview Section
    st.markdown('<div id="overview"></div>', unsafe_allow_html=True)
    show_overview(df_filtered)
    
    st.markdown("---")
    
    # 2. Time Series Analysis Section
    st.markdown('<div id="time-series"></div>', unsafe_allow_html=True)
    show_time_series(df_filtered)
    
    st.markdown("---")
    
    # 3. Geographic Analysis Section
    st.markdown('<div id="geographic"></div>', unsafe_allow_html=True)
    show_geographic_analysis(df_filtered)
    
    st.markdown("---")
    
    # 4. Commodity Analysis Section
    st.markdown('<div id="commodity"></div>', unsafe_allow_html=True)
    show_commodity_analysis(df_filtered)
    
    st.markdown("---")
    
    # 5. Value vs Volume Analysis Section
    st.markdown('<div id="value-volume"></div>', unsafe_allow_html=True)
    show_value_volume_analysis(df_filtered)
    
    st.markdown("---")
    
    # 6. Risk Analysis Section
    st.markdown('<div id="risk"></div>', unsafe_allow_html=True)
    show_risk_analysis(df_filtered)
    
    st.markdown("---")
    
    # 7. Transport Mode Analysis Section
    st.markdown('<div id="transport"></div>', unsafe_allow_html=True)
    show_transport_mode_analysis(df_filtered)
    
    st.markdown("---")
    
    # 8. Key Insights Section
    st.markdown('<div id="insights"></div>', unsafe_allow_html=True)
    show_key_insights(df_filtered)

def show_overview(df):
    """Display overview metrics"""
    st.markdown('<h2 class="section-header">Overview</h2>', unsafe_allow_html=True)
    
    # Key metrics in cards
    col1, col2, col3, col4 = st.columns(4)
    
    total_fob = df['valuefob'].sum()
    total_cif = df['valuecif'].sum()
    total_weight = df['weight'].sum()
    total_records = len(df)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">Total Records</div>
            <div class="metric-card-value">{total_records:,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">Total FOB Value</div>
            <div class="metric-card-value">${total_fob/1e9:.2f}B</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">Total CIF Value</div>
            <div class="metric-card-value">${total_cif/1e9:.2f}B</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-card-title">Total Weight</div>
            <div class="metric-card-value">{total_weight/1e6:.2f}M tonnes</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Year range
    year_range = f"{df['year'].min():.0f} - {df['year'].max():.0f}"
    st.info(f"Date Range: {year_range}")
    
    # Quick summary charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 5 countries
        top_countries = df.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(5)
        fig = px.bar(
            x=top_countries.values / 1e9,
            y=top_countries.index,
            orientation='h',
            title="Top 5 Countries by Import Value (CIF)",
            labels={'x': 'Value (Billions AUD)', 'y': 'Country'}
        )
        fig = apply_light_theme(fig)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top 5 commodities
        top_commodities_df = df.groupby('commodity_description')['valuecif'].sum().sort_values(ascending=False).head(5).reset_index()
        top_commodities_df['commodity_label'] = top_commodities_df['commodity_description'].apply(
            lambda x: x[:75] + '...' if len(x) > 75 else x
        )
        top_commodities_df['value_billions'] = top_commodities_df['valuecif'] / 1e9
        fig = px.bar(
            top_commodities_df,
            x='value_billions',
            y='commodity_label',
            orientation='h',
            title="Top 5 Commodities by Import Value (CIF)",
            labels={'value_billions': 'Value (Billions AUD)', 'commodity_label': 'Commodity'},
            color='value_billions',
            color_continuous_scale='Blues',
            hover_data={'commodity_description': True, 'commodity_label': False}
        )
        fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>Value: %{x:.2f} Billion AUD<extra></extra>',
                          customdata=top_commodities_df[['commodity_description']].values)
        fig = apply_light_theme(fig)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def show_time_series(df):
    """Display time series analysis"""
    st.markdown('<h2 class="section-header">Time Series Analysis</h2>', unsafe_allow_html=True)
    
    # Monthly trends
    monthly_stats = df.groupby(['year', 'month_number', 'month']).agg({
        'valuefob': 'sum',
        'valuecif': 'sum',
        'weight': 'sum',
        'quantity': 'sum'
    }).reset_index()
    
    monthly_stats = monthly_stats.sort_values(['year', 'month_number'])
    monthly_stats['date'] = pd.to_datetime(
        monthly_stats['year'].astype(str) + '-' + 
        monthly_stats['month_number'].astype(str).str.zfill(2) + '-01'
    )
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Monthly FOB Value', 'Monthly CIF Value', 'Monthly Weight', 'Monthly Quantity'),
        vertical_spacing=0.12
    )
    
    # FOB Value
    fig.add_trace(
        go.Scatter(x=monthly_stats['date'], y=monthly_stats['valuefob']/1e9,
                   mode='lines+markers', name='FOB', line=dict(color='#1f77b4', width=2)),
        row=1, col=1
    )
    
    # CIF Value
    fig.add_trace(
        go.Scatter(x=monthly_stats['date'], y=monthly_stats['valuecif']/1e9,
                   mode='lines+markers', name='CIF', line=dict(color='orange', width=2)),
        row=1, col=2
    )
    
    # Weight
    fig.add_trace(
        go.Scatter(x=monthly_stats['date'], y=monthly_stats['weight']/1e6,
                   mode='lines+markers', name='Weight', line=dict(color='green', width=2)),
        row=2, col=1
    )
    
    # Quantity
    fig.add_trace(
        go.Scatter(x=monthly_stats['date'], y=monthly_stats['quantity']/1e6,
                   mode='lines+markers', name='Quantity', line=dict(color='red', width=2)),
        row=2, col=2
    )
    
    fig.update_xaxes(title_text="Month", row=2, col=1)
    fig.update_xaxes(title_text="Month", row=2, col=2)
    fig.update_yaxes(title_text="Value (Billions AUD)", row=1, col=1)
    fig.update_yaxes(title_text="Value (Billions AUD)", row=1, col=2)
    fig.update_yaxes(title_text="Weight (Millions Tonnes)", row=2, col=1)
    fig.update_yaxes(title_text="Quantity (Millions)", row=2, col=2)
    
    fig.update_layout(height=800, showlegend=False, title_text="Monthly Import Trends")
    st.plotly_chart(fig, use_container_width=True)
    
    # Year-over-Year comparison
    st.subheader("Year-over-Year Comparison")
    
    yearly_stats = df.groupby('year').agg({
        'valuefob': 'sum',
        'valuecif': 'sum',
        'weight': 'sum',
        'quantity': 'sum'
    }).reset_index()
    
    if len(yearly_stats) > 1:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                yearly_stats,
                x='year',
                y=['valuefob', 'valuecif'],
                barmode='group',
                title="Year-over-Year Comparison: FOB vs CIF",
                labels={'value': 'Value (AUD)', 'year': 'Year'},
                color_discrete_map={'valuefob': '#1f77b4', 'valuecif': 'orange'}
            )
            fig.update_layout(yaxis_title="Value (AUD)")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Growth rates
            if len(yearly_stats) == 2:
                growth_data = []
                for col in ['valuefob', 'valuecif', 'weight', 'quantity']:
                    values = yearly_stats[col].values
                    if len(values) == 2 and values[0] > 0:
                        growth = ((values[1] - values[0]) / values[0]) * 100
                        growth_data.append({
                            'Metric': col.upper(),
                            'Growth Rate (%)': growth
                        })
                
                growth_df = pd.DataFrame(growth_data)
                fig = px.bar(
                    growth_df,
                    x='Metric',
                    y='Growth Rate (%)',
                    title="Year-over-Year Growth Rates",
                    color='Growth Rate (%)',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig, use_container_width=True)

def show_geographic_analysis(df):
    """Display geographic analysis"""
    st.markdown('<h2 class="section-header">🌍 Geographic Analysis</h2>', unsafe_allow_html=True)
    
    # Top Countries
    st.subheader("Top Countries by Import Value")
    
    country_stats = df.groupby('country_description').agg({
        'valuecif': 'sum',
        'weight': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    
    country_stats['valuecif_pct'] = (country_stats['valuecif'] / country_stats['valuecif'].sum()) * 100
    
    top_n = st.slider("Select number of top countries", 5, 30, 15)
    
    col1, col2 = st.columns(2)
    
    with col1:
        top_countries = country_stats.head(top_n)
        fig = px.bar(
            top_countries,
            x='valuecif',
            y='country_description',
            orientation='h',
            title=f"Top {top_n} Countries by CIF Value",
            labels={'valuecif': 'Value (AUD)', 'country_description': 'Country'},
            color='valuecif',
            color_continuous_scale='Blues'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        top_countries_weight = country_stats.nlargest(top_n, 'weight')
        fig = px.bar(
            top_countries_weight,
            x='weight',
            y='country_description',
            orientation='h',
            title=f"Top {top_n} Countries by Weight",
            labels={'weight': 'Weight (Tonnes)', 'country_description': 'Country'},
            color='weight',
            color_continuous_scale='Oranges'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig, use_container_width=True)
    
    # Port Analysis
    st.subheader("Port Analysis")
    
    # Australian Ports
    ausport_stats = df.groupby('ausport_description').agg({
        'valuecif': 'sum',
        'weight': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        top_ports = ausport_stats.head(15)
        fig = px.bar(
            top_ports,
            x='valuecif',
            y='ausport_description',
            orientation='h',
            title="Top 15 Australian Ports by CIF Value",
            labels={'valuecif': 'Value (AUD)', 'ausport_description': 'Port'},
            color='valuecif',
            color_continuous_scale='Purples'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Origin Ports
        osport_stats = df.groupby('osport_description').agg({
            'valuecif': 'sum',
            'weight': 'sum'
        }).reset_index().sort_values('valuecif', ascending=False)
        
        top_osports = osport_stats.head(15)
        fig = px.bar(
            top_osports,
            x='valuecif',
            y='osport_description',
            orientation='h',
            title="Top 15 Origin Ports by CIF Value",
            labels={'valuecif': 'Value (AUD)', 'osport_description': 'Port'},
            color='valuecif',
            color_continuous_scale='Greens'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # State Analysis
    st.subheader("Import Value by State")
    
    state_stats = df.groupby('state').agg({
        'valuecif': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    
    state_stats['valuecif_pct'] = (state_stats['valuecif'] / state_stats['valuecif'].sum()) * 100
    
    fig = px.bar(
        state_stats,
        x='state',
        y='valuecif',
        title="Import Value by State (CIF)",
        labels={'valuecif': 'Value (AUD)', 'state': 'State'},
        color='valuecif',
        color_continuous_scale='Viridis'
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Port-Country Matrix
    st.subheader("Port-Country Matrix")
    
    top_10_ports = ausport_stats.head(10)['ausport_description'].tolist()
    top_15_countries = country_stats.head(15)['country_description'].tolist()
    
    port_country_data = df[
        (df['ausport_description'].isin(top_10_ports)) & 
        (df['country_description'].isin(top_15_countries))
    ].copy()
    
    port_country_matrix = port_country_data.groupby(['ausport_description', 'country_description']).agg({
        'valuecif': 'sum'
    }).reset_index()
    
    port_country_pivot = port_country_matrix.pivot_table(
        index='ausport_description',
        columns='country_description',
        values='valuecif',
        fill_value=0
    ) / 1e9  # Convert to billions
    
    fig = px.imshow(
        port_country_pivot.values,
        labels=dict(x="Country", y="Australian Port", color="Value (Billions AUD)"),
        x=port_country_pivot.columns,
        y=port_country_pivot.index,
        color_continuous_scale='YlOrRd',
        aspect="auto",
        title="Port-Country Matrix: Import Value Heatmap"
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # Country Concentration and Diversity Analysis
    st.subheader("Port Concentration and Diversity Analysis")
    
    # Create port_country_matrix for ALL ports and ALL countries (matching notebook logic)
    port_country_matrix = df.groupby(['ausport_description', 'country_description']).agg({
        'valuecif': 'sum'
    }).reset_index()
    
    # Calculate percentage of each country for each port using transform
    port_totals = port_country_matrix.groupby('ausport_description')['valuecif'].transform('sum')
    port_country_matrix['pct_of_port'] = (port_country_matrix['valuecif'] / port_totals * 100).round(2)
    
    # Create port_concentration dataframe
    # Use 'nunique' to count UNIQUE countries, not row count
    # Use 'max' to get top country percentage (no order-dependent iloc[0])
    port_concentration = port_country_matrix.groupby('ausport_description').agg({
        'valuecif': 'sum',
        'country_description': 'nunique',  # Count unique countries, not rows
        'pct_of_port': 'max'  # Maximum percentage = top country share
    }).reset_index()
    port_concentration.columns = ['port', 'total_value', 'num_countries', 'top_country_pct']
    port_concentration = port_concentration.sort_values('total_value', ascending=False)
    
    # Get top 10 ports by total value
    top_10_ports_concentration = port_concentration.head(10).copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top Country Concentration Chart
        fig = px.bar(
            top_10_ports_concentration,
            x='top_country_pct',
            y='port',
            orientation='h',
            title="Top Country Concentration by Port<br>(Higher % = More Concentrated)",
            labels={'top_country_pct': 'Top Country Share (%)', 'port': 'Port'},
            color='top_country_pct',
            color_continuous_scale='Blues',
            text=top_10_ports_concentration['top_country_pct'].apply(lambda x: f"{x:.1f}%")
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Country Diversity Chart
        fig = px.bar(
            top_10_ports_concentration,
            x='num_countries',
            y='port',
            orientation='h',
            title="Country Diversity by Port<br>(Higher = More Diverse)",
            labels={'num_countries': 'Number of Countries', 'port': 'Port'},
            color='num_countries',
            color_continuous_scale='Oranges',
            text=top_10_ports_concentration['num_countries'].apply(lambda x: f"{int(x)}")
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)

def show_commodity_analysis(df):
    """Display commodity analysis"""
    st.markdown('<h2 class="section-header"> Commodity Analysis</h2>', unsafe_allow_html=True)
    
    commodity_stats = df.groupby('commodity_description').agg({
        'valuefob': 'sum',
        'valuecif': 'sum',
        'weight': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    
    commodity_stats['valuecif_pct'] = (commodity_stats['valuecif'] / commodity_stats['valuecif'].sum()) * 100
    
    top_n = st.slider("Select number of top commodities", 5, 30, 15)
    
    # Top commodities by value
    st.subheader("Top Commodities by Value")
    
    top_commodities = commodity_stats.head(top_n).copy()
    top_commodities['commodity_label'] = top_commodities['commodity_description'].apply(
        lambda x: x[:75] + '...' if len(x) > 75 else x
    )
    top_commodities['value_billions'] = top_commodities['valuecif'] / 1e9
    
    fig = px.bar(
        top_commodities,
        x='value_billions',
        y='commodity_label',
        orientation='h',
        title=f"Top {top_n} Commodities by CIF Value",
        labels={'value_billions': 'Value (Billions AUD)', 'commodity_label': 'Commodity'},
        color='value_billions',
        color_continuous_scale='Purples',
        hover_data={'commodity_description': True, 'commodity_label': False}
    )
    fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>Value: %{x:.2f} Billion AUD<br>Percentage: %{customdata[1]:.2f}%<extra></extra>',
                      customdata=top_commodities[['commodity_description', 'valuecif_pct']].values)
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top commodities by weight
    st.subheader("Top Commodities by Weight")
    
    top_commodities_weight = commodity_stats.nlargest(top_n, 'weight').copy()
    top_commodities_weight['commodity_label'] = top_commodities_weight['commodity_description'].apply(
        lambda x: x[:75] + '...' if len(x) > 75 else x
    )
    top_commodities_weight['weight_millions'] = top_commodities_weight['weight'] / 1e6
    
    fig = px.bar(
        top_commodities_weight,
        x='weight_millions',
        y='commodity_label',
        orientation='h',
        title=f"Top {top_n} Commodities by Weight",
        labels={'weight_millions': 'Weight (Millions Tonnes)', 'commodity_label': 'Commodity'},
        color='weight_millions',
        color_continuous_scale='YlOrBr',
        hover_data={'commodity_description': True, 'commodity_label': False}
    )
    # Prepare customdata for hover template
    hover_customdata = [[row['commodity_description'], row['valuecif']/1e9] 
                        for _, row in top_commodities_weight.iterrows()]
    fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>Weight: %{x:.2f} Million Tonnes<br>Value: %{customdata[1]:.2f} Billion AUD<extra></extra>',
                      customdata=hover_customdata)
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # CIF vs FOB Comparison
    st.subheader("CIF vs FOB Comparison for Top Commodities")
    
    top_10_comm = commodity_stats.head(10).copy()
    top_10_comm['commodity_label'] = top_10_comm['commodity_description'].apply(
        lambda x: x[:75] + '...' if len(x) > 75 else x
    )
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='CIF Value',
        x=top_10_comm['commodity_label'],
        y=top_10_comm['valuecif'] / 1e9,
        marker_color='steelblue',
        hovertemplate='<b>%{customdata}</b><br>CIF Value: %{y:.2f} Billion AUD<extra></extra>',
        customdata=top_10_comm['commodity_description']
    ))
    
    fig.add_trace(go.Bar(
        name='FOB Value',
        x=top_10_comm['commodity_label'],
        y=top_10_comm['valuefob'] / 1e9,
        marker_color='coral',
        hovertemplate='<b>%{customdata}</b><br>FOB Value: %{y:.2f} Billion AUD<extra></extra>',
        customdata=top_10_comm['commodity_description']
    ))
    
    fig.update_layout(
        title="Top 10 Commodities: CIF vs FOB Comparison",
        xaxis_title="Commodity",
        yaxis_title="Value (Billions AUD)",
        barmode='group',
        height=500,
        xaxis=dict(tickangle=45)
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # SITC Industry Analysis
    st.subheader("SITC-Based Industry Analysis")
    
    if 'industry_sector' in df.columns:
        sector_analysis = df.groupby('industry_sector').agg({
            'valuecif': 'sum',
            'weight': 'sum'
        }).reset_index().sort_values('valuecif', ascending=False)
        
        sector_analysis['valuecif_billions'] = sector_analysis['valuecif'] / 1e9
        sector_analysis['valuecif_pct'] = (sector_analysis['valuecif'] / sector_analysis['valuecif'].sum()) * 100
        
        fig = px.bar(
            sector_analysis,
            x='valuecif_billions',
            y='industry_sector',
            orientation='h',
            title="Import Value by SITC-Based Industry",
            labels={'valuecif_billions': 'Value (Billions AUD)', 'industry_sector': 'Industry'},
            color='valuecif_billions',
            color_continuous_scale='Viridis',
            text=sector_analysis['valuecif_pct'].apply(lambda x: f"{x:.1f}%")
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig, use_container_width=True)

def show_value_volume_analysis(df):
    """Display value vs volume analysis"""
    st.markdown('<h2 class="section-header">Value vs Volume Analysis</h2>', unsafe_allow_html=True)
    
    if 'industry_sector' not in df.columns:
        df['industry_sector'] = df['commodity_code'].apply(map_commodity_code_to_sitc_industry)
    
    # Industry-level analysis
    industry_analysis = df.groupby('industry_sector').agg({
        'valuecif': 'sum',
        'weight': 'sum'
    }).reset_index()
    
    industry_analysis['value_per_tonne'] = (
        industry_analysis['valuecif'] / industry_analysis['weight'].replace(0, np.nan)
    )
    industry_analysis['total_value_billions'] = industry_analysis['valuecif'] / 1e9
    industry_analysis['total_weight_millions'] = industry_analysis['weight'] / 1e6
    
    industry_analysis = industry_analysis[industry_analysis['weight'] > 0].copy()
    
    volume_threshold = industry_analysis['total_value_billions'].median()
    premium_threshold = industry_analysis['value_per_tonne'].median()
    
    # Classify industries
    industry_analysis['category'] = 'Standard'
    market_leaders_mask = industry_analysis['total_value_billions'] >= volume_threshold
    industry_analysis.loc[market_leaders_mask, 'category'] = 'Market Leader'
    
    premium_mask = (industry_analysis['total_value_billions'] < volume_threshold) & \
                   (industry_analysis['value_per_tonne'] >= premium_threshold)
    industry_analysis.loc[premium_mask, 'category'] = 'Premium Product'
    
    # Quadrant Visualization
    st.subheader("Market Leaders vs Premium Products")
    
    if len(industry_analysis) > 0:
        # Scale size for better visualization
        industry_analysis['size_scaled'] = industry_analysis['total_value_billions'] * 10
        
        fig = px.scatter(
            industry_analysis,
            x='total_value_billions',
            y='value_per_tonne',
            size='size_scaled',
            color='category',
            hover_name='industry_sector',
            title="Industry Analysis: Market Leaders vs Premium Products",
            labels={
                'total_value_billions': 'Total Value (Billions AUD) - Volume Indicator',
                'value_per_tonne': 'Value per Tonne (AUD) - Premium Indicator',
                'category': 'Category'
            },
            color_discrete_map={
                'Market Leader': 'steelblue',
                'Premium Product': 'gold',
                'Standard': 'lightgray'
            }
        )
        
        # Add reference lines
        fig.add_hline(y=premium_threshold, line_dash="dash", line_color="green", 
                      annotation_text=f"Premium Threshold: ${premium_threshold:,.0f}/t")
        fig.add_vline(x=volume_threshold, line_dash="dash", line_color="red",
                      annotation_text=f"Volume Threshold: ${volume_threshold:.2f}B")
        
        fig.update_layout(height=700)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No industry data available for visualization.")
    
    # Market Leaders
    st.subheader("Market Leaders (High Volume + High Value)")
    
    market_leaders = industry_analysis[
        industry_analysis['category'] == 'Market Leader'
    ].sort_values('total_value_billions', ascending=False).copy()
    
    if len(market_leaders) > 0:
        fig = px.bar(
            market_leaders,
            x='total_value_billions',
            y='industry_sector',
            orientation='h',
            title="Market Leaders by Total Value",
            labels={'total_value_billions': 'Total Value (Billions AUD)', 'industry_sector': 'Industry'},
            color='total_value_billions',
            color_continuous_scale='Blues'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No market leaders found in the filtered data.")
    
    # Premium Products
    st.subheader("Premium Products (Low Volume + High Value)")
    
    premium_products = industry_analysis[
        industry_analysis['category'] == 'Premium Product'
    ].sort_values('value_per_tonne', ascending=False).copy()
    
    if len(premium_products) > 0:
        fig = px.bar(
            premium_products,
            x='value_per_tonne',
            y='industry_sector',
            orientation='h',
            title="Premium Products by Value per Tonne",
            labels={'value_per_tonne': 'Value per Tonne (AUD)', 'industry_sector': 'Industry'},
            color='value_per_tonne',
            color_continuous_scale='YlOrBr'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No premium products found in the filtered data.")

def show_risk_analysis(df):
    """Display risk analysis"""
    st.markdown('<h2 class="section-header">Risk Analysis</h2>', unsafe_allow_html=True)
    
    # Country Concentration Analysis
    st.subheader("Country Concentration Risk")
    
    commodity_country_matrix = df.groupby(['commodity_description', 'country_description']).agg({
        'valuecif': 'sum'
    }).reset_index()
    
    commodity_totals = commodity_country_matrix.groupby('commodity_description')['valuecif'].sum().reset_index()
    commodity_totals.columns = ['commodity_description', 'total_value']
    
    commodity_country_matrix = commodity_country_matrix.merge(commodity_totals, on='commodity_description')
    commodity_country_matrix['country_share'] = (
        commodity_country_matrix['valuecif'] / commodity_country_matrix['total_value'] * 100
    )
    
    hhi_by_commodity = commodity_country_matrix.groupby('commodity_description').apply(
        lambda x: (x['country_share'] ** 2).sum()
    ).reset_index()
    hhi_by_commodity.columns = ['commodity_description', 'hhi_index']
    
    top_country_share = commodity_country_matrix.groupby('commodity_description').agg({
        'country_share': 'max'
    }).reset_index()
    top_country_share.columns = ['commodity_description', 'top_country_share']
    
    concentration_analysis = commodity_totals.merge(hhi_by_commodity, on='commodity_description')
    concentration_analysis = concentration_analysis.merge(top_country_share, on='commodity_description')
    concentration_analysis['total_value_billions'] = concentration_analysis['total_value'] / 1e9
    
    def classify_risk(hhi, top_share):
        if hhi >= 2500 or top_share >= 50:
            return 'HIGH RISK'
        elif hhi >= 1500 or top_share >= 40:
            return 'MEDIUM RISK'
        else:
            return 'LOW RISK'
    
    concentration_analysis['risk_level'] = concentration_analysis.apply(
        lambda x: classify_risk(x['hhi_index'], x['top_country_share']), axis=1
    )
    
    # Risk distribution
    risk_counts = concentration_analysis['risk_level'].value_counts()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            values=risk_counts.values,
            names=risk_counts.index,
            title="Risk Level Distribution",
            color_discrete_map={
                'HIGH RISK': 'red',
                'MEDIUM RISK': 'orange',
                'LOW RISK': 'green'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top high-risk commodities
        high_risk = concentration_analysis[
            (concentration_analysis['total_value_billions'] >= 1.0) & 
            (concentration_analysis['risk_level'] == 'HIGH RISK')
        ].sort_values('hhi_index', ascending=False).head(15).copy()
        
        if len(high_risk) > 0:
            high_risk['commodity_label'] = high_risk['commodity_description'].apply(
                lambda x: x[:75] + '...' if len(x) > 75 else x
            )
            fig = px.bar(
                high_risk,
                x='hhi_index',
                y='commodity_label',
                orientation='h',
                title="Top 15 High-Risk Commodities by HHI",
                labels={'hhi_index': 'HHI Index', 'commodity_label': 'Commodity'},
                color='hhi_index',
                color_continuous_scale='Reds',
                hover_data={'commodity_description': True, 'commodity_label': False}
            )
            fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>HHI Index: %{x:.0f}<br>Value: %{customdata[1]:.2f} Billion AUD<br>Risk Level: %{customdata[2]}<extra></extra>',
                              customdata=high_risk[['commodity_description', 'total_value_billions', 'risk_level']].values)
            fig.add_vline(x=2500, line_dash="dash", line_color="red", 
                         annotation_text="High Risk Threshold")
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No high-risk commodities found with value >= $1B.")
    
    # Commodity Dependence Analysis
    st.subheader("Commodity Dependence Index")
    
    total_import_value = df['valuecif'].sum()
    
    commodity_dependence = df.groupby('commodity_description').agg({
        'valuecif': 'sum'
    }).reset_index()
    
    commodity_dependence['dependence_pct'] = (commodity_dependence['valuecif'] / total_import_value * 100)
    commodity_dependence['total_value_billions'] = commodity_dependence['valuecif'] / 1e9
    
    def classify_dependence(pct, value_billions):
        if pct >= 5.0 or value_billions >= 40:
            return 'CRITICAL'
        elif pct >= 2.0 or value_billions >= 15:
            return 'HIGH'
        elif pct >= 0.5 or value_billions >= 4:
            return 'MODERATE'
        else:
            return 'LOW'
    
    commodity_dependence['dependence_level'] = commodity_dependence.apply(
        lambda x: classify_dependence(x['dependence_pct'], x['total_value_billions']), axis=1
    )
    
    critical_high = commodity_dependence[
        commodity_dependence['dependence_level'].isin(['CRITICAL', 'HIGH'])
    ].sort_values('dependence_pct', ascending=False).head(20).copy()
    
    if len(critical_high) > 0:
        critical_high['commodity_label'] = critical_high['commodity_description'].apply(
            lambda x: x[:75] + '...' if len(x) > 75 else x
        )
        fig = px.bar(
            critical_high,
            x='dependence_pct',
            y='commodity_label',
            orientation='h',
            title="Top 20 Critical & High Dependence Commodities",
            labels={'dependence_pct': '% of Total Imports', 'commodity_label': 'Commodity'},
            color='dependence_level',
            color_discrete_map={
                'CRITICAL': 'darkred',
                'HIGH': 'orange',
                'MODERATE': 'gold',
                'LOW': 'lightgreen'
            },
            hover_data={'commodity_description': True, 'commodity_label': False}
        )
        fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>Dependence: %{x:.2f}%<br>Value: %{customdata[1]:.2f} Billion AUD<br>Level: %{customdata[2]}<extra></extra>',
                          customdata=critical_high[['commodity_description', 'total_value_billions', 'dependence_level']].values)
        fig.add_vline(x=5, line_dash="dash", line_color="darkred", annotation_text="Critical (5%)")
        fig.add_vline(x=2, line_dash="dash", line_color="orange", annotation_text="High (2%)")
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No critical or high dependence commodities found.")
    
    # Supplier Trend Analysis
    st.subheader("Supplier Trend Analysis (2024 vs 2025)")
    
    country_yearly = df.groupby(['country_description', 'year']).agg({
        'valuecif': 'sum'
    }).reset_index()
    
    if len(country_yearly['year'].unique()) >= 2:
        country_pivot = country_yearly.pivot_table(
            index='country_description',
            columns='year',
            values='valuecif',
            fill_value=0
        ).reset_index()
        
        year_cols = [col for col in country_pivot.columns if col != 'country_description']
        if len(year_cols) >= 2:
            country_pivot.columns = ['country'] + [f'value_{int(col)}' for col in year_cols]
            
            country_trends = country_pivot.copy()
            if 'value_2024' in country_trends.columns and 'value_2025' in country_trends.columns:
                country_trends['absolute_change'] = country_trends['value_2025'] - country_trends['value_2024']
                country_trends['pct_change'] = (
                    (country_trends['value_2025'] - country_trends['value_2024']) / 
                    country_trends['value_2024'].replace(0, np.nan) * 100
                ).fillna(0)
                
                country_trends_filtered = country_trends[
                    (country_trends['value_2024'] > 100_000_000) | 
                    (country_trends['value_2025'] > 100_000_000)
                ].copy()
                
                declining = country_trends_filtered.nsmallest(15, 'absolute_change').copy()
                growing = country_trends_filtered.nlargest(15, 'absolute_change').copy()
                
                declining['change_billions'] = declining['absolute_change'] / 1e9
                declining['country_label'] = declining['country'].apply(
                    lambda x: x[:40] + '...' if len(x) > 40 else x
                )
                
                growing['change_billions'] = growing['absolute_change'] / 1e9
                growing['country_label'] = growing['country'].apply(
                    lambda x: x[:40] + '...' if len(x) > 40 else x
                )
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if len(declining) > 0:
                        fig = px.bar(
                            declining,
                            x='change_billions',
                            y='country_label',
                            orientation='h',
                            title="Top 15 Declining Suppliers",
                            labels={'change_billions': 'Change (Billions AUD)', 'country_label': 'Country'},
                            color='absolute_change',
                            color_continuous_scale='Reds'
                        )
                        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No declining suppliers found.")
                
                with col2:
                    if len(growing) > 0:
                        fig = px.bar(
                            growing,
                            x='change_billions',
                            y='country_label',
                            orientation='h',
                            title="Top 15 Growing Suppliers",
                            labels={'change_billions': 'Change (Billions AUD)', 'country_label': 'Country'},
                            color='absolute_change',
                            color_continuous_scale='Greens'
                        )
                        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No growing suppliers found.")

def show_transport_mode_analysis(df):
    """Display transport mode analysis"""
    st.markdown('<h2 class="section-header">Transport Mode Analysis</h2>', unsafe_allow_html=True)
    
    mode_stats = df.groupby('mode_description').agg({
        'valuefob': 'sum',
        'valuecif': 'sum',
        'weight': 'sum',
        'quantity': 'sum'
    }).reset_index().sort_values('valuefob', ascending=False)
    
    mode_stats['valuefob_pct'] = (mode_stats['valuefob'] / mode_stats['valuefob'].sum()) * 100
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            mode_stats,
            x='mode_description',
            y='valuefob',
            title="Import Value by Transport Mode (FOB)",
            labels={'valuefob': 'Value (AUD)', 'mode_description': 'Transport Mode'},
            color='valuefob',
            color_continuous_scale='Blues'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(
            mode_stats,
            values='valuefob',
            names='mode_description',
            title="Transport Mode Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Mode by weight
    st.subheader("Transport Mode by Weight")
    
    fig = px.bar(
        mode_stats,
        x='mode_description',
        y='weight',
        title="Import Weight by Transport Mode",
        labels={'weight': 'Weight (Tonnes)', 'mode_description': 'Transport Mode'},
        color='weight',
        color_continuous_scale='Oranges'
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

def show_key_insights(df):
    """Display key insights and summary"""
    st.markdown('<h2 class="section-header">Key Insights</h2>', unsafe_allow_html=True)
    
    # Calculate key metrics
    country_stats = df.groupby('country_description').agg({
        'valuecif': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    country_stats['valuecif_pct'] = (country_stats['valuecif'] / country_stats['valuecif'].sum()) * 100
    
    commodity_stats = df.groupby('commodity_description').agg({
        'valuecif': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    commodity_stats['valuecif_pct'] = (commodity_stats['valuecif'] / commodity_stats['valuecif'].sum()) * 100
    
    mode_stats = df.groupby('mode_description').agg({
        'valuefob': 'sum'
    }).reset_index().sort_values('valuefob', ascending=False)
    mode_stats['valuefob_pct'] = (mode_stats['valuefob'] / mode_stats['valuefob'].sum()) * 100
    
    state_stats = df.groupby('state').agg({
        'valuecif': 'sum'
    }).reset_index().sort_values('valuecif', ascending=False)
    state_stats['valuecif_pct'] = (state_stats['valuecif'] / state_stats['valuecif'].sum()) * 100
    
    yearly_stats = df.groupby('year').agg({
        'valuefob': 'sum'
    }).reset_index()
    
    # Display insights
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Top Statistics")
        
        top_country = country_stats.iloc[0]
        st.info(f"**Top Import Source:** {top_country['country_description']}\n\n"
                f"Value: ${top_country['valuecif']/1e9:.2f}B ({top_country['valuecif_pct']:.2f}% of total)")
        
        top_commodity = commodity_stats.iloc[0]
        commodity_name = top_commodity['commodity_description'][:80] + '...' if len(top_commodity['commodity_description']) > 80 else top_commodity['commodity_description']
        st.info(f"**Top Import Commodity:** {commodity_name}\n\n"
                f"Value: ${top_commodity['valuecif']/1e9:.2f}B ({top_commodity['valuecif_pct']:.2f}% of total)")
    
    with col2:
        st.markdown("### Additional Insights")
        
        top_mode = mode_stats.iloc[0]
        st.info(f"**Dominant Transport Mode:** {top_mode['mode_description']}\n\n"
                f"Value: ${top_mode['valuefob']/1e9:.2f}B ({top_mode['valuefob_pct']:.2f}% of total)")
        
        top_state = state_stats.iloc[0]
        st.info(f"**Top Import State:** {top_state['state']}\n\n"
                f"Value: ${top_state['valuecif']/1e9:.2f}B ({top_state['valuecif_pct']:.2f}% of total)")
    
    # Year-over-year growth
    if len(yearly_stats) > 1:
        yoy_growth = ((yearly_stats.iloc[1]['valuefob'] - yearly_stats.iloc[0]['valuefob']) / 
                     yearly_stats.iloc[0]['valuefob']) * 100
        st.markdown("### Year-over-Year Growth")
        st.metric(
            "Growth Rate",
            f"{yoy_growth:+.2f}%",
            delta=f"{yearly_stats.iloc[1]['valuefob']/1e9 - yearly_stats.iloc[0]['valuefob']/1e9:.2f}B"
        )
    
    # Market concentration
    st.markdown("### Market Concentration")
    top_5_countries_pct = country_stats.head(5)['valuecif_pct'].sum()
    top_10_commodities_pct = commodity_stats.head(10)['valuecif_pct'].sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Top 5 Countries Share", f"{top_5_countries_pct:.2f}%")
    with col2:
        st.metric("Top 10 Commodities Share", f"{top_10_commodities_pct:.2f}%")

# Main execution - wrap everything to catch any errors
if __name__ == "__main__":
    import sys
    import traceback
    
    # Always log to stderr first so errors appear in logs
    print("=" * 80, file=sys.stderr)
    print("DASHBOARD STARTING", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    
    try:
        print("Calling main()...", file=sys.stderr)
        main()
        print("main() completed successfully", file=sys.stderr)
    except Exception as e:
        error_msg = f"CRITICAL ERROR: {str(e)}"
        error_type = type(e).__name__
        error_trace = traceback.format_exc()
        
        # Always print to stderr first (appears in logs)
        print("=" * 80, file=sys.stderr)
        print("ERROR CAUGHT:", file=sys.stderr)
        print(error_msg, file=sys.stderr)
        print(f"Error Type: {error_type}", file=sys.stderr)
        print("-" * 80, file=sys.stderr)
        print(error_trace, file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        
        # Then try to display in Streamlit
        try:
            st.error("⚠️ **Critical Error: Dashboard failed to load**")
            st.error(f"**Error:** {str(e)}")
            st.error(f"**Error Type:** {error_type}")
            
            with st.expander("🔍 Show detailed error information"):
                st.code(error_trace)
            
            st.info("""
            **Common causes:**
            1. Missing or incorrect Streamlit secrets configuration
            2. Data file not available in Google Cloud Storage
            3. Missing required Python packages
            4. Data file format issues
            
            **To fix:**
            1. Check Streamlit Cloud logs: Settings → Logs
            2. Verify GCS secrets are configured correctly
            3. Ensure data file exists in GCS bucket
            4. Check that all dependencies are in requirements.txt
            """)
        except Exception as display_error:
            # Even error display failed - log it
            print(f"ERROR DISPLAY ALSO FAILED: {display_error}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            try:
                st.write("Critical error occurred. Check logs for details.")
            except:
                pass

