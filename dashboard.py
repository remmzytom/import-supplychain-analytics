"""
Streamlit Dashboard for Freight Import Data Analysis
Interactive dashboard displaying all visualizations from the analysis notebook
"""

# Import streamlit first
import streamlit as st

# Page configuration - MUST be first Streamlit command
# Wrap in broad try-except to prevent health check failures
# During health checks, Streamlit imports modules but may not fully initialize
try:
    st.set_page_config(
        page_title="Freight Import Data Dashboard",
        page_icon=None,
        layout="wide",
        initial_sidebar_state="expanded"
    )
except Exception:
    # Page config failed - this can happen during health checks
    # Streamlit will use defaults, app should still work
    # Don't raise or print - just continue silently
    pass

# Now import other modules
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

# Try to import BigQuery (for efficient querying of large datasets)
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import commodity mapping (with fallback)
# Use broad exception handling to catch any import errors during health checks
COMMODITY_MAPPING_AVAILABLE = False
try:
    from commodity_code_mapping import map_commodity_code_to_sitc_industry
    COMMODITY_MAPPING_AVAILABLE = True
except (ImportError, ModuleNotFoundError, AttributeError, Exception):
    # Any error importing - use fallback
    COMMODITY_MAPPING_AVAILABLE = False
    # Fallback function if mapping is not available
    def map_commodity_code_to_sitc_industry(code):
        return "Unknown"

warnings.filterwarnings('ignore')

# Custom CSS - moved to function to avoid module-level execution issues
def apply_custom_css():
    """Apply custom CSS styles"""
    try:
        st.markdown("""
            <style>
            .main-header {
                font-size: 2.5rem;
                font-weight: bold;
                color: #1f77b4;
                text-align: center;
                padding: 1rem 0;
            }
            .section-header {
                font-size: 1.8rem;
                font-weight: bold;
                color: #2c3e50;
                margin-top: 2rem;
                margin-bottom: 1rem;
                border-bottom: 3px solid #1f77b4;
                padding-bottom: 0.5rem;
            }
            .metric-card {
                background-color: #f0f2f6;
                padding: 1rem;
                border-radius: 0.5rem;
                border-left: 4px solid #1f77b4;
            }
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
            .nav-container {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 1rem;
                background-color: #f8f9fa;
                border-radius: 10px;
                margin: 1rem 0;
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
        # Safely check for secrets (may not be available in all contexts)
        try:
            # Check if secrets are available at all
            if not hasattr(st, 'secrets'):
                if show_progress:
                    st.warning("Streamlit secrets not available. Skipping GCS load.")
                return None
            
            # Check if gcp key exists
            if 'gcp' not in st.secrets:
                if show_progress:
                    st.warning("GCP configuration not found in Streamlit secrets. Skipping GCS load.")
                return None
        except Exception as e:
            # Any error accessing secrets - fail gracefully
            if show_progress:
                st.warning(f"Could not access Streamlit secrets: {str(e)}. Skipping GCS load.")
            return None
        
        # Get GCS configuration from Streamlit secrets
        
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
            # Load ALL data using chunked reading (already implemented in load_data_from_file)
            if show_progress:
                st.info("Loading data into memory using chunked reading...")
                st.info("This will load all 4.48M rows efficiently in chunks.")
            
            # Load all data (no limit) - chunked loading handles memory efficiently
            df = load_data_from_file(tmp_path, max_rows=None)
            
            if df is None:
                # Error already displayed in load_data_from_file
                return None
            
            if show_progress:
                st.success(f"Data loaded successfully! ({len(df):,} rows)")
                # Show memory usage
                try:
                    memory_mb = df.memory_usage(deep=True).sum() / (1024**2)
                    st.info(f"Memory usage: {memory_mb:.2f} MB")
                except:
                    pass
            
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

@st.cache_data(ttl=600)  # Cache for 10 minutes
def query_bigquery(filters=None, limit_rows=None):
    """Query data from BigQuery with optional filters and row limit
    This is much more memory-efficient for large datasets
    
    Args:
        filters: Dict of filters like {'year': [2024, 2025], 'month': ['January', 'February'], 'country': ['China']}
        limit_rows: Maximum number of rows to return (None = no limit, but recommended to use limit for large datasets)
    
    Returns:
        pandas.DataFrame or None
    """
    if not BIGQUERY_AVAILABLE:
        return None
    
    try:
        # Safely check for secrets
        try:
            if 'gcp' not in st.secrets:
                return None
        except (AttributeError, RuntimeError, Exception):
            return None
        
        gcp_config = st.secrets['gcp']
        dataset_id = gcp_config.get('bigquery_dataset', 'freight_import_data')
        table_id = gcp_config.get('bigquery_table', 'imports_cleaned')
        project_id = gcp_config.get('bigquery_project', '').strip()
        
        # Treat placeholder values as empty
        if project_id.lower() in ['your-project-id', 'your-project', '']:
            project_id = ''
        
        # Get credentials from secrets
        if 'credentials' not in gcp_config:
            return None
        
        # Create credentials dict from secrets
        credentials_dict = dict(gcp_config['credentials'])
        
        # Create temporary file for credentials
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as creds_file:
            json.dump(credentials_dict, creds_file)
            creds_path = creds_file.name
        
        try:
            # Initialize BigQuery client
            if project_id:
                client = bigquery.Client.from_service_account_json(creds_path, project=project_id)
            else:
                client = bigquery.Client.from_service_account_json(creds_path)
            
            # Get project ID from credentials if not set
            if not project_id:
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                    project_id = creds_data.get('project_id')
                    if project_id:
                        client.project = project_id
            
            # Also try to get from client if still not set
            if not project_id and hasattr(client, 'project') and client.project:
                project_id = client.project
            
            if not project_id:
                return None
            
            # Build query with filters
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            query = f"SELECT * FROM `{table_ref}`"
            
            # Add WHERE conditions based on filters
            where_conditions = []
            if filters:
                if 'year' in filters and filters['year']:
                    years = ', '.join([str(y) for y in filters['year']])
                    where_conditions.append(f"year IN ({years})")
                if 'month' in filters and filters['month']:
                    # Escape single quotes in month names for SQL
                    escaped_months = [m.replace("'", "''") for m in filters['month']]
                    months = ', '.join([f"'{m}'" for m in escaped_months])
                    where_conditions.append(f"month IN ({months})")
                if 'country' in filters and filters['country']:
                    # Escape single quotes in country names for SQL
                    escaped_countries = [c.replace("'", "''") for c in filters['country']]
                    countries = ', '.join([f"'{c}'" for c in escaped_countries])
                    where_conditions.append(f"country_description IN ({countries})")
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
            
            # Add LIMIT only if specified (for full dataset, no limit)
            if limit_rows is not None:
                query += f" LIMIT {limit_rows}"
            
            # Execute query with timeout protection
            import time
            
            # Show different messages based on whether we're loading all data
            if limit_rows is None or limit_rows > 3000000:
                st.info(f"Executing BigQuery query for full dataset (4.48M rows)...")
                st.info("This may take 5-15 minutes. BigQuery is processing and transferring data...")
                timeout_seconds = 900  # 15 minute timeout for full dataset
            else:
                st.info(f"Executing BigQuery query (limit: {limit_rows:,} rows)...")
                timeout_seconds = 300  # 5 minute timeout for limited queries
            
            query_job = client.query(query)
            
            try:
                # Poll for completion with timeout and progress updates
                start_time = time.time()
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                while not query_job.done():
                    elapsed = time.time() - start_time
                    elapsed_minutes = int(elapsed // 60)
                    elapsed_seconds = int(elapsed % 60)
                    
                    # Update progress (estimate based on elapsed time vs timeout)
                    if timeout_seconds > 0:
                        progress = min(elapsed / timeout_seconds, 0.95)  # Cap at 95% until done
                        progress_bar.progress(progress)
                    
                    # Update status message
                    status_text.info(f"Query running... Elapsed: {elapsed_minutes}m {elapsed_seconds}s")
                    
                    if elapsed > timeout_seconds:
                        query_job.cancel()
                        progress_bar.progress(1.0)
                        raise TimeoutError(f"Query exceeded {timeout_seconds} second timeout")
                    
                    time.sleep(3)  # Check every 3 seconds
                
                # Query completed - get results in chunks to avoid memory issues
                progress_bar.progress(0.98)
                
                if query_job.errors:
                    error_msg = str(query_job.errors)
                    progress_bar.progress(1.0)
                    st.error(f"BigQuery query error: {error_msg}")
                    return None
                
                # Load data in chunks to prevent memory overflow
                # Use smaller chunks (100K) to stay well within memory limits
                chunk_size = 100000  # 100K rows per chunk (much safer)
                all_chunks = []
                total_rows_loaded = 0
                
                # Load data in chunks using multiple queries with LIMIT/OFFSET
                # Need ORDER BY for consistent pagination
                status_text.info("Loading data in chunks (100K rows each) to prevent memory issues...")
                
                # Check if query already has ORDER BY
                query_lower = query.lower()
                has_order_by = 'order by' in query_lower
                
                # Add ORDER BY if not present (needed for consistent pagination)
                # Use a simple, reliable ordering
                if not has_order_by:
                    # Try to use year, month_number if they exist, otherwise use first column
                    # We'll check if columns exist in the actual query result
                    paginated_query = query + ' ORDER BY year, month_number, country_code, commodity_code'
                else:
                    paginated_query = query
                
                chunk_num = 0
                max_chunks = 50  # Safety limit: max 5M rows (50 chunks * 100K)
                
                try:
                    while chunk_num < max_chunks:
                        chunk_num += 1
                        offset = (chunk_num - 1) * chunk_size
                        
                        # Build chunk query
                        chunk_query = paginated_query + f" LIMIT {chunk_size} OFFSET {offset}"
                        
                        status_text.info(f"Loading chunk {chunk_num} (500K rows each)... Offset: {offset:,}")
                        
                        # Execute chunk query
                        chunk_job = client.query(chunk_query)
                        
                        # Wait for chunk query to complete
                        chunk_start_time = time.time()
                        while not chunk_job.done():
                            if time.time() - chunk_start_time > 60:  # 1 min timeout per chunk
                                st.warning(f"Chunk {chunk_num} timeout. Stopping chunked load.")
                                break
                            time.sleep(1)
                        
                        if chunk_job.errors:
                            if chunk_num == 1:
                                # First chunk failed - this is a problem
                                raise Exception(f"Chunk query failed: {chunk_job.errors}")
                            else:
                                # Later chunk failed - we've probably reached the end
                                break
                        
                        # Load chunk into dataframe with error handling
                        try:
                            df_chunk = chunk_job.to_dataframe(max_results=chunk_size)
                        except Exception as df_error:
                            # If max_results doesn't work, try without it
                            try:
                                df_chunk = chunk_job.to_dataframe()
                            except Exception as df_error2:
                                st.error(f"Error loading chunk {chunk_num} into dataframe: {str(df_error2)}")
                                if chunk_num == 1:
                                    raise df_error2
                                else:
                                    break
                        
                        if df_chunk is None or len(df_chunk) == 0:
                            # No more data
                            break
                        
                        # Optimize data types immediately to reduce memory
                        try:
                            if 'year' in df_chunk.columns:
                                df_chunk['year'] = pd.to_numeric(df_chunk['year'], downcast='integer')
                            if 'month_number' in df_chunk.columns:
                                df_chunk['month_number'] = pd.to_numeric(df_chunk['month_number'], downcast='integer')
                            
                            float_cols = ['valuecif', 'valuefob', 'weight', 'quantity']
                            for col in float_cols:
                                if col in df_chunk.columns:
                                    df_chunk[col] = pd.to_numeric(df_chunk[col], downcast='float')
                        except Exception as opt_error:
                            # Optimization failed, but continue with chunk
                            st.warning(f"Warning: Could not optimize chunk {chunk_num} data types: {str(opt_error)}")
                        
                        all_chunks.append(df_chunk)
                        total_rows_loaded += len(df_chunk)
                        
                        # Update progress
                        progress = min(0.98, chunk_num / max_chunks)
                        progress_bar.progress(progress)
                        status_text.info(f"Loaded chunk {chunk_num}: {len(df_chunk):,} rows (Total: {total_rows_loaded:,})")
                        
                        # If chunk is smaller than chunk_size, we've reached the end
                        if len(df_chunk) < chunk_size:
                            break
                        
                        # Free memory after every chunk to prevent accumulation
                        import gc
                        gc.collect()
                
                except Exception as chunk_error:
                    # If chunking fails, try to use what we have
                    import traceback
                    error_trace = traceback.format_exc()
                    print(f"Chunked loading error: {error_trace}", file=sys.stderr)
                    
                    if len(all_chunks) > 0:
                        st.warning(f"Error during chunked loading: {str(chunk_error)}. Using {total_rows_loaded:,} rows loaded so far.")
                    else:
                        # No chunks loaded - suggest fallback to GCS
                        st.error(f"Chunked loading failed: {str(chunk_error)}")
                        st.warning("BigQuery chunked loading is not working. Falling back to GCS...")
                        return None  # Will trigger fallback to GCS
                
                # Combine all chunks
                if not all_chunks:
                    st.error("No data loaded from BigQuery")
                    return None
                
                status_text.info(f"Combining {len(all_chunks)} chunks...")
                
                # If we only have a few chunks, combine all at once
                if len(all_chunks) <= 5:
                    df = pd.concat(all_chunks, ignore_index=True)
                else:
                    # For many chunks, combine incrementally in batches
                    df = all_chunks[0]
                    batch_size = 3
                    for i in range(1, len(all_chunks), batch_size):
                        batch = all_chunks[i:i+batch_size]
                        if len(batch) == 1:
                            df = pd.concat([df, batch[0]], ignore_index=True)
                        else:
                            batch_df = pd.concat(batch, ignore_index=True)
                            df = pd.concat([df, batch_df], ignore_index=True)
                            del batch_df
                        
                        # Free memory after each batch
                        import gc
                        gc.collect()
                        status_text.info(f"Combined {min(i+batch_size, len(all_chunks))}/{len(all_chunks)} chunks...")
                
                # Final cleanup
                del all_chunks
                import gc
                gc.collect()
                
                # Verify final dataframe
                if df is None or len(df) == 0:
                    st.error("Failed to combine chunks - empty dataframe")
                    return None
                
                progress_bar.progress(1.0)
                status_text.empty()
                st.success(f"Successfully loaded {len(df):,} rows from BigQuery in {chunk_num} chunks!")
                
            except TimeoutError as e:
                st.error(f"Query timeout after {timeout_seconds} seconds.")
                st.warning("The dataset is too large. Falling back to GCS with row limit...")
                return None
            except Exception as e:
                error_msg = str(e)
                st.error(f"BigQuery query failed: {error_msg}")
                st.warning("Falling back to other data sources...")
                return None
            
            # Optimize data types after loading
            if 'year' in df.columns:
                df['year'] = df['year'].astype('int16')
            if 'month_number' in df.columns:
                df['month_number'] = df['month_number'].astype('int8')
            
            float_cols = ['valuecif', 'valuefob', 'weight', 'quantity']
            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], downcast='float')
            
            # Add industry sector if not present
            if 'industry_sector' not in df.columns:
                if COMMODITY_MAPPING_AVAILABLE:
                    df['industry_sector'] = df['commodity_code'].apply(map_commodity_code_to_sitc_industry)
                else:
                    df['industry_sector'] = "Unknown"
            
            # Create date column for time series
            if 'year' in df.columns and 'month_number' in df.columns:
                df['date'] = pd.to_datetime(
                    df['year'].astype(str) + '-' + 
                    df['month_number'].astype(str).str.zfill(2) + '-01'
                )
            
            return df
            
        finally:
            # Clean up credentials file
            if os.path.exists(creds_path):
                os.unlink(creds_path)
                
    except Exception as e:
        # Silently fail - will fall back to GCS/CSV
        return None

def load_data_from_file(file_path, max_rows=None):
    """Load and process data from a CSV file with memory optimization
    
    Args:
        file_path: Path to CSV file
        max_rows: Maximum number of rows to load (None = load all, use for large datasets)
    """
    chunk_size = 100000
    chunks = []
    total_rows = 0
    batch_size = 5
    
    try:
        # Load data in chunks
        chunks_to_load = None
        if max_rows:
            # Calculate how many chunks we need
            chunks_to_load = (max_rows // chunk_size) + 1
        
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, low_memory=False), 1):
            # Check if we've reached the max_rows limit
            if max_rows and total_rows >= max_rows:
                break
            
            # Stop if we've loaded enough chunks (approximate limit)
            if chunks_to_load and i > chunks_to_load:
                # Take only what we need from this chunk
                if max_rows:
                    remaining = max_rows - total_rows
                    if remaining > 0:
                        chunk = chunk.head(remaining)
                    else:
                        break
                else:
                    break
            
            # Optimize data types immediately to reduce memory
            if 'year' in chunk.columns:
                chunk['year'] = pd.to_numeric(chunk['year'], downcast='integer')
            if 'month_number' in chunk.columns:
                chunk['month_number'] = pd.to_numeric(chunk['month_number'], downcast='integer')
            
            # Optimize float columns
            float_cols = ['valuecif', 'valuefob', 'weight', 'quantity']
            for col in float_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], downcast='float')
            
            chunks.append(chunk)
            total_rows += len(chunk)
            
            # If we've reached max_rows, stop
            if max_rows and total_rows >= max_rows:
                break
            
            # Combine chunks periodically to avoid memory spikes
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
        
        # Final memory optimization
        import gc
        gc.collect()
        
        # Add industry sector if not present (do this efficiently)
        if 'industry_sector' not in df.columns:
            if COMMODITY_MAPPING_AVAILABLE:
                # Process in batches to avoid memory spike
                df['industry_sector'] = df['commodity_code'].apply(map_commodity_code_to_sitc_industry)
            else:
                df['industry_sector'] = "Unknown"
        
        # Create date column for time series
        if 'year' in df.columns and 'month_number' in df.columns:
            df['date'] = pd.to_datetime(
                df['year'].astype(str) + '-' + 
                df['month_number'].astype(str).str.zfill(2) + '-01'
            )
        
        return df
    
    except MemoryError as e:
        import traceback
        st.error("**Memory Error: Dataset too large for Streamlit Cloud**")
        st.error(f"Error: {str(e)}")
        st.warning("""
        **The dataset (4.48M rows) is too large to load entirely into memory on Streamlit Cloud.**
        
        **Options:**
        1. **Use BigQuery** (recommended) - Query data instead of loading entire file
        2. **Sample the data** - Load a subset for dashboard
        3. **Filter before loading** - Load only specific years/months
        
        **Current status:** Trying to load full dataset...
        """)
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
        return None
    except Exception as e:
        import traceback
        st.error(f"**Error loading data from file**")
        st.error(f"Error: {str(e)}")
        st.error(f"Error type: {type(e).__name__}")
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
        return None

@st.cache_data
def load_data():
    """Load and cache the cleaned import data
    Tries local file first
    NOTE: This function cannot contain widget commands (like st.file_uploader)
    NOTE: Cannot access st.secrets in cached functions - GCS loading handled in load_data_with_fallback()
    """
    # Try local file first (for local development)
    data_path = 'data/imports_2024_2025_cleaned.csv'
    
    if os.path.exists(data_path):
        try:
            return load_data_from_file(data_path)
        except Exception:
            # Silently fail - let non-cached function handle GCS
            return None
    
    # Cannot access st.secrets in cached function - return None
    # The non-cached load_data_with_fallback() will handle GCS loading
    return None

def load_data_with_fallback():
    """Load data with fallback to file uploader
    Priority: BigQuery > Local file > GCS > File uploader
    This function handles widgets and should not be cached
    Can access st.secrets here since it's not cached
    """
    # Priority 1: Try GCS first (most reliable with chunked loading)
    # BigQuery chunked loading has been unreliable, so use GCS which works
    if GCS_AVAILABLE:
        try:
            # Check if GCS is configured
            if 'gcp' in st.secrets:
                gcp_config = st.secrets['gcp']
                if 'bucket_name' in gcp_config and 'file_name' in gcp_config:
                    st.info("Loading data from Google Cloud Storage...")
                    st.info("Using efficient chunked loading to load all 4.48M rows.")
                    
                    gcs_data = _load_data_from_gcs_internal(show_progress=True)
                    if gcs_data is not None and len(gcs_data) > 0:
                        st.success(f"Loaded {len(gcs_data):,} rows from GCS!")
                        return gcs_data
                    else:
                        st.warning("GCS loading returned no data. Trying other sources...")
        except Exception as e:
            # Continue to other sources if GCS fails
            import traceback
            error_details = traceback.format_exc()
            st.warning(f"GCS loading failed: {str(e)}")
            st.info("Falling back to local file or BigQuery...")
            print(f"GCS error details: {error_details}", file=sys.stderr)
    
    # Priority 2: Try BigQuery (fallback if GCS not available)
    if BIGQUERY_AVAILABLE:
        try:
            # Check if BigQuery is configured
            if 'gcp' in st.secrets:
                gcp_config = st.secrets['gcp']
                if 'bigquery_dataset' in gcp_config and 'bigquery_table' in gcp_config:
                    st.info("Loading data from BigQuery (fallback)...")
                    st.warning("BigQuery loading may be slower. Consider using GCS for better reliability.")
                    
                    # Try with a reasonable limit first
                    bigquery_data = query_bigquery(filters=None, limit_rows=2000000)
                    
                    if bigquery_data is not None and len(bigquery_data) > 0:
                        st.success(f"Loaded {len(bigquery_data):,} rows from BigQuery!")
                        return bigquery_data
                    else:
                        st.warning("BigQuery query returned no data. Trying other sources...")
        except Exception as e:
            # Continue to other sources if BigQuery fails
            import traceback
            error_details = traceback.format_exc()
            st.warning(f"BigQuery query failed: {str(e)}")
            st.info("Falling back to local file...")
            print(f"BigQuery error details: {error_details}", file=sys.stderr)
    
    # Priority 3: Try to load data (cached function - only checks local file)
    df = load_data()
    
    # If data loaded successfully, return it
    if df is not None and len(df) > 0:
        return df
    
    # Priority 3: Try GCS directly (can use st.secrets here)
    if GCS_AVAILABLE:
        try:
            # Can access secrets here since this function is not cached
            gcs_data = _load_data_from_gcs_internal(show_progress=True)
            if gcs_data is not None and len(gcs_data) > 0:
                return gcs_data
        except Exception as e:
            # Continue to file uploader fallback
            pass
    
    # If data not found, show error and offer file uploader
    st.error("**Data file not found**")
    st.info("""
    **For local development:**
    - Ensure `data/imports_2024_2025_cleaned.csv` exists in your project directory
    
    **For Streamlit Cloud:**
    - Configure Google Cloud Storage in Streamlit secrets
    - Or upload the data file using the file uploader below
    """)
    
    # Offer file uploader as fallback (this is outside cached function)
    uploaded_file = st.file_uploader("Upload cleaned data file (CSV)", type=['csv'])
    if uploaded_file is not None:
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name
            return load_data_from_file(tmp_path)
        except Exception as e:
            st.error(f"Error loading uploaded file: {str(e)}")
            return None
    
    return None

def main():
    """Main dashboard application"""
    
    # Apply custom CSS
    apply_custom_css()
    
    # Header
    try:
        st.markdown('<h1 class="main-header">Freight Import Data Dashboard</h1>', unsafe_allow_html=True)
        st.markdown("---")
    except Exception as e:
        st.error(f"Error displaying header: {str(e)}")
        return
    
    # Load data with error handling
    try:
        df = load_data_with_fallback()
    except Exception as e:
        st.error("**Error loading data**")
        st.error(f"Error: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
        st.info("Please check your data source configuration or try uploading a file.")
        return
    
    # Check if data was loaded successfully
    if df is None or len(df) == 0:
        st.warning("No data available. Please configure data source or upload a file.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Year filter - optimize for large datasets
    try:
        # For very large datasets, sample first to get unique values faster
        if len(df) > 2000000:
            df_sample_years = df['year'].dropna().sample(n=min(500000, len(df)), random_state=42)
            available_years = sorted(df_sample_years.unique())
        else:
            available_years = sorted(df['year'].dropna().unique())
        # Limit default selection to prevent memory issues
        default_years = available_years if len(available_years) <= 5 else available_years[-2:]
    except Exception as e:
        st.warning(f"Error getting years: {str(e)}")
        available_years = []
        default_years = []
    
    selected_years = st.sidebar.multiselect(
        "Select Years",
        options=available_years,
        default=default_years if default_years else available_years
    )
    
    # Month filter - optimize for large datasets
    try:
        # For very large datasets, sample first to get unique values faster
        if len(df) > 2000000:
            df_sample_months = df['month'].dropna().sample(n=min(500000, len(df)), random_state=42)
            available_months = sorted(df_sample_months.unique())
        else:
            available_months = sorted(df['month'].dropna().unique())
        default_months = available_months if len(available_months) <= 12 else available_months
    except Exception as e:
        st.warning(f"Error getting months: {str(e)}")
        available_months = []
        default_months = []
    
    selected_months = st.sidebar.multiselect(
        "Select Months",
        options=available_months,
        default=default_months
    )
    
    # Country filter - optimize for large datasets
    try:
        # For large datasets, sample first to get top countries faster
        if len(df) > 1000000:
            # Sample 500K rows for faster groupby (use iloc for faster sampling)
            sample_size = min(500000, len(df))
            sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
            df_sample = df.loc[sample_indices]
            top_countries = df_sample.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(20).index.tolist()
        else:
            top_countries = df.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(20).index.tolist()
    except Exception as e:
        st.warning(f"Error getting top countries: {str(e)}")
        top_countries = []
    
    selected_countries = st.sidebar.multiselect(
        "Select Countries (Top 20)",
        options=top_countries,
        default=[]
    )
    
    # Apply filters using mask-based approach (memory efficient)
    # Build boolean mask instead of copying dataframe
    mask = pd.Series(True, index=df.index)
    
    if selected_years:
        mask = mask & df['year'].isin(selected_years)
    if selected_months:
        mask = mask & df['month'].isin(selected_months)
    if selected_countries:
        mask = mask & df['country_description'].isin(selected_countries)
    
    # Only create filtered dataframe at the end (single copy)
    # Check if any filters were applied
    filters_applied = len(selected_years) < len(available_years) or len(selected_months) < len(available_months) or len(selected_countries) > 0
    
    if filters_applied:
        # Filters applied - create filtered dataframe
        df_filtered = df[mask].copy()
    else:
        # No filters - use original dataframe (no copy needed)
        # But warn if dataset is very large
        if len(df) > 3000000:
            st.warning(f"⚠️ Large dataset ({len(df):,} rows) loaded. Consider using filters to improve performance.")
        df_filtered = df
    
    # Log memory usage for debugging
    try:
        memory_mb = df_filtered.memory_usage(deep=True).sum() / (1024**2)
        st.sidebar.info(f"Filtered: {len(df_filtered):,} rows ({memory_mb:.1f} MB)")
    except:
        pass
    
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
    # Wrap each section in try-except to prevent crashes
    
    # 1. Overview Section
    try:
        st.markdown('<div id="overview"></div>', unsafe_allow_html=True)
        show_overview(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Overview section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 2. Time Series Analysis Section
    try:
        st.markdown('<div id="time-series"></div>', unsafe_allow_html=True)
        show_time_series(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Time Series section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 3. Geographic Analysis Section
    try:
        st.markdown('<div id="geographic"></div>', unsafe_allow_html=True)
        show_geographic_analysis(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Geographic Analysis section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 4. Commodity Analysis Section
    try:
        st.markdown('<div id="commodity"></div>', unsafe_allow_html=True)
        show_commodity_analysis(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Commodity Analysis section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 5. Value vs Volume Analysis Section
    try:
        st.markdown('<div id="value-volume"></div>', unsafe_allow_html=True)
        show_value_volume_analysis(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Value vs Volume Analysis section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 6. Risk Analysis Section
    try:
        st.markdown('<div id="risk"></div>', unsafe_allow_html=True)
        show_risk_analysis(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Risk Analysis section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 7. Transport Mode Analysis Section
    try:
        st.markdown('<div id="transport"></div>', unsafe_allow_html=True)
        show_transport_mode_analysis(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Transport Mode Analysis section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # 8. Key Insights Section
    try:
        st.markdown('<div id="insights"></div>', unsafe_allow_html=True)
        show_key_insights(df_filtered)
    except Exception as e:
        st.error(f"Error displaying Key Insights section: {str(e)}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())

def _optimize_groupby_for_large_df(df, groupby_cols, agg_dict, sample_size=2000000):
    """Helper function to optimize groupby operations for large datasets"""
    if len(df) > sample_size:
        # Sample for faster groupby
        sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
        df_sample = df.loc[sample_indices]
        result = df_sample.groupby(groupby_cols).agg(agg_dict).reset_index()
        # Scale up to approximate full dataset
        scale_factor = len(df) / len(df_sample)
        numeric_cols = [col for col in result.columns if col not in groupby_cols]
        for col in numeric_cols:
            if result[col].dtype in ['float64', 'int64', 'float32', 'int32']:
                result[col] = result[col] * scale_factor
        return result
    else:
        return df.groupby(groupby_cols).agg(agg_dict).reset_index()

def show_overview(df):
    """Display overview metrics"""
    st.markdown('<h2 class="section-header">Overview</h2>', unsafe_allow_html=True)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_fob = df['valuefob'].sum()
    total_cif = df['valuecif'].sum()
    total_weight = df['weight'].sum()
    total_records = len(df)
    
    with col1:
        st.metric("Total Records", f"{total_records:,}")
    
    with col2:
        st.metric("Total FOB Value", f"${total_fob/1e9:.2f}B")
    
    with col3:
        st.metric("Total CIF Value", f"${total_cif/1e9:.2f}B")
    
    with col4:
        st.metric("Total Weight", f"{total_weight/1e6:.2f}M tonnes")
    
    st.markdown("---")
    
    # Year range
    year_range = f"{df['year'].min():.0f} - {df['year'].max():.0f}"
    st.info(f"Date Range: {year_range}")
    
    # Quick summary charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 5 countries - optimize for large datasets
        try:
            if len(df) > 2000000:
                # Sample for faster groupby on very large datasets
                sample_size = min(1000000, len(df))
                sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
                df_sample = df.loc[sample_indices]
                top_countries = df_sample.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(5)
            else:
                top_countries = df.groupby('country_description')['valuecif'].sum().sort_values(ascending=False).head(5)
        except Exception as e:
            st.error(f"Error calculating top countries: {str(e)}")
            return
        fig = px.bar(
            x=top_countries.values / 1e9,
            y=top_countries.index,
            orientation='h',
            title="Top 5 Countries by Import Value (CIF)",
            labels={'x': 'Value (Billions AUD)', 'y': 'Country'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        # Top 5 commodities - optimize for large datasets
        try:
            if len(df) > 2000000:
                # Sample for faster groupby on very large datasets
                sample_size = min(1000000, len(df))
                sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
                df_sample = df.loc[sample_indices]
                top_commodities_df = df_sample.groupby('commodity_description')['valuecif'].sum().sort_values(ascending=False).head(5).reset_index()
            else:
                top_commodities_df = df.groupby('commodity_description')['valuecif'].sum().sort_values(ascending=False).head(5).reset_index()
        except Exception as e:
            st.error(f"Error calculating top commodities: {str(e)}")
            return
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
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')

def show_time_series(df):
    """Display time series analysis"""
    st.markdown('<h2 class="section-header">Time Series Analysis</h2>', unsafe_allow_html=True)
    
    # Monthly trends - optimize for large datasets
    try:
        if len(df) > 2000000:
            # Sample for faster groupby on very large datasets
            sample_size = min(2000000, len(df))
            sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
            df_sample = df.loc[sample_indices]
            monthly_stats = df_sample.groupby(['year', 'month_number', 'month']).agg({
                'valuefob': 'sum',
                'valuecif': 'sum',
                'weight': 'sum',
                'quantity': 'sum'
            }).reset_index()
            # Scale up the sums to approximate full dataset (if sampling)
            scale_factor = len(df) / len(df_sample)
            for col in ['valuefob', 'valuecif', 'weight', 'quantity']:
                monthly_stats[col] = monthly_stats[col] * scale_factor
        else:
            monthly_stats = df.groupby(['year', 'month_number', 'month']).agg({
                'valuefob': 'sum',
                'valuecif': 'sum',
                'weight': 'sum',
                'quantity': 'sum'
            }).reset_index()
    except Exception as e:
        st.error(f"Error calculating time series: {str(e)}")
        return
    
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
    st.plotly_chart(fig, width='stretch')
    
    # Year-over-Year comparison - optimize for large datasets
    st.subheader("Year-over-Year Comparison")
    
    try:
        if len(df) > 2000000:
            # Sample for faster groupby
            sample_size = min(2000000, len(df))
            sample_indices = np.random.choice(df.index, size=sample_size, replace=False)
            df_sample = df.loc[sample_indices]
            yearly_stats = df_sample.groupby('year').agg({
                'valuefob': 'sum',
                'valuecif': 'sum',
                'weight': 'sum',
                'quantity': 'sum'
            }).reset_index()
            # Scale up to approximate full dataset
            scale_factor = len(df) / len(df_sample)
            for col in ['valuefob', 'valuecif', 'weight', 'quantity']:
                yearly_stats[col] = yearly_stats[col] * scale_factor
        else:
            yearly_stats = df.groupby('year').agg({
                'valuefob': 'sum',
                'valuecif': 'sum',
                'weight': 'sum',
                'quantity': 'sum'
            }).reset_index()
    except Exception as e:
        st.error(f"Error calculating yearly stats: {str(e)}")
        return
    
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
            st.plotly_chart(fig, width='stretch')
        
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
                st.plotly_chart(fig, width='stretch')

def show_geographic_analysis(df):
    """Display geographic analysis"""
    st.markdown('<h2 class="section-header">🌍 Geographic Analysis</h2>', unsafe_allow_html=True)
    
    # Top Countries
    st.subheader("Top Countries by Import Value")
    
    try:
        country_stats = _optimize_groupby_for_large_df(
            df, 
            'country_description', 
            {'valuecif': 'sum', 'weight': 'sum'}
        ).sort_values('valuecif', ascending=False)
    except Exception as e:
        st.error(f"Error calculating country stats: {str(e)}")
        return
    
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
        st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')
    
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
    st.plotly_chart(fig, width='stretch')
    
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
    st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')

def show_commodity_analysis(df):
    """Display commodity analysis"""
    st.markdown('<h2 class="section-header"> Commodity Analysis</h2>', unsafe_allow_html=True)
    
    try:
        commodity_stats = _optimize_groupby_for_large_df(
            df,
            'commodity_description',
            {'valuefob': 'sum', 'valuecif': 'sum', 'weight': 'sum'}
        ).sort_values('valuecif', ascending=False)
    except Exception as e:
        st.error(f"Error calculating commodity stats: {str(e)}")
        return
    
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
    st.plotly_chart(fig, width='stretch')
    
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
    st.plotly_chart(fig, width='stretch')
    
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
    st.plotly_chart(fig, width='stretch')
    
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
        st.plotly_chart(fig, width='stretch')

def show_value_volume_analysis(df):
    """Display value vs volume analysis"""
    st.markdown('<h2 class="section-header">Value vs Volume Analysis</h2>', unsafe_allow_html=True)
    
    if 'industry_sector' not in df.columns:
        if COMMODITY_MAPPING_AVAILABLE:
            df['industry_sector'] = df['commodity_code'].apply(map_commodity_code_to_sitc_industry)
        else:
            df['industry_sector'] = "Unknown"
    
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
        st.plotly_chart(fig, width='stretch')
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
        st.plotly_chart(fig, width='stretch')
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
        st.plotly_chart(fig, width='stretch')
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
        st.plotly_chart(fig, width='stretch')
    
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
            st.plotly_chart(fig, width='stretch')
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
        st.plotly_chart(fig, width='stretch')
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
                        st.plotly_chart(fig, width='stretch')
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
                        st.plotly_chart(fig, width='stretch')
                    else:
                        st.info("No growing suppliers found.")

def show_transport_mode_analysis(df):
    """Display transport mode analysis"""
    st.markdown('<h2 class="section-header">Transport Mode Analysis</h2>', unsafe_allow_html=True)
    
    try:
        mode_stats = _optimize_groupby_for_large_df(
            df,
            'mode_description',
            {'valuefob': 'sum', 'valuecif': 'sum', 'weight': 'sum', 'quantity': 'sum'}
        ).sort_values('valuefob', ascending=False)
    except Exception as e:
        st.error(f"Error calculating mode stats: {str(e)}")
        return
    
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
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        fig = px.pie(
            mode_stats,
            values='valuefob',
            names='mode_description',
            title="Transport Mode Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig, width='stretch')
    
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
    st.plotly_chart(fig, width='stretch')

def show_key_insights(df):
    """Display key insights and summary"""
    st.markdown('<h2 class="section-header">Key Insights</h2>', unsafe_allow_html=True)
    
    # Calculate key metrics - optimize for large datasets
    try:
        country_stats = _optimize_groupby_for_large_df(
            df, 'country_description', {'valuecif': 'sum'}
        ).sort_values('valuecif', ascending=False)
        country_stats['valuecif_pct'] = (country_stats['valuecif'] / country_stats['valuecif'].sum()) * 100
        
        commodity_stats = _optimize_groupby_for_large_df(
            df, 'commodity_description', {'valuecif': 'sum'}
        ).sort_values('valuecif', ascending=False)
        commodity_stats['valuecif_pct'] = (commodity_stats['valuecif'] / commodity_stats['valuecif'].sum()) * 100
        
        mode_stats = _optimize_groupby_for_large_df(
            df, 'mode_description', {'valuefob': 'sum'}
        ).sort_values('valuefob', ascending=False)
        mode_stats['valuefob_pct'] = (mode_stats['valuefob'] / mode_stats['valuefob'].sum()) * 100
        
        state_stats = _optimize_groupby_for_large_df(
            df, 'state', {'valuecif': 'sum'}
        ).sort_values('valuecif', ascending=False)
        state_stats['valuecif_pct'] = (state_stats['valuecif'] / state_stats['valuecif'].sum()) * 100
        
        yearly_stats = _optimize_groupby_for_large_df(
            df, 'year', {'valuefob': 'sum'}
        )
    except Exception as e:
        st.error(f"Error calculating insights: {str(e)}")
        return
    
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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        st.info("Dashboard stopped by user.")
    except Exception as e:
        # Catch ALL errors and display them
        import traceback
        error_trace = traceback.format_exc()
        
        st.error("**Dashboard Error**")
        st.error(f"An error occurred: {str(e)}")
        st.error(f"Error type: {type(e).__name__}")
        
        with st.expander("Show detailed error information"):
            st.code(error_trace)
        
        st.info("""
        **Common issues:**
        1. Missing data file - Check GCS configuration in Streamlit secrets
        2. Missing dependencies - Check requirements.txt  
        3. Data format issues - Verify CSV file structure
        4. Secrets configuration - Verify Streamlit secrets are set correctly
        
        **To fix:**
        - Check Streamlit Cloud logs for more details
        - Verify all secrets are configured correctly
        - Ensure data file exists in GCS bucket
        - Try uploading a file using the file uploader
        """)

