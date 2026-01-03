"""
Freight Import Data Pipeline - Complete Automation Script
==========================================================
This file contains the complete data pipeline from extraction to dashboard.

Pipeline Steps:
1. Data Extraction - Downloads raw data from ABS website (uses imports_extractor.py)
2. Data Cleaning - Cleans and preprocesses data (uses logic from import_data_cleaning.ipynb)
3. Data Analysis - Generates summary statistics
4. Dashboard Deployment - Launches Streamlit dashboard (uses dashboard.py)

"""

import sys
import os
import argparse
import logging
import subprocess
import json
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Base directory for the project
BASE_DIR = Path(__file__).parent

# Data directories
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Data Extraction Configuration
EXTRACT_YEARS = ['2024', '2025']
RAW_DATA_FILE = DATA_DIR / "imports_2024_2025.csv"
CLEANED_DATA_FILE = DATA_DIR / "imports_2024_2025_cleaned.csv"

# Data Cleaning Configuration
CLEAN_CHUNK_SIZE = 100000

# Data Analysis Configuration
ANALYSIS_OUTPUT_DIR = OUTPUT_DIR / "analysis"
ANALYSIS_OUTPUT_DIR.mkdir(exist_ok=True)
TOP_N_COUNTRIES = 15
TOP_N_COMMODITIES = 15
TOP_N_PORTS = 10

# Dashboard Configuration
DASHBOARD_PORT = 8501
DASHBOARD_HOST = "localhost"

# Logging Configuration
LOG_FILE = LOGS_DIR / "pipeline.log"
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Pipeline Execution Configuration
RUN_EXTRACTION = True
RUN_CLEANING = True
RUN_ANALYSIS = True
RUN_DASHBOARD = True
CONTINUE_ON_ERROR = False

# Set up logging
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# STEP 1: DATA EXTRACTION
# ============================================================================
# Uses your existing imports_extractor.py module

def step1_extract_data(year_filter=None, output_path=None):
    """
    Step 1: Extract data from ABS website
    Uses: imports_extractor.py
    """
    try:
        logger.info("=" * 60)
        logger.info("STEP 1: DATA EXTRACTION")
        logger.info("=" * 60)
        logger.info("Using existing imports_extractor.py module")
        
        if output_path is None:
            output_path = RAW_DATA_FILE
        if year_filter is None:
            year_filter = EXTRACT_YEARS
        
        # Check if file already exists
        if os.path.exists(output_path):
            logger.info(f"Raw data file already exists: {output_path}")
            logger.info("Skipping extraction. Using existing file.")
            # Verify file is readable
            try:
                sample = pd.read_csv(output_path, nrows=1000)
                logger.info(f"Verified existing file contains {len(sample):,} sample records")
                return True
            except Exception as e:
                logger.warning(f"Existing file may be corrupted: {e}")
                logger.info("Proceeding with extraction to overwrite...")
        
        # Import the existing extraction module
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from imports_extractor import extract_imports_data, extract_imports_2024_2025
        
        logger.info(f"Extracting data for years: {year_filter}")
        
        # Use the existing extraction function
        if year_filter == ['2024', '2025']:
            df = extract_imports_2024_2025()
        else:
            df = extract_imports_data(year_filter=year_filter)
        
        if df is not None:
            # Handle file path mapping
            years_str = "_".join(year_filter) if year_filter else "all"
            existing_path = f"data/imports_{years_str}.csv"
            
            if os.path.exists(existing_path) and existing_path != str(output_path):
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                shutil.move(existing_path, output_path)
                logger.info(f"Moved data to: {output_path}")
            elif not os.path.exists(output_path) and os.path.exists(existing_path):
                logger.info(f"Using existing file: {existing_path}")
                output_path = existing_path
            
            logger.info(f"Data available at: {output_path}")
            logger.info(f"Total records: {len(df):,}")
            logger.info("\n[SUCCESS] STEP 1 COMPLETE: Data extraction successful!")
            logger.info("=" * 60 + "\n")
        else:
            logger.error("Extraction failed")
        
        return df
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


# ============================================================================
# STEP 2: DATA CLEANING
# ============================================================================
# Uses logic from your import_data_cleaning.ipynb notebook

def _clean_chunk(chunk):
    """
    Clean a single chunk of data
    Logic from: import_data_cleaning.ipynb
    """
    df_clean = chunk.copy()
    
    # Step 1: Standardize column names (from notebook Cell 9)
    df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_')
    
    # Step 2: Convert numeric columns (from notebook Cell 10)
    numeric_columns = ['weight', 'valuefob', 'valuecif', 'quantity']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].fillna(0)
            negative_count = (df_clean[col] < 0).sum()
            if negative_count > 0:
                df_clean[col] = df_clean[col].clip(lower=0)
    
    # Step 3: Clean text columns (from notebook Cell 11)
    text_columns = ['country_description', 'commodity_description', 'mode_description', 
                    'ausport_description', 'osport_description', 'state', 'unit_quantity']
    
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].replace({'nan': 'Unknown', 'NaN': 'Unknown', 'None': 'Unknown'})
            df_clean[col] = df_clean[col].fillna('Unknown')
            df_clean[col] = df_clean[col].str.strip()
            df_clean[col] = df_clean[col].str.replace(r'\s+', ' ', regex=True)
            df_clean[col] = df_clean[col].str.replace(r'[\n\r\t]+', ' ', regex=True)
            df_clean[col] = df_clean[col].str.replace('"', '', regex=False)
            df_clean[col] = df_clean[col].str.replace("'", '', regex=False)
    
    # Step 4: Standardize unit names (from notebook Cell 10)
    if 'unit_quantity' in df_clean.columns:
        unit_mapping = {
            'Litres Al': 'Litres',
            'litres al': 'Litres',
            'LITRES AL': 'Litres',
        }
        df_clean['unit_quantity'] = df_clean['unit_quantity'].map(unit_mapping).fillna(df_clean['unit_quantity'])
        
        # Round Number unit quantities to integers (from notebook Cell 11)
        number_mask = df_clean['unit_quantity'] == 'Number'
        if number_mask.any():
            df_clean.loc[number_mask, 'quantity'] = pd.to_numeric(
                df_clean.loc[number_mask, 'quantity'], errors='coerce'
            ).round(0)
    
    # Step 5: Clean month column and create month_number (from notebook Cell 13)
    if 'month' in df_clean.columns:
        df_clean['month'] = df_clean['month'].astype(str)
        month_name = df_clean['month'].str.extract(r'^([A-Za-z]+)')[0]
        df_clean['month'] = month_name
        
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        df_clean['month_number'] = df_clean['month'].map(month_map)
    
    # Step 6: Create derived features (from notebook Cell 14)
    if 'weight' in df_clean.columns and 'valuefob' in df_clean.columns:
        denominator = df_clean['weight'].replace(0, np.nan)
        df_clean['value_per_tonne_fob'] = df_clean['valuefob'] / denominator
    
    if 'weight' in df_clean.columns and 'valuecif' in df_clean.columns:
        denominator = df_clean['weight'].replace(0, np.nan)
        df_clean['value_per_tonne_cif'] = df_clean['valuecif'] / denominator
    
    if 'valuecif' in df_clean.columns and 'valuefob' in df_clean.columns:
        df_clean['insurance_freight_cost'] = df_clean['valuecif'] - df_clean['valuefob']
        df_clean['insurance_freight_cost'] = df_clean['insurance_freight_cost'].clip(lower=0)
    
    # Ensure year column exists
    if 'year' not in df_clean.columns:
        original_month = chunk.get('month', None)
        if original_month is not None:
            df_clean['year'] = original_month.astype(str).str.extract(r'(\d{4})')
            df_clean['year'] = pd.to_numeric(df_clean['year'], errors='coerce')
            df_clean['year'] = df_clean['year'].fillna(0).astype(int)
        else:
            df_clean['year'] = 0
    
    return df_clean


def step2_clean_data(input_path=None, output_path=None):
    """
    Step 2: Clean and preprocess raw data
    Uses: Logic from import_data_cleaning.ipynb
    """
    try:
        logger.info("=" * 60)
        logger.info("STEP 2: DATA CLEANING")
        logger.info("=" * 60)
        logger.info("Using logic from import_data_cleaning.ipynb")
        
        if input_path is None:
            input_path = RAW_DATA_FILE
        if output_path is None:
            output_path = CLEANED_DATA_FILE
        
        if not Path(input_path).exists():
            logger.error(f"Input file not found: {input_path}")
            return None
        
        if Path(output_path).exists():
            logger.info(f"Cleaned data file already exists: {output_path}")
            logger.info("Skipping cleaning. Using existing cleaned file.")
            # Verify file is readable
            try:
                sample = pd.read_csv(output_path, nrows=1000)
                logger.info(f"Verified existing cleaned file contains {len(sample):,} sample records")
                return True
            except Exception as e:
                logger.warning(f"Existing cleaned file may be corrupted: {e}")
                logger.info("Proceeding with cleaning to overwrite...")
        
        logger.info(f"Loading raw data from: {input_path}")
        
        # Load and clean data in chunks
        chunks = []
        total_rows = 0
        
        for chunk in pd.read_csv(input_path, chunksize=CLEAN_CHUNK_SIZE):
            total_rows += len(chunk)
            cleaned_chunk = _clean_chunk(chunk)
            if len(cleaned_chunk) > 0:
                chunks.append(cleaned_chunk)
            if total_rows % 500000 == 0:
                logger.info(f"Processed {total_rows:,} rows...")
        
        logger.info(f"Combining {len(chunks)} cleaned chunks...")
        
        # Combine chunks in batches
        batch_size = 5
        combined_chunks = []
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i+batch_size]
            logger.info(f"Combining batch {i//batch_size + 1}/{(len(chunks)-1)//batch_size + 1}")
            combined_batch = pd.concat(batch, ignore_index=True)
            combined_chunks.append(combined_batch)
            del batch, combined_batch
            import gc
            gc.collect()
        
        df_clean = pd.concat(combined_chunks, ignore_index=True)
        
        # Remove duplicates
        initial_count = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        duplicates_removed = initial_count - len(df_clean)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed:,} duplicate rows")
        
        # Save cleaned data
        logger.info(f"Saving cleaned data to: {output_path}")
        df_clean.to_csv(output_path, index=False)
        logger.info(f"[SUCCESS] Cleaned data saved successfully!")
        
        logger.info("\n" + "=" * 60)
        logger.info("CLEANING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Final record count: {len(df_clean):,}")
        logger.info(f"Columns: {len(df_clean.columns)}")
        logger.info("\n[SUCCESS] STEP 2 COMPLETE: Data cleaning successful!")
        logger.info("=" * 60 + "\n")
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error during cleaning: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


# ============================================================================
# STEP 3: DATA ANALYSIS
# ============================================================================

def step3_analyze_data(input_path=None, output_dir=None):
    """
    Step 3: Generate summary statistics and insights
    """
    try:
        logger.info("=" * 60)
        logger.info("STEP 3: DATA ANALYSIS")
        logger.info("=" * 60)
        
        if input_path is None:
            input_path = CLEANED_DATA_FILE
        if output_dir is None:
            output_dir = ANALYSIS_OUTPUT_DIR
        
        if not Path(input_path).exists():
            logger.error(f"Input file not found: {input_path}")
            return None
        
        logger.info(f"Loading cleaned data from: {input_path}")
        
        # Initialize summary statistics
        summary_stats = {
            'total_records': 0,
            'date_range': {},
            'total_value_fob': 0,
            'total_value_cif': 0,
            'total_weight': 0,
            'total_quantity': 0,
            'top_countries': [],
            'top_commodities': [],
            'top_ports': [],
            'transport_modes': {},
            'states': {}
        }
        
        # Process in chunks to calculate statistics
        chunk_size = 100000
        chunks_processed = 0
        
        for chunk in pd.read_csv(input_path, chunksize=chunk_size):
            chunks_processed += 1
            summary_stats['total_records'] += len(chunk)
            summary_stats['total_value_fob'] += chunk['valuefob'].sum()
            summary_stats['total_value_cif'] += chunk['valuecif'].sum()
            summary_stats['total_weight'] += chunk['weight'].sum()
            summary_stats['total_quantity'] += chunk['quantity'].sum()
            if chunks_processed % 10 == 0:
                logger.info(f"Processed {chunks_processed} chunks...")
        
        logger.info(f"Processed {chunks_processed} chunks")
        logger.info(f"Total records: {summary_stats['total_records']:,}")
        
        # Load sample for detailed analysis
        logger.info("Loading data for detailed analysis...")
        df = pd.read_csv(input_path, nrows=1000000)
        if summary_stats['total_records'] <= 1000000:
            df = pd.read_csv(input_path)
        
        logger.info(f"Analyzing {len(df):,} records...")
        
        # Date range
        if 'year' in df.columns:
            summary_stats['date_range'] = {
                'min_year': int(df['year'].min()),
                'max_year': int(df['year'].max()),
                'years': sorted(df['year'].unique().tolist())
            }
        
        # Top countries
        if 'country_description' in df.columns and 'valuecif' in df.columns:
            country_stats = df.groupby('country_description')['valuecif'].sum().sort_values(ascending=False)
            summary_stats['top_countries'] = [
                {'country': country, 'value_cif': float(value)}
                for country, value in country_stats.head(TOP_N_COUNTRIES).items()
            ]
        
        # Top commodities
        if 'commodity_description' in df.columns and 'valuecif' in df.columns:
            commodity_stats = df.groupby('commodity_description')['valuecif'].sum().sort_values(ascending=False)
            summary_stats['top_commodities'] = [
                {'commodity': commodity, 'value_cif': float(value)}
                for commodity, value in commodity_stats.head(TOP_N_COMMODITIES).items()
            ]
        
        # Top ports
        if 'ausport_description' in df.columns and 'valuecif' in df.columns:
            port_stats = df.groupby('ausport_description')['valuecif'].sum().sort_values(ascending=False)
            summary_stats['top_ports'] = [
                {'port': port, 'value_cif': float(value)}
                for port, value in port_stats.head(TOP_N_PORTS).items()
            ]
        
        # Transport modes
        if 'mode_description' in df.columns and 'valuecif' in df.columns:
            mode_stats = df.groupby('mode_description')['valuecif'].sum().sort_values(ascending=False)
            summary_stats['transport_modes'] = {
                mode: float(value) for mode, value in mode_stats.items()
            }
        
        # States
        if 'state' in df.columns and 'valuecif' in df.columns:
            state_stats = df.groupby('state')['valuecif'].sum().sort_values(ascending=False)
            summary_stats['states'] = {
                state: float(value) for state, value in state_stats.items()
            }
        
        # Save summary statistics
        output_file = Path(output_dir) / "summary_statistics.json"
        logger.info(f"Saving summary statistics to: {output_file}")
        with open(output_file, 'w') as f:
            json.dump(summary_stats, f, indent=2)
        
        # Create report
        report_file = Path(output_dir) / "analysis_report.txt"
        logger.info(f"Creating analysis report: {report_file}")
        with open(report_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("DATA ANALYSIS SUMMARY REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Total Records: {summary_stats['total_records']:,}\n\n")
            f.write("Date Range:\n")
            if summary_stats['date_range']:
                f.write(f"  Years: {summary_stats['date_range'].get('min_year')} - {summary_stats['date_range'].get('max_year')}\n\n")
            f.write("Total Values:\n")
            f.write(f"  FOB Value: ${summary_stats['total_value_fob']/1e9:.2f} Billion AUD\n")
            f.write(f"  CIF Value: ${summary_stats['total_value_cif']/1e9:.2f} Billion AUD\n")
            f.write(f"  Total Weight: {summary_stats['total_weight']/1e6:.2f} Million Tonnes\n\n")
            f.write(f"Top {TOP_N_COUNTRIES} Countries by CIF Value:\n")
            for i, country in enumerate(summary_stats['top_countries'][:TOP_N_COUNTRIES], 1):
                f.write(f"  {i}. {country['country']}: ${country['value_cif']/1e9:.2f}B\n")
        
        logger.info("\n[SUCCESS] STEP 3 COMPLETE: Data analysis successful!")
        logger.info("=" * 60 + "\n")
        
        return summary_stats
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


# ============================================================================
# STEP 4: DASHBOARD DEPLOYMENT
# ============================================================================
# Uses your existing dashboard.py

def step4_run_dashboard(port=None, host=None):
    """
    Step 4: Launch Streamlit dashboard
    Uses: dashboard.py
    """
    try:
        logger.info("=" * 60)
        logger.info("STEP 4: DASHBOARD DEPLOYMENT")
        logger.info("=" * 60)
        logger.info("Using existing dashboard.py")
        
        if port is None:
            port = DASHBOARD_PORT
        if host is None:
            host = DASHBOARD_HOST
        
        # Check if cleaned data exists
        if not Path(CLEANED_DATA_FILE).exists():
            logger.error(f"Cleaned data file not found: {CLEANED_DATA_FILE}")
            logger.error("Please run data extraction and cleaning steps first.")
            return False
        
        # Check if dashboard.py exists
        dashboard_file = Path(__file__).parent / "dashboard.py"
        if not dashboard_file.exists():
            logger.error(f"Dashboard file not found: {dashboard_file}")
            return False
        
        logger.info(f"Starting Streamlit dashboard...")
        logger.info(f"  Dashboard file: {dashboard_file}")
        logger.info(f"  Data file: {CLEANED_DATA_FILE}")
        logger.info(f"  URL: http://{host}:{port}")
        logger.info("\n" + "=" * 60)
        logger.info("DASHBOARD STARTING...")
        logger.info("=" * 60)
        # Check if port is already in use
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port_in_use = sock.connect_ex((host, port)) == 0
        sock.close()
        
        if port_in_use:
            logger.warning(f"Port {port} is already in use!")
            logger.info(f"Dashboard is already running at http://{host}:{port}")
            logger.info("The dashboard should be accessible in your browser.")
            logger.info("If you want to restart it, stop the existing process first:")
            logger.info(f"  Windows: taskkill /PID <process_id> /F")
            logger.info(f"  Or use: streamlit stop (if Streamlit CLI is available)")
            return True  # Consider this a success since dashboard is accessible
        
        logger.info(f"\nOpen your browser and navigate to: http://{host}:{port}")
        logger.info("\nPress Ctrl+C to stop the dashboard.\n")
        
        # Run Streamlit command
        cmd = [
            sys.executable,
            "-m", "streamlit", "run",
            str(dashboard_file),
            "--server.port", str(port),
            "--server.address", host,
            "--server.headless", "true"
        ]
        
        subprocess.run(cmd)
        return True
        
    except KeyboardInterrupt:
        logger.info("\nDashboard stopped by user.")
        return True
    except Exception as e:
        logger.error(f"Error starting dashboard: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def check_dashboard_requirements():
    """Check if dashboard requirements are met"""
    missing = []
    if not Path(CLEANED_DATA_FILE).exists():
        missing.append(f"Cleaned data file: {CLEANED_DATA_FILE}")
    dashboard_file = Path(__file__).parent / "dashboard.py"
    if not dashboard_file.exists():
        missing.append(f"Dashboard file: {dashboard_file}")
    try:
        import streamlit
    except ImportError:
        missing.append("streamlit package (install with: pip install streamlit)")
    try:
        import plotly
    except ImportError:
        missing.append("plotly package (install with: pip install plotly)")
    return len(missing) == 0, missing


# ============================================================================
# MAIN PIPELINE ORCHESTRATOR
# ============================================================================

def print_banner():
    """Print pipeline banner"""
    banner = """
    ================================================================
    
         FREIGHT IMPORT DATA PIPELINE
         Complete Automation Script
    
    ================================================================
    """
    print(banner)


def run_pipeline(
    run_extract=RUN_EXTRACTION,
    run_clean=RUN_CLEANING,
    run_analyze=RUN_ANALYSIS,
    run_dash=RUN_DASHBOARD,
    continue_on_error=CONTINUE_ON_ERROR
):
    """
    Run the complete data pipeline
    
    Args:
        run_extract: Whether to run extraction step
        run_clean: Whether to run cleaning step
        run_analyze: Whether to run analysis step
        run_dash: Whether to run dashboard step
        continue_on_error: Whether to continue if a step fails
    
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    print_banner()
    
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION STARTED")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    results = {
        'extraction': None,
        'cleaning': None,
        'analysis': None,
        'dashboard': None
    }
    
    # Step 1: Data Extraction
    if run_extract:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: DATA EXTRACTION")
        logger.info("=" * 60)
        try:
            result = step1_extract_data()
            results['extraction'] = result is not None
            if not result and not continue_on_error:
                logger.error("Extraction failed. Stopping pipeline.")
                return False
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            results['extraction'] = False
            if not continue_on_error:
                return False
    else:
        logger.info("Skipping extraction step")
        results['extraction'] = None
    
    # Step 2: Data Cleaning
    if run_clean:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: DATA CLEANING")
        logger.info("=" * 60)
        try:
            result = step2_clean_data()
            results['cleaning'] = result is not None
            if not result and not continue_on_error:
                logger.error("Cleaning failed. Stopping pipeline.")
                return False
        except Exception as e:
            logger.error(f"Cleaning error: {e}")
            results['cleaning'] = False
            if not continue_on_error:
                return False
    else:
        logger.info("Skipping cleaning step")
        results['cleaning'] = None
    
    # Step 3: Data Analysis
    if run_analyze:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: DATA ANALYSIS")
        logger.info("=" * 60)
        try:
            result = step3_analyze_data()
            results['analysis'] = result is not None
            if not result and not continue_on_error:
                logger.error("Analysis failed. Stopping pipeline.")
                return False
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            results['analysis'] = False
            if not continue_on_error:
                return False
    else:
        logger.info("Skipping analysis step")
        results['analysis'] = None
    
    # Step 4: Dashboard Deployment
    if run_dash:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 4: DASHBOARD DEPLOYMENT")
        logger.info("=" * 60)
        try:
            requirements_met, missing = check_dashboard_requirements()
            if not requirements_met:
                logger.error("Dashboard requirements not met:")
                for item in missing:
                    logger.error(f"  - {item}")
                if not continue_on_error:
                    return False
            
            result = step4_run_dashboard()
            results['dashboard'] = result
            if not result and not continue_on_error:
                logger.error("Dashboard failed. Stopping pipeline.")
                return False
        except Exception as e:
            logger.error(f"Dashboard error: {e}")
            results['dashboard'] = False
            if not continue_on_error:
                return False
    else:
        logger.info("Skipping dashboard step")
        results['dashboard'] = None
    
    # Pipeline Summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("\nStep Results:")
    logger.info(f"  1. Extraction: {'[SUCCESS]' if results['extraction'] else '[FAILED]' if results['extraction'] is False else '[SKIPPED]'}")
    logger.info(f"  2. Cleaning:    {'[SUCCESS]' if results['cleaning'] else '[FAILED]' if results['cleaning'] is False else '[SKIPPED]'}")
    logger.info(f"  3. Analysis:    {'[SUCCESS]' if results['analysis'] else '[FAILED]' if results['analysis'] is False else '[SKIPPED]'}")
    logger.info(f"  4. Dashboard:   {'[SUCCESS]' if results['dashboard'] else '[FAILED]' if results['dashboard'] is False else '[SKIPPED]'}")
    logger.info("=" * 60)
    
    executed_steps = [k for k, v in results.items() if v is not None]
    successful_steps = [k for k, v in results.items() if v is True]
    
    if len(executed_steps) == len(successful_steps):
        logger.info("\n[SUCCESS] PIPELINE COMPLETED SUCCESSFULLY!")
        return True
    else:
        logger.warning(f"\n[WARNING] PIPELINE COMPLETED WITH WARNINGS ({len(successful_steps)}/{len(executed_steps)} steps successful)")
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run the Freight Import Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--step', choices=['extract', 'clean', 'analyze', 'dashboard'],
                      help='Run only a specific step')
    parser.add_argument('--skip-extract', action='store_true', help='Skip extraction step')
    parser.add_argument('--skip-clean', action='store_true', help='Skip cleaning step')
    parser.add_argument('--skip-analyze', action='store_true', help='Skip analysis step')
    parser.add_argument('--skip-dashboard', action='store_true', help='Skip dashboard step')
    parser.add_argument('--continue-on-error', action='store_true',
                      help='Continue pipeline even if a step fails')
    
    args = parser.parse_args()
    
    # Determine which steps to run
    if args.step:
        run_extract = args.step == 'extract'
        run_clean = args.step == 'clean'
        run_analyze = args.step == 'analyze'
        run_dash = args.step == 'dashboard'
    else:
        run_extract = RUN_EXTRACTION and not args.skip_extract
        run_clean = RUN_CLEANING and not args.skip_clean
        run_analyze = RUN_ANALYSIS and not args.skip_analyze
        run_dash = RUN_DASHBOARD and not args.skip_dashboard
    
    continue_on_error = args.continue_on_error or CONTINUE_ON_ERROR
    
    # Run pipeline
    success = run_pipeline(
        run_extract=run_extract,
        run_clean=run_clean,
        run_analyze=run_analyze,
        run_dash=run_dash,
        continue_on_error=continue_on_error
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
