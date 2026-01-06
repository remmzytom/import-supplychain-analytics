"""
Automated Data Pipeline Runner
Checks ABS website for new data, downloads, merges, processes, and uploads to GCS
Designed to run weekly via GitHub Actions
"""

import sys
import os
import logging
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import tempfile
from google.cloud import storage
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from imports_extractor import extract_imports_2024_2025
from run_pipeline import (
    step2_clean_data,
    step3_analyze_data,
    CLEANED_DATA_FILE,
    DATA_DIR,
    BASE_DIR
)

# Configuration
ABS_DATA_URL = "https://aueprod01ckanstg.blob.core.windows.net/public-catalogue/public/82d5fb9d-61ae-4ddd-873b-5c9501b6b743/imports.csv.zip"
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'freight-import-data')
GCS_FILE_NAME = os.getenv('GCS_FILE_NAME', 'imports_2024_2025_cleaned.csv')
CHECK_INTERVAL_DAYS = 7  # Check weekly

# Email configuration (from environment variables)
EMAIL_FROM = os.getenv('EMAIL_FROM')
EMAIL_TO = os.getenv('EMAIL_TO')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # App password for Gmail
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT') or '587')  # Handle empty string

# Logging setup
log_file = Path('automation.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Ensure log file exists
log_file.touch(exist_ok=True)


def check_for_new_data():
    """
    Check ABS website for new data using two methods:
    1. Check Last-Modified header (file modification date)
    2. Download and check latest date in data (if header suggests new data)
    
    Returns: (has_new_data: bool, last_modified: datetime, latest_data_date: datetime)
    """
    try:
        logger.info("Checking ABS website for new data...")
        
        # Method 1: Check Last-Modified header
        response = requests.head(ABS_DATA_URL, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        last_modified_str = response.headers.get('Last-Modified')
        if last_modified_str:
            from email.utils import parsedate_to_datetime
            last_modified = parsedate_to_datetime(last_modified_str)
            logger.info(f"ABS file Last-Modified: {last_modified}")
        else:
            logger.warning("Last-Modified header not found, using current time")
            last_modified = datetime.now()
        
        # Check if file was modified in the last week
        days_since_modification = (datetime.now(last_modified.tzinfo) - last_modified).days
        logger.info(f"Days since last modification: {days_since_modification}")
        
        # Method 2: Check if data exists in GCS
        latest_data_date = None
        data_exists_in_gcs = False
        try:
            # Try to get latest date from GCS
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(GCS_FILE_NAME)
            
            if blob.exists():
                data_exists_in_gcs = True
                # Download a sample to check latest date
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    
                    # Read last few rows to get latest date
                    df_sample = pd.read_csv(tmp_file.name, nrows=1000)
                    if 'year' in df_sample.columns and 'month_number' in df_sample.columns:
                        df_sample['date'] = pd.to_datetime(
                            df_sample['year'].astype(str) + '-' + 
                            df_sample['month_number'].astype(str).str.zfill(2) + '-01'
                        )
                        latest_data_date = df_sample['date'].max()
                        logger.info(f"Latest date in existing data: {latest_data_date}")
                    
                    os.unlink(tmp_file.name)
        except Exception as e:
            logger.warning(f"Could not check existing data date: {e}")
            # If we can't check existing data, assume we need to update
            latest_data_date = None
            data_exists_in_gcs = False
        
        # Always process if no data exists in GCS (initial setup)
        if not data_exists_in_gcs:
            logger.info("No existing data in GCS. Processing initial data upload...")
            return True, last_modified, latest_data_date
        
        # If file was modified recently, we likely have new data
        if days_since_modification <= CHECK_INTERVAL_DAYS:
            logger.info("New data detected (file modified recently)")
            return True, last_modified, latest_data_date
        else:
            logger.info("No new data detected (file not modified recently and data already exists)")
            return False, last_modified, latest_data_date
        
    except Exception as e:
        logger.error(f"Error checking for new data: {e}")
        raise


def download_and_merge_data():
    """
    Download new data and merge with existing data
    Strategy: Append new data to existing (keep full history)
    """
    try:
        logger.info("Downloading new data from ABS...")
        
        # Download new data
        new_df = extract_imports_2024_2025()
        
        if new_df is None or len(new_df) == 0:
            logger.error("Failed to download new data")
            return None
        
        logger.info(f"Downloaded {len(new_df):,} new records")
        
        # Try to load existing data from GCS
        existing_df = None
        try:
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(GCS_FILE_NAME)
            
            if blob.exists():
                logger.info("Loading existing data from GCS...")
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    existing_df = pd.read_csv(tmp_file.name, low_memory=False)
                    logger.info(f"Loaded {len(existing_df):,} existing records")
                    os.unlink(tmp_file.name)
        except Exception as e:
            logger.warning(f"Could not load existing data: {e}. Starting fresh.")
        
        # Merge data: Append new to existing (keep full history)
        if existing_df is not None:
            # Remove duplicates based on key columns (if any)
            # Combine both dataframes
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates if they exist (based on unique identifiers)
            # Assuming we have year, month, country, commodity_code, etc.
            key_columns = ['year', 'month_number', 'country_code', 'commodity_code', 'ausport_code']
            if all(col in combined_df.columns for col in key_columns):
                before_count = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=key_columns, keep='last')
                after_count = len(combined_df)
                if before_count != after_count:
                    logger.info(f"Removed {before_count - after_count:,} duplicate records")
            
            logger.info(f"Merged data: {len(existing_df):,} existing + {len(new_df):,} new = {len(combined_df):,} total")
            return combined_df
        else:
            logger.info("No existing data found. Using new data only.")
            return new_df
            
    except Exception as e:
        logger.error(f"Error downloading/merging data: {e}")
        raise


def upload_to_gcs(file_path):
    """Upload cleaned data file to Google Cloud Storage"""
    try:
        logger.info(f"Uploading {file_path} to GCS...")
        
        # Check if credentials file exists (from GitHub Actions)
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials from: {creds_path}")
            client = storage.Client.from_service_account_json(creds_path)
        else:
            # Try default credentials (for local or if env var not set)
            logger.info("Using default credentials")
            client = storage.Client()
        
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_FILE_NAME)
        
        logger.info(f"Uploading to bucket: {GCS_BUCKET_NAME}, file: {GCS_FILE_NAME}")
        blob.upload_from_filename(file_path)
        logger.info(f"Successfully uploaded to gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}")
        
        return True
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        logger.error(f"Bucket: {GCS_BUCKET_NAME}, File: {GCS_FILE_NAME}")
        logger.error(f"Credentials path: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")
        raise


def send_email_notification(success=True, message=""):
    """Send email notification about automation status"""
    if not EMAIL_FROM or not EMAIL_TO or not EMAIL_PASSWORD:
        logger.warning("Email not configured. Skipping notification.")
        return
    
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = f"Freight Import Data Pipeline - {'SUCCESS' if success else 'FAILURE'}"
        
        body = f"""
Freight Import Data Pipeline Automation Report
{'=' * 50}

Status: {'SUCCESS' if success else 'FAILURE'}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{message}

---
This is an automated message from the Freight Import Data Pipeline.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info("Email notification sent successfully")
    except Exception as e:
        logger.error(f"Error sending email notification: {e}")


def run_automation():
    """Main automation function"""
    start_time = datetime.now()
    success = False
    message = ""
    
    # Ensure DATA_DIR exists
    DATA_DIR.mkdir(exist_ok=True)
    
    try:
        logger.info("=" * 60)
        logger.info("AUTOMATED DATA PIPELINE - WEEKLY UPDATE")
        logger.info("=" * 60)
        
        # Step 1: Check for new data
        has_new_data, last_modified, latest_data_date = check_for_new_data()
        
        if not has_new_data:
            message = f"No new data detected. Last modified: {last_modified}"
            logger.info(message)
            send_email_notification(success=True, message=message)
            return True
        
        # Step 2: Download and merge data
        merged_df = download_and_merge_data()
        if merged_df is None:
            raise Exception("Failed to download/merge data")
        
        # Step 3: Save merged data temporarily
        DATA_DIR.mkdir(exist_ok=True)
        temp_raw_file = DATA_DIR / "imports_merged_temp.csv"
        logger.info(f"Saving merged data to {temp_raw_file}...")
        merged_df.to_csv(temp_raw_file, index=False)
        
        # Step 4: Clean data
        logger.info("Cleaning merged data...")
        temp_cleaned_file = DATA_DIR / "imports_merged_temp_cleaned.csv"
        result = step2_clean_data(input_path=str(temp_raw_file), output_path=str(temp_cleaned_file))
        if result is None or not temp_cleaned_file.exists():
            raise Exception("Data cleaning failed")
        
        # Step 5: Analyze data
        logger.info("Analyzing data...")
        analysis_result = step3_analyze_data(input_path=str(temp_cleaned_file))
        if analysis_result is None:
            raise Exception("Data analysis failed")
        
        # Step 6: Upload to GCS
        upload_to_gcs(str(temp_cleaned_file))
        
        # Step 7: Cleanup temporary files
        if temp_raw_file.exists():
            os.unlink(temp_raw_file)
        if temp_cleaned_file.exists():
            # Keep cleaned file for now, will be replaced next run
            pass
        
        # Success!
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        
        message = f"""
Pipeline completed successfully!

Summary:
- New data detected: Yes
- Last modified: {last_modified}
- Total records processed: {len(merged_df):,}
- Duration: {duration:.1f} minutes
- Data uploaded to: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}

Dashboard will auto-update on Streamlit Cloud.
        """
        
        logger.info(message)
        success = True
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        
        error_type = type(e).__name__
        error_message = str(e)
        
        message = f"""
Pipeline FAILED!

Error Type: {error_type}
Error: {error_message}
Duration: {duration:.1f} minutes

Please check logs for details.
        """
        
        logger.error("=" * 60)
        logger.error("PIPELINE FAILURE DETAILS")
        logger.error("=" * 60)
        logger.error(message)
        logger.error("=" * 60)
        logger.exception("Full error traceback:")
        success = False
    
    finally:
        # Send notification (don't let email failure stop the process)
        try:
            send_email_notification(success=success, message=message)
        except Exception as email_error:
            logger.warning(f"Failed to send email notification: {email_error}")
    
    return success


if __name__ == "__main__":
    success = run_automation()
    sys.exit(0 if success else 1)

