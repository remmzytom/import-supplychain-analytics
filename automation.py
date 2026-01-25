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
from google.api_core import exceptions as google_exceptions
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

# Get bucket name from environment and clean it
_raw_bucket_name = os.getenv('GCS_BUCKET_NAME', 'freight-import-data')
GCS_BUCKET_NAME = _raw_bucket_name.strip('"').strip("'").strip()  # Remove quotes and whitespace
GCS_FILE_NAME = os.getenv('GCS_FILE_NAME', 'imports_2024_2025_cleaned.csv').strip('"').strip("'").strip()
CHECK_INTERVAL_DAYS = 7  # Check weekly

# Email configuration (from environment variables)
EMAIL_FROM = os.getenv('EMAIL_FROM')
EMAIL_TO = os.getenv('EMAIL_TO')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # App password for Gmail
# Handle empty string - if SMTP_SERVER is empty or not set, use default
_smtp_server = os.getenv('SMTP_SERVER', '').strip()
SMTP_SERVER = _smtp_server if _smtp_server else 'smtp.gmail.com'
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

# Log configuration (after logger is set up)
logger.info("=" * 60)
logger.info("CONFIGURATION")
logger.info("=" * 60)
logger.info(f"GCS_BUCKET_NAME: '{GCS_BUCKET_NAME}' (length: {len(GCS_BUCKET_NAME)}, raw: '{_raw_bucket_name}')")
logger.info(f"GCS_FILE_NAME: '{GCS_FILE_NAME}'")
if GCS_BUCKET_NAME:
    logger.info(f"Bucket name starts with: '{GCS_BUCKET_NAME[0]}' (isalnum: {GCS_BUCKET_NAME[0].isalnum()})")
    logger.info(f"Bucket name ends with: '{GCS_BUCKET_NAME[-1]}' (isalnum: {GCS_BUCKET_NAME[-1].isalnum()})")
else:
    logger.error("GCS_BUCKET_NAME is empty!")

# Validate bucket name format
if not GCS_BUCKET_NAME:
    error_msg = "GCS_BUCKET_NAME environment variable is not set or is empty!"
    logger.error(error_msg)
    raise ValueError(error_msg)

if not (GCS_BUCKET_NAME[0].isalnum() and GCS_BUCKET_NAME[-1].isalnum()):
    error_msg = f"INVALID BUCKET NAME: '{GCS_BUCKET_NAME}' does not meet GCS requirements!"
    error_msg += f"\n  - Must start with letter/number (starts with: '{GCS_BUCKET_NAME[0]}' - isalnum: {GCS_BUCKET_NAME[0].isalnum()})"
    error_msg += f"\n  - Must end with letter/number (ends with: '{GCS_BUCKET_NAME[-1]}' - isalnum: {GCS_BUCKET_NAME[-1].isalnum()})"
    error_msg += f"\n  - Raw value from env: '{_raw_bucket_name}'"
    logger.error(error_msg)
    raise ValueError(error_msg)


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
            # Check if credentials file exists (from GitHub Actions)
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and os.path.exists(creds_path):
                client = storage.Client.from_service_account_json(creds_path)
            else:
                client = storage.Client()
            
            # Clean bucket name
            bucket_name = GCS_BUCKET_NAME.strip('"').strip("'").strip()
            logger.info(f"Checking GCS bucket: '{bucket_name}' (length: {len(bucket_name)})")
            logger.info(f"Bucket name validation: starts='{bucket_name[0]}' ends='{bucket_name[-1]}'")
            bucket = client.bucket(bucket_name)
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
            # Check if credentials file exists (from GitHub Actions)
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and os.path.exists(creds_path):
                client = storage.Client.from_service_account_json(creds_path)
            else:
                client = storage.Client()
            
            # Clean bucket name
            bucket_name = GCS_BUCKET_NAME.strip('"').strip("'").strip()
            logger.info(f"Loading from GCS bucket: '{bucket_name}'")
            bucket = client.bucket(bucket_name)
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
        
        # Ensure bucket name is clean
        bucket_name = GCS_BUCKET_NAME.strip('"').strip("'").strip()
        file_name = GCS_FILE_NAME.strip('"').strip("'").strip()
        
        logger.info(f"Bucket name: '{bucket_name}' (repr: {repr(bucket_name)}, length: {len(bucket_name)})")
        logger.info(f"File name: '{file_name}'")
        logger.info(f"Bucket name bytes: {bucket_name.encode('utf-8')}")
        
        # Validate bucket name one more time
        if not bucket_name:
            raise ValueError("Bucket name is empty!")
        if not (bucket_name[0].isalnum() and bucket_name[-1].isalnum()):
            raise ValueError(f"Invalid bucket name format: '{bucket_name}' (starts: '{bucket_name[0]}', ends: '{bucket_name[-1]}')")
        
        # Check if credentials file exists (from GitHub Actions)
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        service_account_email = None
        
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials from: {creds_path}")
            # Read service account email from credentials file
            try:
                import json
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                    service_account_email = creds_data.get('client_email', 'Unknown')
                    logger.info(f"Service account email: {service_account_email}")
            except Exception as creds_e:
                logger.warning(f"Could not read service account email: {creds_e}")
            
            client = storage.Client.from_service_account_json(creds_path)
        else:
            # Try default credentials (for local or if env var not set)
            logger.info("Using default credentials")
            try:
                client = storage.Client()
                # Try to get the service account email from default credentials
                try:
                    service_account_email = client.get_service_account_email()
                    logger.info(f"Default service account email: {service_account_email}")
                except:
                    pass
            except Exception as default_e:
                logger.error(f"Failed to initialize default credentials: {default_e}")
                raise
        
        logger.info(f"Creating bucket reference for: '{bucket_name}'")
        bucket = client.bucket(bucket_name)
        logger.info(f"Creating blob reference for: '{file_name}'")
        blob = bucket.blob(file_name)
        
        logger.info(f"Starting upload to gs://{bucket_name}/{file_name}")
        try:
            blob.upload_from_filename(file_path)
            logger.info(f"Successfully uploaded to gs://{bucket_name}/{file_name}")
            return True
        except google_exceptions.Forbidden as perm_error:
            error_msg = f"""
{'=' * 60}
PERMISSION DENIED ERROR
{'=' * 60}
The service account '{service_account_email or 'Unknown'}' does not have permission 
to upload objects to bucket '{bucket_name}'.

Required permission: storage.objects.create

To fix this:
1. Go to Google Cloud Console → Storage → Browser
2. Select bucket: {bucket_name}
3. Click "Permissions" tab
4. Click "Grant Access"
5. Add Principal: {service_account_email or 'YOUR_SERVICE_ACCOUNT_EMAIL'}
6. Add Role: Storage Object Creator (roles/storage.objectCreator)
   OR Storage Admin (roles/storage.admin) for full access
7. Click "Save"

If you don't know the service account email, check your GitHub secret 
'GCP_SERVICE_ACCOUNT_KEY' - it should contain a 'client_email' field.

Original error: {str(perm_error)}
{'=' * 60}
            """
            logger.error(error_msg)
            raise Exception(error_msg.strip()) from perm_error
        
    except google_exceptions.Forbidden as perm_error:
        # Catch permission errors that might occur earlier
        error_msg = f"""
{'=' * 60}
PERMISSION DENIED ERROR
{'=' * 60}
Service account '{service_account_email or 'Unknown'}' lacks permissions 
for bucket '{bucket_name}'.

Please grant 'Storage Object Creator' role to: {service_account_email or 'YOUR_SERVICE_ACCOUNT_EMAIL'}

Original error: {str(perm_error)}
{'=' * 60}
        """
        logger.error(error_msg)
        raise Exception(error_msg.strip()) from perm_error
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("GCS UPLOAD ERROR")
        logger.error("=" * 60)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Service account: {service_account_email or 'Unknown'}")
        logger.error(f"Bucket name used: '{GCS_BUCKET_NAME}' (repr: {repr(GCS_BUCKET_NAME)})")
        logger.error(f"File name used: '{GCS_FILE_NAME}'")
        logger.error(f"Credentials path: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")
        logger.exception("Full traceback:")
        raise


def send_email_notification(success=True, message=""):
    """Send email notification about automation status"""
    # Log email configuration status (without exposing passwords)
    logger.info("=" * 60)
    logger.info("EMAIL NOTIFICATION CHECK")
    logger.info("=" * 60)
    logger.info(f"EMAIL_FROM configured: {'Yes' if EMAIL_FROM else 'No'}")
    logger.info(f"EMAIL_TO configured: {'Yes' if EMAIL_TO else 'No'}")
    logger.info(f"EMAIL_PASSWORD configured: {'Yes' if EMAIL_PASSWORD else 'No'}")
    logger.info(f"SMTP_SERVER: {SMTP_SERVER}")
    logger.info(f"SMTP_PORT: {SMTP_PORT}")
    
    if not EMAIL_FROM or not EMAIL_TO or not EMAIL_PASSWORD:
        logger.warning("Email not configured. Skipping notification.")
        logger.warning(f"Missing: EMAIL_FROM={bool(EMAIL_FROM)}, EMAIL_TO={bool(EMAIL_TO)}, EMAIL_PASSWORD={bool(EMAIL_PASSWORD)}")
        return
    
    try:
        logger.info(f"Preparing email notification...")
        logger.info(f"From: {EMAIL_FROM}")
        logger.info(f"To: {EMAIL_TO}")
        
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = f"Freight Import Data Pipeline - {'SUCCESS' if success else 'FAILURE'}"
        
        status_emoji = "✅" if success else "❌"
        status_color = "green" if success else "red"
        
        body = f"""
Freight Import Data Pipeline Automation Report
{'=' * 60}

{status_emoji} Status: {'SUCCESS' if success else 'FAILURE'}
📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

{message}

---
This is an automated message from the Freight Import Data Pipeline.
You are receiving this because EMAIL_TO is configured in GitHub Secrets.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        logger.info(f"Connecting to SMTP server: {SMTP_SERVER}:{SMTP_PORT}")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        logger.info("Starting TLS...")
        server.starttls()
        logger.info("Logging in to SMTP server...")
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        logger.info("Sending email message...")
        server.send_message(msg)
        logger.info("Closing SMTP connection...")
        server.quit()
        
        logger.info("=" * 60)
        logger.info("✅ Email notification sent successfully!")
        logger.info(f"   Sent to: {EMAIL_TO}")
        logger.info("=" * 60)
    except smtplib.SMTPAuthenticationError as auth_error:
        logger.error("=" * 60)
        logger.error("❌ SMTP Authentication Error!")
        logger.error(f"Error: {str(auth_error)}")
        logger.error("Possible causes:")
        logger.error("1. Incorrect email password (should be App Password, not regular password)")
        logger.error("2. 2-Step Verification not enabled")
        logger.error("3. App Password not generated correctly")
        logger.error("=" * 60)
    except smtplib.SMTPException as smtp_error:
        logger.error("=" * 60)
        logger.error("❌ SMTP Error!")
        logger.error(f"Error: {str(smtp_error)}")
        logger.error("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ Error sending email notification!")
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        import traceback
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)


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
            # Don't send email here - let finally block handle it to avoid duplicate emails
            success = True
            # Continue to finally block which will send the email
        
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
        try:
            analysis_result = step3_analyze_data(input_path=str(temp_cleaned_file))
            if analysis_result is None:
                logger.warning("Analysis returned None, but continuing...")
            else:
                logger.info("Data analysis completed successfully")
        except Exception as analysis_error:
            logger.warning(f"Analysis step had an error: {analysis_error}")
            logger.warning("Continuing with upload despite analysis error...")
            # Don't fail the pipeline if analysis has issues - we still want to upload data
        
        # Step 6: Upload to GCS
        try:
            upload_to_gcs(str(temp_cleaned_file))
            logger.info("GCS upload completed successfully")
        except Exception as upload_error:
            logger.error(f"GCS upload failed: {upload_error}")
            logger.exception("GCS upload traceback:")
            # Don't fail the entire pipeline if upload fails - we can retry
            raise Exception(f"Failed to upload to GCS: {upload_error}")
        
        # Step 7: Cleanup temporary files
        if temp_raw_file.exists():
            os.unlink(temp_raw_file)
        if temp_cleaned_file.exists():
            # Keep cleaned file for now, will be replaced next run
            pass
        
        # Success!
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        
        # Check if BigQuery upload was successful
        bigquery_status = "Not attempted"
        try:
            from google.cloud import bigquery
            BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'freight_import_data')
            BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'imports_cleaned')
            BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', '')
            
            # Try to check if BigQuery table exists and has data
            client = bigquery.Client()
            if BIGQUERY_PROJECT:
                client.project = BIGQUERY_PROJECT
            
            table_ref = f"{client.project}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
            try:
                table = client.get_table(table_ref)
                bigquery_status = f"Success - {table.num_rows:,} rows"
            except:
                bigquery_status = "Table not found"
        except Exception as bq_check_error:
            bigquery_status = f"Check failed: {str(bq_check_error)[:50]}"
        
        message = f"""
Pipeline completed successfully!

Summary:
- New data detected: Yes
- Last modified: {last_modified}
- Latest data date: {latest_data_date}
- Total records processed: {len(merged_df):,}
- Duration: {duration:.1f} minutes
- Data uploaded to GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}
- BigQuery status: {bigquery_status}

Dashboard will auto-update on Streamlit Cloud.
Next check: {datetime.now() + timedelta(days=CHECK_INTERVAL_DAYS)} (weekly)
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
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

Please check GitHub Actions logs for details:
- Go to your repository → Actions tab
- Check the latest workflow run
- Review automation.log artifact

The pipeline will retry automatically next week.
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
            if 'message' in locals():
                send_email_notification(success=success, message=message)
            else:
                logger.warning("No message to send in notification")
        except Exception as email_error:
            logger.warning(f"Failed to send email notification: {email_error}")
            # Don't fail the pipeline if email fails
            pass
    
    logger.info("=" * 60)
    logger.info(f"Pipeline completed. Success: {success}")
    logger.info("=" * 60)
    return success


if __name__ == "__main__":
    success = run_automation()
    sys.exit(0 if success else 1)

