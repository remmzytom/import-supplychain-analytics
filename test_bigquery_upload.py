"""
One-time script to upload existing GCS data to BigQuery for testing
Run this manually to test BigQuery upload functionality
"""

import os
import sys
import logging
import tempfile
from google.cloud import storage
from google.cloud import bigquery

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'freight-import-data').strip('"').strip("'").strip()
GCS_FILE_NAME = os.getenv('GCS_FILE_NAME', 'imports_2024_2025_cleaned.csv').strip('"').strip("'").strip()
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'freight_import_data').strip('"').strip("'").strip()
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'imports_cleaned').strip('"').strip("'").strip()
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', '').strip('"').strip("'").strip()

def download_from_gcs():
    """Download CSV from GCS"""
    try:
        logger.info(f"Downloading {GCS_FILE_NAME} from GCS bucket {GCS_BUCKET_NAME}...")
        
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            client = storage.Client.from_service_account_json(creds_path)
        else:
            client = storage.Client()
        
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_FILE_NAME)
        
        if not blob.exists():
            logger.error(f"File {GCS_FILE_NAME} not found in bucket {GCS_BUCKET_NAME}")
            return None
        
        # Download to temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False)
        blob.download_to_filename(temp_file.name)
        temp_file.close()
        
        logger.info(f"Downloaded to {temp_file.name}")
        return temp_file.name
        
    except Exception as e:
        logger.error(f"Error downloading from GCS: {e}")
        import traceback
        traceback.print_exc()
        return None

def upload_to_bigquery(file_path):
    """Upload CSV to BigQuery"""
    try:
        logger.info(f"Uploading {file_path} to BigQuery...")
        
        dataset_id = BIGQUERY_DATASET
        table_id = BIGQUERY_TABLE
        
        logger.info(f"BigQuery Dataset: {dataset_id}")
        logger.info(f"BigQuery Table: {table_id}")
        
        # Initialize BigQuery client
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials from: {creds_path}")
            client = bigquery.Client.from_service_account_json(creds_path)
            # Get project ID from credentials if not set
            if not BIGQUERY_PROJECT:
                import json
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                    project_id = creds_data.get('project_id')
                    if project_id:
                        client.project = project_id
                        logger.info(f"Using project ID from credentials: {project_id}")
        else:
            logger.info("Using default credentials")
            client = bigquery.Client()
        
        project_id = client.project or BIGQUERY_PROJECT
        if not project_id:
            raise ValueError("BigQuery project ID not found. Set BIGQUERY_PROJECT environment variable.")
        
        logger.info(f"Using project: {project_id}")
        
        # Construct full table reference
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        logger.info(f"Full table reference: {table_ref}")
        
        # Check if dataset exists, create if not
        try:
            dataset = client.get_dataset(dataset_id)
            logger.info(f"Dataset '{dataset_id}' already exists")
        except Exception:
            logger.info(f"Creating dataset '{dataset_id}'...")
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"
            dataset = client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset '{dataset_id}' created")
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
        )
        
        # Load data from CSV file
        logger.info(f"Loading data from {file_path} to {table_ref}...")
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        # Wait for job to complete
        logger.info(f"BigQuery load job started: {job.job_id}")
        job.result()
        
        # Get table info
        table = client.get_table(table_ref)
        logger.info(f"Successfully loaded {table.num_rows:,} rows to {table_ref}")
        logger.info(f"Table size: {table.num_bytes / (1024**2):.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("BIGQUERY UPLOAD ERROR")
        logger.error("=" * 60)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Dataset: {BIGQUERY_DATASET}")
        logger.error(f"Table: {BIGQUERY_TABLE}")
        logger.error(f"Project: {BIGQUERY_PROJECT or 'Auto-detect'}")
        import traceback
        logger.exception("Full traceback:")
        return False

def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("BIGQUERY UPLOAD TEST")
    logger.info("=" * 60)
    
    # Step 1: Download from GCS
    temp_file = download_from_gcs()
    if not temp_file:
        logger.error("Failed to download from GCS")
        return False
    
    try:
        # Step 2: Upload to BigQuery
        success = upload_to_bigquery(temp_file)
        
        if success:
            logger.info("=" * 60)
            logger.info("SUCCESS: Data uploaded to BigQuery!")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("FAILED: BigQuery upload failed")
            logger.error("=" * 60)
        
        return success
        
    finally:
        # Cleanup
        if os.path.exists(temp_file):
            os.unlink(temp_file)
            logger.info(f"Cleaned up temporary file: {temp_file}")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

