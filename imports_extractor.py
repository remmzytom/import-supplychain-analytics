import pandas as pd
import zipfile
import os
import requests
import logging
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_imports_data(year_filter=None):
    """
    Extract imports data from ABS international trade website
    
    Args:
        year_filter: Optional list of years to filter (e.g., ['2024', '2025']). 
                    If None, extracts all data.
    """
    try:
        # Direct download URL for imports dataset
        url = "https://aueprod01ckanstg.blob.core.windows.net/public-catalogue/public/82d5fb9d-61ae-4ddd-873b-5c9501b6b743/imports.csv.zip"
        
        # Download complete ZIP file with streaming to avoid memory issues
        logger.info("Downloading ABS imports dataset...")
        response = requests.get(url, timeout=300, verify=False, stream=True)  # Increased timeout
        response.raise_for_status()
        
        # Get total file size if available
        total_size = int(response.headers.get('content-length', 0))
        total_size_mb = total_size / (1024 * 1024) if total_size > 0 else 0
        
        # Save ZIP file with streaming to avoid memory issues
        zip_path = "temp_imports.zip"
        logger.info(f"Saving ZIP file with streaming... (Total size: {total_size_mb:.2f} MB)" if total_size_mb > 0 else "Saving ZIP file with streaming...")
        downloaded = 0
        chunk_size = 8192  # 8KB chunks
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Show progress every 10MB
                    if downloaded % (10 * 1024 * 1024) < chunk_size:
                        downloaded_mb = downloaded / (1024 * 1024)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            logger.info(f"Downloaded: {downloaded_mb:.2f} MB / {total_size_mb:.2f} MB ({percent:.1f}%)")
                        else:
                            logger.info(f"Downloaded: {downloaded_mb:.2f} MB...")
        
        logger.info(f"Download complete! Total size: {downloaded / (1024 * 1024):.2f} MB")
        
        logger.info("Extracting and loading dataset in chunks...")
        with zipfile.ZipFile(zip_path) as zip_ref:
            # Find CSV file in ZIP
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            if not csv_files:
                logger.error("No CSV file found in ZIP archive")
                return None
            csv_file = csv_files[0]
            
            # Read CSV in chunks
            with zip_ref.open(csv_file) as f:
                logger.info("Loading dataset in chunks...")
                chunk_size = 10000  # 10k rows at a time
                chunks = []
                total_processed = 0
                
                for chunk in pd.read_csv(f, chunksize=chunk_size):
                    total_processed += len(chunk)
                    
                    # Apply year filter if specified
                    if year_filter:
                        # Extract year from month column if it exists
                        if 'month' in chunk.columns:
                            chunk['year'] = chunk['month'].astype(str).str.extract(r'(\d{4})')
                            chunk = chunk[chunk['year'].isin(year_filter)]
                    
                    if len(chunk) > 0:
                        chunks.append(chunk)
                        logger.info(f"Processed chunk with {len(chunk)} records (total processed: {total_processed:,})")
                    
                    # Clean up memory aggressively
                    del chunk
                    import gc
                    gc.collect()
                    
                    if total_processed % 500000 == 0:  # Log every 500k rows
                        logger.info(f"Processed {total_processed:,} rows...")
                
                if chunks:
                    logger.info("Combining chunks...")
                    # Process chunks in smaller batches to avoid memory issues
                    batch_size = 2  # Process 2 chunks at a time
                    combined_chunks = []
                    
                    for i in range(0, len(chunks), batch_size):
                        batch = chunks[i:i+batch_size]
                        logger.info(f"Processing batch {i//batch_size + 1}/{(len(chunks)-1)//batch_size + 1}")
                        combined_batch = pd.concat(batch, ignore_index=True)
                        combined_chunks.append(combined_batch)
                        # Clean up memory
                        del batch, combined_batch
                    
                    # Final combination
                    df = pd.concat(combined_chunks, ignore_index=True)
                    logger.info(f"Successfully combined {len(df):,} records")
                else:
                    logger.warning("No data found")
                    return None
        
        logger.info(f"Loaded {len(df):,} records from imports dataset")
        
        # Convert numeric columns if they exist
        numeric_columns = ['quantity', 'gross_weight_tonnes', 'value_fob_aud', 'value_cif_aud']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"Converted {col} to numeric")
        
        # Save data to data folder
        os.makedirs("data", exist_ok=True)
        if year_filter:
            years_str = "_".join(year_filter)
            output_path = f"data/imports_{years_str}.csv"
        else:
            output_path = "data/imports_all.csv"
        
        df.to_csv(output_path, index=False)
        logger.info(f"Saved imports data to: {output_path}")
        
        # Show summary
        print(f"\n{'='*60}")
        print(f"IMPORTS DATA ANALYSIS")
        print(f"{'='*60}")
        print(f"Total records in dataset: {len(df):,}")
        
        # Show breakdown by year if month column exists
        if 'month' in df.columns:
            df['year'] = df['month'].astype(str).str.extract(r'(\d{4})')
            print(f"\nRecords by year:")
            print("-" * 40)
            year_breakdown = df['year'].value_counts().sort_index()
            for year, count in year_breakdown.items():
                print(f"{year}: {count:,} records")
            
            # Show breakdown by month (top 15)
            print(f"\nTop 15 months by record count:")
            print("-" * 40)
            month_breakdown = df['month'].value_counts().sort_index()
            for month, count in month_breakdown.head(15).items():
                print(f"{month}: {count:,} records")
        
        # Show column information
        print(f"\nDataset columns:")
        print("-" * 40)
        for col in df.columns:
            print(f"  - {col}")
        
        # Show sample data
        print(f"\nSample imports data:")
        print("-" * 40)
        sample_cols = ['month', 'country_of_origin', 'value_fob_aud', 'value_cif_aud', 'gross_weight_tonnes']
        available_cols = [col for col in sample_cols if col in df.columns]
        if available_cols:
            print(df[available_cols].head(10).to_string(index=False))
        
        # Summary statistics
        print(f"\nSummary Statistics:")
        print("-" * 40)
        numeric_cols_available = [col for col in numeric_columns if col in df.columns]
        if numeric_cols_available:
            print(df[numeric_cols_available].describe())
        
        logger.info("Successfully processed all imports data")
        
        # Clean up temporary ZIP file
        if os.path.exists(zip_path):
            os.remove(zip_path)
            logger.info("Cleaned up temporary ZIP file")
        
        return df
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def extract_imports_2024_2025():
    """
    Extract 2024 and 2025 imports records - perfect balance for stakeholder analysis
    """
    return extract_imports_data(year_filter=['2024', '2025'])

def main():
    import sys
    
    print("Starting Imports Data Extraction...")
    print("=" * 60)
    
    # Default to 2024-2025, but allow override
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            print("Extracting all imports data...")
            result = extract_imports_data()
        else:
            years = sys.argv[1].split(',')
            print(f"Extracting imports data for years: {years}")
            result = extract_imports_data(year_filter=years)
    else:
        # Default: Extract 2024-2025 data
        print("Extracting 2024-2025 imports data (default)...")
        result = extract_imports_2024_2025()
    
    if result is not None:
        if len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1] != "--all"):
            print(f"\nSUCCESS! Extracted {len(result):,} records of 2024-2025 imports data")
            print(f" Data saved to: data/imports_2024_2025.csv")
        else:
            print(f"\nSUCCESS! Extracted {len(result):,} records of imports data")
            print(f" Data saved to: data/imports_all.csv")
    else:
        print(f"\n No imports data found or error occurred")

if __name__ == "__main__":
    main()

