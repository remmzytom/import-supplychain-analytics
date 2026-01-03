# Data File Setup for Streamlit Cloud

## ⚠️ Important: Data Files Not Included

The data files (`imports_2024_2025.csv` and `imports_2024_2025_cleaned.csv`) are **too large** for GitHub (>1GB each) and are excluded from this repository.

## Options for Streamlit Cloud Deployment

### Option 1: Upload Data File to Streamlit Cloud (Recommended)

1. **After deploying to Streamlit Cloud**, you can upload the data file:
   - Go to your Streamlit Cloud app settings
   - Use the file uploader or connect external storage
   - Or use Streamlit's secrets to store file paths

2. **Modify dashboard.py** to accept uploaded file:
   ```python
   # Add file uploader in dashboard
   uploaded_file = st.file_uploader("Upload cleaned data file", type=['csv'])
   if uploaded_file:
       df = pd.read_csv(uploaded_file)
   ```

### Option 2: Use Google Cloud Storage (Best for Production)

1. **Upload data to Google Cloud Storage:**
   ```bash
   # Install gsutil
   gsutil cp data/imports_2024_2025_cleaned.csv gs://your-bucket-name/
   ```

2. **Modify dashboard.py** to load from GCS:
   ```python
   from google.cloud import storage
   import tempfile
   
   def load_data_from_gcs():
       client = storage.Client()
       bucket = client.bucket('your-bucket-name')
       blob = bucket.blob('imports_2024_2025_cleaned.csv')
       
       with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
           blob.download_to_filename(tmp.name)
           return pd.read_csv(tmp.name)
   ```

3. **Add credentials** to Streamlit Cloud secrets:
   - Go to Streamlit Cloud → Settings → Secrets
   - Add Google Cloud credentials

### Option 3: Generate Data on First Run

Modify the dashboard to run the extraction/cleaning pipeline on first load (slower but always fresh).

### Option 4: Use Git LFS (Not Recommended for Files This Large)

Git LFS has storage limits and costs. Not ideal for 1GB+ files.

## Current Setup

The dashboard expects the data file at:
```
data/imports_2024_2025_cleaned.csv
```

**For local development**, ensure this file exists in your `data/` folder.

**For Streamlit Cloud**, you'll need to implement one of the options above.

## Quick Fix for Testing

For now, you can:
1. Deploy to Streamlit Cloud (it will show an error about missing data)
2. Then implement one of the data loading options above
3. Or use the file uploader approach for quick testing

## Recommended Approach

**For production**: Use **Google Cloud Storage** (Option 2)
- Scalable
- Reliable
- Cost-effective (~$0.02/month for storage)
- Fast loading

**For quick testing**: Use **file uploader** (Option 1)
- Simple
- No external setup needed
- Good for demos

