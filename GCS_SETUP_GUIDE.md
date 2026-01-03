# Google Cloud Storage Setup Guide for Streamlit Cloud

## Step-by-Step Setup

### Step 1: Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with your Google account
3. Click "Select a project" → "New Project"
4. Name: `freight-import-analytics` (or your choice)
5. Click "Create"
6. Wait for project creation (30 seconds)

### Step 2: Enable Google Cloud Storage API

1. In Google Cloud Console, go to "APIs & Services" → "Library"
2. Search for "Cloud Storage API"
3. Click "Cloud Storage API" → "Enable"

### Step 3: Create Storage Bucket

1. Go to "Cloud Storage" → "Buckets"
2. Click "Create Bucket"
3. Fill in:
   - **Name**: `freight-import-data` (must be globally unique)
   - **Location type**: Region
   - **Location**: Choose closest to you (e.g., `us-central1`)
   - **Storage class**: Standard
   - **Access control**: Uniform
   - **Public access**: Uncheck (we'll use service account)
4. Click "Create"

### Step 4: Upload Data File

**Option A: Using Google Cloud Console (Web UI)**
1. Click on your bucket name
2. Click "Upload Files"
3. Upload `imports_2024_2025_cleaned.csv`
4. Wait for upload to complete

**Option B: Using gsutil (Command Line)**
```bash
# Install Google Cloud SDK first: https://cloud.google.com/sdk/docs/install
gsutil cp data/imports_2024_2025_cleaned.csv gs://freight-import-data/
```

### Step 5: Create Service Account for Streamlit Cloud

1. Go to "IAM & Admin" → "Service Accounts"
2. Click "Create Service Account"
3. Fill in:
   - **Name**: `streamlit-cloud-access`
   - **Description**: "Access for Streamlit Cloud dashboard"
4. Click "Create and Continue"
5. **Grant role**: "Storage Object Viewer" (read-only access)
6. Click "Continue" → "Done"

### Step 6: Create and Download JSON Key

1. Click on the service account you just created
2. Go to "Keys" tab
3. Click "Add Key" → "Create new key"
4. Choose "JSON"
5. Click "Create"
6. **IMPORTANT**: Save the downloaded JSON file securely
7. Copy the file contents - you'll need it for Streamlit Cloud secrets

### Step 7: Configure Streamlit Cloud Secrets

1. Deploy your app to Streamlit Cloud first (see STREAMLIT_CLOUD_DEPLOY.md)
2. Go to your Streamlit Cloud app → "Settings" → "Secrets"
3. Add the following:

```toml
[gcp]
bucket_name = "freight-import-data"
file_name = "imports_2024_2025_cleaned.csv"

[gcp.credentials]
# Paste the entire contents of your service account JSON file here
type = "service_account"
project_id = "your-project-id"
private_key_id = "your-private-key-id"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "your-service-account@project.iam.gserviceaccount.com"
client_id = "your-client-id"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
```

**Or simpler approach**: Just paste the entire JSON file content under `[gcp.credentials]`:

```toml
[gcp]
bucket_name = "freight-import-data"
file_name = "imports_2024_2025_cleaned.csv"

[gcp.credentials]
# Paste entire service account JSON here
```

### Step 8: Update Dashboard Code

The dashboard code has been updated to load from GCS. Make sure `dashboard.py` includes the GCS loading function.

### Step 9: Update Requirements

Add to `requirements.txt`:
```
google-cloud-storage>=2.10.0
```

### Step 10: Push Changes and Redeploy

```bash
git add dashboard.py requirements.txt
git commit -m "Add Google Cloud Storage support"
git push
```

Streamlit Cloud will auto-redeploy.

## Cost Estimate

- **Storage**: ~$0.02/month for 1.3GB (Standard storage)
- **Data transfer**: Free (within Google Cloud)
- **API calls**: Free (within free tier limits)
- **Total**: ~$0.02-0.05/month

## Troubleshooting

### Error: "Bucket not found"
- Check bucket name in Streamlit secrets matches exactly
- Ensure bucket exists in your GCP project

### Error: "Permission denied"
- Verify service account has "Storage Object Viewer" role
- Check JSON credentials are correct in Streamlit secrets

### Error: "Module not found: google.cloud.storage"
- Ensure `google-cloud-storage` is in `requirements.txt`
- Wait for Streamlit Cloud to reinstall dependencies

### Slow loading
- First load caches data (takes ~30-60 seconds)
- Subsequent loads are fast due to Streamlit caching

## Security Notes

- Service account has read-only access (can't modify data)
- Credentials stored securely in Streamlit Cloud secrets
- Bucket is private (not publicly accessible)
- Only your Streamlit app can access the data

## Next Steps

1. Complete Steps 1-6 above
2. Deploy to Streamlit Cloud (if not done)
3. Add secrets in Streamlit Cloud
4. Push updated code
5. Test dashboard!

