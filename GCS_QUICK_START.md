# Quick Start: Google Cloud Storage Setup

## 🚀 5-Minute Setup Guide

### Step 1: Create GCS Bucket (2 minutes)

1. Go to [Google Cloud Console](https://console.cloud.google.com/storage)
2. Click "Create Bucket"
3. Name: `freight-import-data` (or your choice - must be globally unique)
4. Location: Choose closest region
5. Click "Create"

### Step 2: Upload Data File (1 minute)

1. Click your bucket name
2. Click "Upload Files"
3. Upload `data/imports_2024_2025_cleaned.csv`
4. Wait for upload

### Step 3: Create Service Account (1 minute)

1. Go to [IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Click "Create Service Account"
3. Name: `streamlit-cloud-access`
4. Grant role: **"Storage Object Viewer"**
5. Click "Create and Continue" → "Done"

### Step 4: Download JSON Key (30 seconds)

1. Click the service account
2. Go to "Keys" tab
3. "Add Key" → "Create new key" → "JSON"
4. **Save the downloaded file** - you'll need it!

### Step 5: Configure Streamlit Cloud (30 seconds)

1. Deploy your app to Streamlit Cloud (if not done)
2. Go to your app → "Settings" → "Secrets"
3. Paste this (replace with your values):

```toml
[gcp]
bucket_name = "freight-import-data"
file_name = "imports_2024_2025_cleaned.csv"

[gcp.credentials]
# Paste entire contents of your service account JSON file here
```

4. Click "Save"

### Step 6: Done! 🎉

Your dashboard will now load data from Google Cloud Storage!

## 📝 What Changed

- ✅ Dashboard now loads from GCS automatically
- ✅ Falls back to local file if GCS not configured
- ✅ Includes file uploader as backup option
- ✅ All changes pushed to GitHub

## 💰 Cost

~$0.02/month for storage (essentially free!)

## 🆘 Troubleshooting

**"Bucket not found"**: Check bucket name in secrets matches exactly

**"Permission denied"**: Ensure service account has "Storage Object Viewer" role

**"Module not found"**: Wait for Streamlit Cloud to reinstall dependencies (2-3 minutes)

## Next Steps

1. Complete Steps 1-5 above
2. Your dashboard will auto-update on Streamlit Cloud
3. Test it - data should load from GCS!

