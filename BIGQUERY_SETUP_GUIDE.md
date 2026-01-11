# BigQuery Setup Guide

This guide will help you set up BigQuery for efficient data querying, solving the memory issues with large datasets.

## Why BigQuery?

- **Memory Efficient**: Query only what you need, not entire CSV files
- **Fast**: BigQuery processes queries on Google's servers
- **Scalable**: Handles billions of rows easily
- **Free Tier**: 10GB storage + 1TB queries/month (your dataset fits!)

## Step 1: Enable BigQuery API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project (or create one)
3. Go to **APIs & Services** → **Library**
4. Search for "BigQuery API"
5. Click **Enable**

## Step 2: Create BigQuery Dataset

1. Go to **BigQuery** in Google Cloud Console
2. Click on your project name
3. Click **Create Dataset**
4. Fill in:
   - **Dataset ID**: `freight_import_data` (or your preferred name)
   - **Location**: `US` (or your preferred region)
5. Click **Create Dataset**

## Step 3: Grant Permissions to Service Account

Your service account needs BigQuery permissions:

1. Go to **IAM & Admin** → **IAM**
2. Find your service account (the one used for GCS)
3. Click **Edit** (pencil icon)
4. Click **Add Another Role**
5. Add these roles:
   - **BigQuery Data Editor** (`roles/bigquery.dataEditor`) - to write data
   - **BigQuery Job User** (`roles/bigquery.jobUser`) - to run queries
   - **BigQuery User** (`roles/bigquery.user`) - to read data
6. Click **Save**

## Step 4: Configure GitHub Actions Secrets

Add these environment variables to your GitHub repository secrets:

1. Go to your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
2. Add these secrets (if not already added):

```
BIGQUERY_DATASET=freight_import_data
BIGQUERY_TABLE=imports_cleaned
BIGQUERY_PROJECT=your-project-id
```

**Note**: `BIGQUERY_PROJECT` is optional - it will be detected from your service account credentials if not set.

## Step 5: Configure Streamlit Cloud Secrets

Add BigQuery configuration to Streamlit secrets:

1. Go to [Streamlit Cloud](https://share.streamlit.io/)
2. Select your app
3. Go to **Settings** → **Secrets**
4. Add/update your `gcp` section:

```toml
[gcp]
bucket_name = "freight-import-data"
file_name = "imports_2024_2025_cleaned.csv"
bigquery_dataset = "freight_import_data"
bigquery_table = "imports_cleaned"
bigquery_project = "your-project-id"  # Optional, will auto-detect

[gcp.credentials]
type = "service_account"
project_id = "your-project-id"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "your-service-account@project.iam.gserviceaccount.com"
# ... rest of credentials
```

## Step 6: Test the Setup

### Test Automation Upload

1. Run your automation (manually or wait for scheduled run)
2. Check logs for:
   ```
   Uploading to BigQuery...
   Successfully loaded X rows to project.dataset.table
   ```

### Test Dashboard Query

1. Deploy/update your Streamlit app
2. Check logs for:
   ```
   Querying BigQuery...
   Query returned X rows
   ```

## Troubleshooting

### Error: "BigQuery API not enabled"
- **Solution**: Enable BigQuery API (Step 1)

### Error: "Permission denied"
- **Solution**: Grant BigQuery roles to service account (Step 3)

### Error: "Dataset not found"
- **Solution**: Create dataset in BigQuery (Step 2)

### Error: "Table not found"
- **Solution**: Run automation once to create table, or create manually

### Dashboard still crashes
- **Check**: Are you querying BigQuery or still downloading CSV?
- **Check logs**: Look for "Querying BigQuery..." vs "Loading from GCS..."
- **Solution**: Ensure BigQuery secrets are configured correctly

## How It Works

### Before (GCS only):
```
Dashboard → Download entire CSV (4.4M rows) → Load into memory → CRASH
```

### After (BigQuery):
```
Dashboard → Query BigQuery → "SELECT * LIMIT 2000000" → Returns 2M rows → Works!
```

## Cost Estimate

**Free Tier Includes:**
- 10GB storage (your dataset: ~1GB) ✅
- 1TB queries/month (your queries: ~10-50GB/month) ✅

**You should stay within free tier!**

## Next Steps

1. ✅ Enable BigQuery API
2. ✅ Create dataset
3. ✅ Grant permissions
4. ✅ Configure secrets
5. ✅ Run automation (will upload to BigQuery)
6. ✅ Update dashboard (will query BigQuery)

## Support

If you encounter issues:
1. Check Google Cloud Console → BigQuery → Job History
2. Check Streamlit Cloud logs
3. Check GitHub Actions logs

