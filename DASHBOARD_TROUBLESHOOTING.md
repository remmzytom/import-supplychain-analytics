# Dashboard Troubleshooting Guide

## Common Issues and Solutions

### Issue 1: Dashboard Crashes on Load

**Symptoms:**
- Dashboard shows error immediately
- Blank page or "Something went wrong" message
- Error about missing data or GCS access

**Solutions:**

#### Check 1: Verify File Exists in GCS
1. Go to Google Cloud Console → Storage → Browser
2. Open bucket: `freight-import-data`
3. Verify file exists: `imports_2024_2025_cleaned.csv` (or your configured file name)
4. If file doesn't exist:
   - Run the GitHub Actions workflow manually
   - Wait for it to complete (6-7 minutes)
   - Check the logs to confirm upload succeeded

#### Check 2: Verify Streamlit Secrets
1. Go to Streamlit Cloud → Your App → Settings → Secrets
2. Verify secrets are configured correctly:
   ```toml
   [gcp]
   bucket_name = "freight-import-data"
   file_name = "imports_2024_2025_cleaned.csv"
   
   [gcp.credentials]
   type = "service_account"
   project_id = "your-project-id"
   private_key_id = "..."
   private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   client_email = "your-service-account@project.iam.gserviceaccount.com"
   # ... rest of credentials
   ```

#### Check 3: Check Service Account Permissions
1. Go to Google Cloud Console → Storage → Browser
2. Click bucket: `freight-import-data`
3. Click **Permissions** tab
4. Verify service account has **Storage Object Viewer** role:
   - Service account: `streamlit-cloud-access@manifest-wind-478914-p8.iam.gserviceaccount.com`
   - Role: `Storage Object Viewer` (or `Storage Admin`)

#### Check 4: Check File Name Match
- **Automation uploads to:** Check `GCS_FILE_NAME` secret in GitHub (usually `imports_2024_2025_cleaned.csv`)
- **Dashboard expects:** Check `gcp.file_name` in Streamlit secrets
- **They must match exactly!**

---

### Issue 2: "File not found" Error

**Solution:**
1. Check if automation has run successfully
2. Verify the file name in GCS matches Streamlit secrets
3. Check file permissions in GCS bucket

---

### Issue 3: "Permission Denied" Error

**Solution:**
1. Grant **Storage Object Viewer** role to Streamlit Cloud service account
2. Go to: GCS → Bucket → Permissions → Grant Access
3. Principal: `streamlit-cloud-access@manifest-wind-478914-p8.iam.gserviceaccount.com`
4. Role: `Storage Object Viewer`

---

### Issue 4: "Credentials Error"

**Solution:**
1. Verify credentials JSON is valid
2. Check that all required fields are present:
   - `type`
   - `project_id`
   - `private_key_id`
   - `private_key`
   - `client_email`
   - `client_id`
   - `auth_uri`
   - `token_uri`
   - `auth_provider_x509_cert_url`
   - `client_x509_cert_url`
3. Ensure `private_key` includes `\n` characters (not actual newlines)

---

### Issue 5: Dashboard Loads But Shows No Data

**Solution:**
1. Check if data file is empty or corrupted
2. Verify file was uploaded completely (check file size)
3. Check dashboard logs for data loading errors
4. Try uploading a test file manually

---

## Quick Diagnostic Steps

1. **Check GitHub Actions Logs:**
   - Go to GitHub → Actions → Latest run
   - Verify upload step completed successfully
   - Check for any errors

2. **Check Streamlit Cloud Logs:**
   - Go to Streamlit Cloud → Your App → Logs
   - Look for error messages
   - Check what file it's trying to load

3. **Verify GCS File:**
   - Go to Google Cloud Console
   - Check file exists and has correct name
   - Verify file size is reasonable (> 0 bytes)

4. **Test Credentials Locally:**
   ```python
   from google.cloud import storage
   client = storage.Client.from_service_account_json('path/to/credentials.json')
   bucket = client.bucket('freight-import-data')
   blob = bucket.blob('imports_2024_2025_cleaned.csv')
   print(f"Exists: {blob.exists()}")
   print(f"Size: {blob.size} bytes")
   ```

---

## File Name Configuration

Make sure these match:

1. **GitHub Secret:** `GCS_FILE_NAME` = `imports_2024_2025_cleaned.csv`
2. **Streamlit Secret:** `gcp.file_name` = `imports_2024_2025_cleaned.csv`
3. **Actual file in GCS:** `imports_2024_2025_cleaned.csv`

---

## Still Having Issues?

1. Check the detailed error message in the dashboard (click the expander)
2. Review Streamlit Cloud logs
3. Verify all secrets are set correctly
4. Ensure automation has run and uploaded the file
5. Check service account permissions


