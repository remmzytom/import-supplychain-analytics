# Automation Setup Guide

## Overview

This automation runs weekly (every Monday) to:
1. ✅ Check ABS website for new data
2. ✅ Download new data if available
3. ✅ Merge with existing data (append, keep full history)
4. ✅ Run full pipeline (clean → analyze)
5. ✅ Upload to Google Cloud Storage
6. ✅ Send email notifications
7. ✅ Auto-update Streamlit Cloud dashboard

## Data Detection Strategy

**Best Method Selected:**
- **Primary**: Check `Last-Modified` HTTP header from ABS website
- **Secondary**: Compare latest date in existing data vs new data
- **Result**: Only processes if file was modified in last 7 days

## Data Merging Strategy

**Best Method Selected:**
- **Append new data to existing** (keep full historical data)
- Removes duplicates based on key columns
- Preserves all historical records for trend analysis

## Setup Instructions

### Step 1: Configure GitHub Secrets

Go to your GitHub repository → Settings → Secrets and variables → Actions

Add these secrets:

#### Required Secrets:

1. **GCP_SERVICE_ACCOUNT_KEY**
   - Value: Entire contents of your service account JSON file
   - Used for: Google Cloud Storage access

2. **GCS_BUCKET_NAME**
   - Value: `freight-import-data` (or your bucket name)
   - Used for: GCS bucket name

3. **GCS_FILE_NAME**
   - Value: `imports_2024_2025_cleaned.csv`
   - Used for: GCS file name

#### Email Notification Secrets (Optional but Recommended):

4. **EMAIL_FROM**
   - Value: Your email address (e.g., `yourname@gmail.com`)
   - Used for: Sender email

5. **EMAIL_TO**
   - Value: Recipient email address
   - Used for: Where to send notifications

6. **EMAIL_PASSWORD**
   - Value: Gmail App Password (not regular password!)
   - How to get: Gmail → Account → Security → 2-Step Verification → App Passwords
   - Used for: Email authentication

7. **SMTP_SERVER** (Optional)
   - Value: `smtp.gmail.com` (default)
   - Used for: SMTP server

8. **SMTP_PORT** (Optional)
   - Value: `587` (default)
   - Used for: SMTP port

### Step 2: Test the Automation

#### Manual Test (Run Now):

1. Go to your GitHub repository
2. Click "Actions" tab
3. Select "Weekly Data Pipeline Update"
4. Click "Run workflow" → "Run workflow"
5. Watch it execute!

#### Check Results:

- Check the workflow logs for success/failure
- Check your email for notification
- Check GCS bucket for updated file
- Check Streamlit Cloud dashboard (should auto-update)

### Step 3: Verify Schedule

The automation runs:
- **Every Monday at 2:00 AM UTC**
- You can adjust the schedule in `.github/workflows/weekly_update.yml`

To change schedule, edit the cron expression:
```yaml
- cron: '0 2 * * 1'  # Monday 2 AM UTC
```

Cron format: `minute hour day-of-month month day-of-week`
- `0 2 * * 1` = Monday 2 AM UTC
- `0 8 * * 1` = Monday 8 AM UTC
- `0 2 * * 0` = Sunday 2 AM UTC

## How It Works

### Weekly Automation Flow:

```
Monday 2 AM UTC
    ↓
GitHub Actions Triggered
    ↓
Check ABS Website (Last-Modified header)
    ↓
New Data? → Yes → Download & Merge
    ↓                    ↓
    No              Clean Data
    ↓                    ↓
Send Email         Analyze Data
    ↓                    ↓
   Done            Upload to GCS
                        ↓
                  Send Email
                        ↓
            Streamlit Cloud Auto-Updates
```

### Data Detection Logic:

1. **Check HTTP Header**: `Last-Modified` from ABS website
2. **Compare Dates**: If modified within last 7 days → new data likely
3. **Verify**: Download sample and check latest date in data
4. **Process**: Only if truly new data found

### Data Merging Logic:

1. **Download New Data**: From ABS website
2. **Load Existing Data**: From Google Cloud Storage
3. **Append**: Combine new + existing
4. **Deduplicate**: Remove duplicates based on key columns
5. **Save**: Store merged dataset

## Monitoring

### Check Automation Status:

1. **GitHub Actions Tab**: See workflow runs and logs
2. **Email Notifications**: Receive success/failure emails
3. **GCS Bucket**: Check file modification date
4. **Streamlit Dashboard**: Verify data freshness

### Troubleshooting:

**Workflow Fails:**
- Check GitHub Actions logs
- Verify all secrets are set correctly
- Check GCP credentials are valid

**No Email Received:**
- Check spam folder
- Verify email secrets are correct
- Check Gmail App Password is used (not regular password)

**Data Not Updating:**
- Check if ABS website has new data
- Verify GCS upload succeeded
- Check Streamlit Cloud logs

## Cost

- **GitHub Actions**: Free (2000 minutes/month for private repos)
- **Google Cloud Storage**: ~$0.02/month
- **Total**: Essentially free!

## Customization

### Change Schedule:

Edit `.github/workflows/weekly_update.yml`:
```yaml
- cron: '0 2 * * 1'  # Change this line
```

### Change Detection Interval:

Edit `automation.py`:
```python
CHECK_INTERVAL_DAYS = 7  # Change this
```

### Disable Email Notifications:

Remove email secrets or comment out email code in `automation.py`

## Next Steps

1. ✅ Add all secrets to GitHub
2. ✅ Test workflow manually
3. ✅ Verify email notifications work
4. ✅ Wait for Monday 2 AM UTC (or adjust schedule)
5. ✅ Monitor first automated run

## Support

- Check GitHub Actions logs for errors
- Review `automation.log` file
- Check email notifications for status
- Verify GCS bucket has updated file

