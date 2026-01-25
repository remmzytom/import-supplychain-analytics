# Email Notification Setup Checklist

## Verify GitHub Secrets Are Set

Go to: **Repository → Settings → Secrets and variables → Actions**

Check that these secrets exist (exact names, case-sensitive):

- [ ] `EMAIL_FROM` - Your Gmail address
- [ ] `EMAIL_TO` - Recipient email  
- [ ] `EMAIL_PASSWORD` - Gmail App Password (16 characters, no spaces)
- [ ] `SMTP_SERVER` - `smtp.gmail.com` (optional, defaults to this)
- [ ] `SMTP_PORT` - `587` (optional, defaults to this)

## Common Issues

### Issue 1: Email Password Format
- ❌ Wrong: `abcd efgh ijkl mnop` (with spaces)
- ✅ Correct: `abcdefghijklmnop` (no spaces)

### Issue 2: Using Regular Password Instead of App Password
- ❌ Wrong: Your regular Gmail password
- ✅ Correct: Gmail App Password (16 characters)

### Issue 3: Secrets Not Visible in Workflow
- Make sure secrets are in: **Settings → Secrets and variables → Actions**
- NOT in: **Settings → Secrets and variables → Dependabot**

### Issue 4: Email Going to Spam
- Check your spam/junk folder
- Gmail may filter automated emails

## How to Check Logs

1. Go to **Actions** tab
2. Click latest workflow run
3. Click **"Run automated pipeline"** step
4. Search for: `EMAIL NOTIFICATION CHECK`
5. Look for these messages:
   - `EMAIL_FROM configured: Yes/No`
   - `EMAIL_TO configured: Yes/No`
   - `EMAIL_PASSWORD configured: Yes/No`
   - Any error messages

## Test Email Configuration

After verifying secrets, manually trigger the workflow:
1. Go to **Actions** tab
2. Select **"Weekly Data Pipeline Update"**
3. Click **"Run workflow"**
4. Check logs for email status

