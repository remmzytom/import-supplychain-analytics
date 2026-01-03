# Quick Start: Deploy to Streamlit Cloud

## 🎯 3-Minute Deployment Guide

### Step 1: Push to GitHub (2 minutes)

```bash
# Navigate to project
cd Freight_import_data_project

# Initialize git (if not done)
git init

# Add all files
git add .

# Commit
git commit -m "Ready for Streamlit Cloud deployment"

# Create repo on GitHub.com, then:
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git branch -M main
git push -u origin main
```

### Step 2: Deploy on Streamlit Cloud (1 minute)

1. Go to **[share.streamlit.io](https://share.streamlit.io)**
2. Sign in with GitHub
3. Click **"New app"**
4. Fill in:
   - **Repository**: Select your repo
   - **Branch**: `main`
   - **Main file**: `dashboard.py`
   - **App URL**: Choose a name (e.g., `freight-import-dashboard`)
5. Click **"Deploy"**
6. Wait 2-5 minutes
7. **Done!** 🎉

### Your Dashboard URL
```
https://YOUR_APP_NAME.streamlit.app
```

## ✅ What's Already Prepared

- ✅ `dashboard.py` - Ready to deploy
- ✅ `requirements.txt` - All dependencies listed
- ✅ `.streamlit/config.toml` - Streamlit config
- ✅ `.gitignore` - Excludes unnecessary files
- ✅ `commodity_code_mapping.py` - Included
- ✅ Data file path configured correctly

## 📝 Important Notes

1. **Data File**: Make sure `data/imports_2024_2025_cleaned.csv` is in your repo
2. **File Size**: If > 100MB, GitHub may warn you (but should still work)
3. **Auto-Updates**: Every time you push to GitHub, Streamlit Cloud auto-updates

## 🆘 Need Help?

See `STREAMLIT_CLOUD_DEPLOY.md` for detailed instructions and troubleshooting.

