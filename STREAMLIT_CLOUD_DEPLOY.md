# Streamlit Cloud Deployment Guide

## Prerequisites

1. **GitHub Account**: You need a GitHub account
2. **Streamlit Cloud Account**: Sign up at [share.streamlit.io](https://share.streamlit.io) (free)
3. **Data File**: Your cleaned data file (`data/imports_2024_2025_cleaned.csv`)

## Step-by-Step Deployment

### Step 1: Prepare Your Repository

1. **Initialize Git** (if not already done):
   ```bash
   cd Freight_import_data_project
   git init
   ```

2. **Create .gitignore** (already created):
   - Excludes unnecessary files like `__pycache__`, logs, etc.
   - **Note**: Data files are included by default. If your data file is too large (>100MB), consider using Git LFS or external storage.

### Step 2: Check Data File Size

Check if your data file is manageable:
```bash
# Windows PowerShell
Get-Item "data\imports_2024_2025_cleaned.csv" | Select-Object Name, @{Name="Size(MB)";Expression={[math]::Round($_.Length / 1MB, 2)}}
```

**If file is > 100MB:**
- Option A: Use Git LFS (Large File Storage)
- Option B: Store data in Google Cloud Storage and modify dashboard to load from there
- Option C: Compress the data file

### Step 3: Commit Files to Git

```bash
git add .
git commit -m "Initial commit for Streamlit Cloud deployment"
```

**Required files for deployment:**
- `dashboard.py` (main dashboard file)
- `commodity_code_mapping.py` (required dependency)
- `requirements.txt` (Python dependencies)
- `data/imports_2024_2025_cleaned.csv` (data file)
- `.streamlit/config.toml` (Streamlit configuration)
- `.gitignore` (Git ignore rules)

**Optional but recommended:**
- `README.md` (project documentation)
- `STREAMLIT_CLOUD_DEPLOY.md` (this file)

### Step 4: Push to GitHub

1. **Create a new repository on GitHub:**
   - Go to [github.com](https://github.com)
   - Click "New repository"
   - Name it (e.g., `freight-import-dashboard`)
   - Choose Public or Private
   - **Don't** initialize with README (you already have files)

2. **Push your code:**
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
   git branch -M main
   git push -u origin main
   ```

### Step 5: Deploy to Streamlit Cloud

1. **Go to Streamlit Cloud:**
   - Visit [share.streamlit.io](https://share.streamlit.io)
   - Sign in with your GitHub account

2. **Deploy New App:**
   - Click "New app"
   - Select your repository: `YOUR_USERNAME/YOUR_REPO_NAME`
   - Select branch: `main`
   - Main file path: `dashboard.py`
   - App URL: Choose a custom name (e.g., `freight-import-dashboard`)

3. **Click "Deploy"**

4. **Wait for deployment:**
   - Streamlit Cloud will install dependencies from `requirements.txt`
   - It will run `streamlit run dashboard.py`
   - First deployment takes 2-5 minutes

### Step 6: Access Your Dashboard

Once deployed, you'll get a URL like:
```
https://YOUR_APP_NAME.streamlit.app
```

Share this URL with your stakeholders!

## Troubleshooting

### Issue: "Data file not found"
- **Solution**: Ensure `data/imports_2024_2025_cleaned.csv` is in your repository
- Check file path in `dashboard.py` (line 90)

### Issue: "Module not found"
- **Solution**: Add missing package to `requirements.txt`
- Common missing packages: `openpyxl`, `xlrd`, etc.

### Issue: "File too large"
- **Solution**: Use Git LFS for large files:
  ```bash
  git lfs install
  git lfs track "data/*.csv"
  git add .gitattributes
  git add data/
  git commit -m "Add data files with LFS"
  ```

### Issue: "Deployment failed"
- Check the logs in Streamlit Cloud dashboard
- Verify all dependencies are in `requirements.txt`
- Ensure `dashboard.py` is at the root or specify correct path

## Updating Your Dashboard

1. **Make changes locally**
2. **Commit and push:**
   ```bash
   git add .
   git commit -m "Update dashboard"
   git push
   ```
3. **Streamlit Cloud auto-updates** (usually within 1-2 minutes)

## File Structure for Streamlit Cloud

```
your-repo/
├── dashboard.py              # Main dashboard file (required)
├── commodity_code_mapping.py # Required dependency
├── requirements.txt          # Python dependencies (required)
├── .streamlit/
│   └── config.toml          # Streamlit config (optional)
├── data/
│   └── imports_2024_2025_cleaned.csv  # Data file
├── README.md                # Project docs (optional)
└── .gitignore              # Git ignore rules
```

## Next Steps After Deployment

1. **Test your dashboard** at the Streamlit Cloud URL
2. **Share the URL** with stakeholders
3. **Set up automatic updates** (already enabled - just push to GitHub)
4. **Monitor usage** in Streamlit Cloud dashboard

## Support

- Streamlit Cloud Docs: [docs.streamlit.io/streamlit-community-cloud](https://docs.streamlit.io/streamlit-community-cloud)
- Streamlit Forum: [discuss.streamlit.io](https://discuss.streamlit.io)

