# Streamlit Cloud Deployment Checklist

## ✅ Pre-Deployment Checklist

### Files Required
- [x] `dashboard.py` - Main dashboard file
- [x] `commodity_code_mapping.py` - Required dependency
- [x] `requirements.txt` - All Python dependencies
- [x] `data/imports_2024_2025_cleaned.csv` - Data file
- [x] `.streamlit/config.toml` - Streamlit configuration
- [x] `.gitignore` - Git ignore rules

### Verification Steps

1. **Check data file exists:**
   ```bash
   ls data/imports_2024_2025_cleaned.csv
   ```

2. **Check data file size:**
   - If > 100MB, consider Git LFS or external storage
   - Current file should be manageable

3. **Test dashboard locally:**
   ```bash
   streamlit run dashboard.py
   ```
   - Should load without errors
   - All visualizations should work

4. **Verify requirements.txt:**
   - Contains: pandas, streamlit, plotly, numpy, requests, pyarrow
   - All dependencies are listed

## 🚀 Deployment Steps

### 1. Initialize Git (if not done)
```bash
git init
git add .
git commit -m "Initial commit for Streamlit Cloud"
```

### 2. Create GitHub Repository
- Go to github.com
- Create new repository
- Don't initialize with README

### 3. Push to GitHub
```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git branch -M main
git push -u origin main
```

### 4. Deploy to Streamlit Cloud
- Go to share.streamlit.io
- Sign in with GitHub
- Click "New app"
- Select repository
- Main file: `dashboard.py`
- Deploy!

## 📋 Post-Deployment

- [ ] Test dashboard at Streamlit Cloud URL
- [ ] Verify all visualizations load
- [ ] Check data loads correctly
- [ ] Share URL with stakeholders

## 🔧 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| Data file not found | Ensure `data/` folder is in repo |
| Module not found | Add to `requirements.txt` |
| File too large | Use Git LFS or external storage |
| Deployment fails | Check logs in Streamlit Cloud |

## 📝 Notes

- Streamlit Cloud auto-updates on git push
- Free tier has resource limits
- Data file should be < 1GB ideally
- Check deployment logs if issues occur

