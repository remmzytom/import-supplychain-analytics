# Import Supply Chain Analytics Dashboard

Interactive Streamlit dashboard for Australian freight import supply chain analytics. Features automated data extraction, cleaning pipeline, geographic analysis, commodity insights, risk assessment, and transport mode visualization.

## 🚀 Quick Start

### Local Development
1. Install dependencies: `pip install -r requirements.txt`
2. Run data pipeline: `python run_pipeline.py`
3. Launch dashboard: `streamlit run dashboard.py`

### Streamlit Cloud Deployment
See [STREAMLIT_CLOUD_DEPLOY.md](STREAMLIT_CLOUD_DEPLOY.md) for deployment instructions.

**⚠️ Note**: Data files are excluded from this repository (too large for GitHub). See [DATA_SETUP.md](DATA_SETUP.md) for data file setup options.

## 📊 Features

- **Automated Data Pipeline**: Extraction → Cleaning → Analysis → Dashboard
- **Interactive Visualizations**: Geographic, commodity, risk, and transport mode analysis
- **Supply Chain Insights**: Country concentration, commodity dependence, supplier trends
- **Real-time Analysis**: Dynamic filters and interactive charts

---

## Original: ABS Imports Data Extractor

This project extracts imports data from the Australian Bureau of Statistics (ABS) international trade website.

## Features

- Downloads imports data from ABS public catalogue
- Processes large datasets efficiently using chunked reading
- Supports filtering by specific years
- Saves extracted data to CSV format
- Provides detailed analysis and summary statistics

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Extract 2024-2025 data (default):
```bash
python imports_extractor.py
```

### Extract all imports data:
```bash
python imports_extractor.py --all
```

### Extract data for custom years:
```bash
python imports_extractor.py 2023,2024
```

## Output

The script will:
- Download the imports CSV zip file from ABS
- Extract and process the data (default: 2024-2025 only)
- Save the results to `data/imports_2024_2025.csv` (or `data/imports_all.csv` if using --all)
- Display summary statistics and analysis

## Data Source

URL: https://aueprod01ckanstg.blob.core.windows.net/public-catalogue/public/82d5fb9d-61ae-4ddd-873b-5c9501b6b743/imports.csv.zip

## Notes

- The script uses streaming downloads to handle large files efficiently
- Data is processed in chunks to minimize memory usage
- Temporary ZIP files are automatically cleaned up after processing

