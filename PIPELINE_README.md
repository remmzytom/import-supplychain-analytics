# Freight Import Data Pipeline

## Overview

This is an automated data pipeline for processing freight import data from the Australian Bureau of Statistics (ABS). The pipeline consists of four main steps:

1. **Data Extraction** - Downloads raw data from ABS website
2. **Data Cleaning** - Cleans and preprocesses the data
3. **Data Analysis** - Generates summary statistics and insights
4. **Dashboard Deployment** - Launches interactive Streamlit dashboard

## Pipeline Architecture

```
┌─────────────────┐
│  Data Source    │
│  (ABS Website)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Step 1:       │
│   Extraction    │
│  (pipeline_     │
│   extract.py)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Step 2:       │
│   Cleaning      │
│  (pipeline_     │
│   clean.py)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Step 3:       │
│   Analysis      │
│  (pipeline_     │
│   analyze.py)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Step 4:       │
│   Dashboard     │
│  (pipeline_     │
│   dashboard.py) │
└─────────────────┘
```

## File Structure

```
Freight_import_data_project/
├── pipeline_config.py          # Configuration settings
├── pipeline_extract.py         # Step 1: Data extraction (uses imports_extractor.py)
├── pipeline_clean.py           # Step 2: Data cleaning (uses import_data_cleaning.ipynb logic)
├── pipeline_analyze.py         # Step 3: Data analysis
├── pipeline_dashboard.py       # Step 4: Dashboard deployment (uses dashboard.py)
├── run_pipeline.py             # Main orchestrator
│
├── Existing Files (Used by Pipeline):
├── imports_extractor.py        # Original extraction module (used by pipeline_extract.py)
├── import_data_cleaning.ipynb  # Original cleaning notebook (logic used by pipeline_clean.py)
├── dashboard.py                 # Streamlit dashboard application (used by pipeline_dashboard.py)
├── commodity_code_mapping.py   # SITC industry mapping
│
├── data/                       # Data directory
│   ├── imports_2024_2025.csv          # Raw data
│   └── imports_2024_2025_cleaned.csv  # Cleaned data
├── output/                     # Analysis outputs
│   └── analysis/
│       ├── summary_statistics.json
│       └── analysis_report.txt
├── logs/                       # Log files
│   └── pipeline.log
└── requirements.txt           # Python dependencies
```

## Integration with Existing Files

The pipeline integrates with your existing code:

- **`pipeline_extract.py`** → Uses `imports_extractor.py` for data extraction
- **`pipeline_clean.py`** → Implements cleaning logic from `import_data_cleaning.ipynb`
- **`pipeline_dashboard.py`** → Launches `dashboard.py` for visualization

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Verify Installation

```bash
python -c "import pandas, streamlit, plotly; print('All dependencies installed!')"
```

## Usage

### Run Complete Pipeline

Run all steps in sequence:

```bash
python run_pipeline.py
```

### Run Individual Steps

Run only a specific step:

```bash
# Extract data only
python run_pipeline.py --step extract

# Clean data only
python run_pipeline.py --step clean

# Analyze data only
python run_pipeline.py --step analyze

# Launch dashboard only
python run_pipeline.py --step dashboard
```

### Skip Steps

Skip specific steps:

```bash
# Skip extraction (use existing raw data)
python run_pipeline.py --skip-extract

# Skip cleaning (use existing cleaned data)
python run_pipeline.py --skip-clean

# Skip analysis
python run_pipeline.py --skip-analyze

# Skip dashboard
python run_pipeline.py --skip-dashboard
```

### Continue on Errors

Continue pipeline execution even if a step fails:

```bash
python run_pipeline.py --continue-on-error
```

### Run Individual Modules

You can also run each module independently:

```bash
# Extract
python pipeline_extract.py

# Clean
python pipeline_clean.py

# Analyze
python pipeline_analyze.py

# Dashboard
python pipeline_dashboard.py
```

## Configuration

Edit `pipeline_config.py` to customize pipeline behavior:

### Data Extraction
- `EXTRACT_YEARS`: Years to extract (default: ['2024', '2025'])
- `EXTRACT_CHUNK_SIZE`: Chunk size for reading data (default: 10000)

### Data Cleaning
- `CLEAN_CHUNK_SIZE`: Chunk size for cleaning (default: 100000)
- `MIN_VALUE_FOB`: Minimum FOB value threshold (default: 0)
- `MIN_VALUE_CIF`: Minimum CIF value threshold (default: 0)
- `MIN_WEIGHT`: Minimum weight threshold (default: 0)

### Data Analysis
- `TOP_N_COUNTRIES`: Number of top countries to analyze (default: 15)
- `TOP_N_COMMODITIES`: Number of top commodities to analyze (default: 15)
- `TOP_N_PORTS`: Number of top ports to analyze (default: 10)

### Dashboard
- `DASHBOARD_PORT`: Port number (default: 8501)
- `DASHBOARD_HOST`: Host address (default: "localhost")

### Pipeline Execution
- `RUN_EXTRACTION`: Enable/disable extraction step (default: True)
- `RUN_CLEANING`: Enable/disable cleaning step (default: True)
- `RUN_ANALYSIS`: Enable/disable analysis step (default: True)
- `RUN_DASHBOARD`: Enable/disable dashboard step (default: True)
- `CONTINUE_ON_ERROR`: Continue on errors (default: False)

## Step-by-Step Guide

### Step 1: Data Extraction

**Purpose**: Download raw import data from ABS website

**What it does**:
- Uses `imports_extractor.py` module
- Downloads ZIP file from ABS website
- Extracts CSV data
- Filters by year (if specified)
- Saves to `data/imports_2024_2025.csv`

**Output**: Raw data file (~4.5M records)

**Time**: ~5-10 minutes (depending on internet speed)

**Uses**: `imports_extractor.py` - Your existing extraction module

### Step 2: Data Cleaning

**Purpose**: Clean and preprocess raw data

**What it does**:
- Implements cleaning logic from `import_data_cleaning.ipynb`
- Standardizes column names
- Converts numeric columns
- Handles missing values
- Removes negative values
- Cleans text fields
- Standardizes unit names
- Rounds Number quantities to integers
- Creates derived features (value per tonne, month_number, insurance_freight_cost)
- Removes duplicates

**Output**: Cleaned data file `data/imports_2024_2025_cleaned.csv`

**Time**: ~10-15 minutes

**Uses**: Logic from `import_data_cleaning.ipynb` - Your existing cleaning notebook

### Step 3: Data Analysis

**Purpose**: Generate summary statistics and insights

**What it does**:
- Calculates total values (FOB, CIF, weight, quantity)
- Identifies top countries, commodities, and ports
- Analyzes transport mode distribution
- Analyzes state distribution
- Generates summary report

**Output**: 
- `output/analysis/summary_statistics.json`
- `output/analysis/analysis_report.txt`

**Time**: ~5 minutes

### Step 4: Dashboard Deployment

**Purpose**: Launch interactive Streamlit dashboard

**What it does**:
- Checks requirements
- Launches `dashboard.py` using Streamlit
- Opens dashboard in browser

**Output**: Interactive web dashboard at `http://localhost:8501`

**Time**: Dashboard runs until stopped (Ctrl+C)

**Uses**: `dashboard.py` - Your existing Streamlit dashboard application

## Dashboard Features

The dashboard provides:

1. **Overview**: Key metrics and summary charts
2. **Time Series Analysis**: Monthly trends and year-over-year comparisons
3. **Geographic Analysis**: Countries, ports, states, and port-country matrix
4. **Commodity Analysis**: Top commodities by value and weight
5. **Value vs Volume Analysis**: Market leaders vs premium products
6. **Risk Analysis**: Country concentration, commodity dependence
7. **Transport Mode Analysis**: Distribution by transport mode
8. **Key Insights**: Summary of findings

## Troubleshooting

### Common Issues

#### 1. Data Extraction Fails

**Problem**: Cannot download data from ABS website

**Solutions**:
- Check internet connection
- Verify ABS website is accessible
- Check if URL in `pipeline_config.py` is still valid
- Try running extraction again

#### 2. Memory Errors

**Problem**: Out of memory during processing

**Solutions**:
- Reduce chunk sizes in `pipeline_config.py`
- Process data in smaller batches
- Close other applications
- Use a machine with more RAM

#### 3. Dashboard Won't Start

**Problem**: Dashboard fails to launch

**Solutions**:
- Check if cleaned data file exists
- Verify Streamlit is installed: `pip install streamlit`
- Check if port 8501 is available
- Review logs in `logs/pipeline.log`

#### 4. Missing Dependencies

**Problem**: Import errors

**Solutions**:
- Install requirements: `pip install -r requirements.txt`
- Verify installation: `python -c "import pandas, streamlit, plotly"`

### Log Files

Check `logs/pipeline.log` for detailed error messages and execution logs.

## Automation

### Schedule Pipeline Execution

#### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily, weekly, etc.)
4. Set action: `python C:\path\to\run_pipeline.py`

#### Linux Cron

Add to crontab (`crontab -e`):

```bash
# Run pipeline daily at 2 AM
0 2 * * * cd /path/to/project && python run_pipeline.py --skip-extract --skip-dashboard
```

#### Python Script

```python
import schedule
import time
from run_pipeline import run_pipeline

def job():
    run_pipeline(run_dash=False)  # Don't launch dashboard in automation

schedule.every().day.at("02:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
```

## Best Practices

1. **Run extraction periodically** to get latest data
2. **Keep cleaned data** - only re-clean when raw data changes
3. **Review analysis outputs** before sharing with stakeholders
4. **Monitor logs** for errors and warnings
5. **Backup data files** regularly
6. **Test pipeline** after configuration changes

## Performance Tips

1. **Use chunked processing** for large datasets
2. **Skip unnecessary steps** if data hasn't changed
3. **Run analysis separately** from dashboard for faster iteration
4. **Use SSD storage** for faster I/O
5. **Increase chunk sizes** if you have more RAM

## Support

For issues or questions:
1. Check `logs/pipeline.log` for error details
2. Review configuration in `pipeline_config.py`
3. Verify data files exist in `data/` directory
4. Check requirements are installed correctly

## Next Steps

After running the pipeline:

1. **Review Analysis Report**: Check `output/analysis/analysis_report.txt`
2. **Explore Dashboard**: Navigate through different sections
3. **Customize Visualizations**: Modify `dashboard.py` as needed
4. **Schedule Automation**: Set up regular pipeline execution
5. **Share Insights**: Export dashboard visualizations for stakeholders

---

**Last Updated**: 2024
**Pipeline Version**: 1.0

