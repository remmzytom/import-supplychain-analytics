# Freight Import Data Dashboard

## Overview
This Streamlit dashboard provides an interactive and dynamic visualization of all analyses from the `import_data_analysis.ipynb` notebook. It allows stakeholders to explore freight import data with filters, interactive charts, and comprehensive insights.

## Features

### 📊 Sections Available:

1. **Overview** - Key metrics and quick summary charts
2. **Time Series Analysis** - Monthly trends and year-over-year comparisons
3. **Geographic Analysis** - Countries, ports (Australian & origin), states, and port-country matrix
4. **Commodity Analysis** - Top commodities by value/weight, CIF vs FOB comparison, SITC industry analysis
5. **Value vs Volume Analysis** - Market leaders vs premium products quadrant analysis
6. **Risk Analysis** - Country concentration risk, commodity dependence index, supplier trend analysis
7. **Transport Mode Analysis** - Analysis by transport mode (Air, Sea, Post)
8. **Key Insights** - Summary of important findings and statistics

### 🔍 Interactive Features:

- **Filters**: Filter by year, month, and top countries
- **Dynamic Charts**: All visualizations are interactive using Plotly
- **Responsive Design**: Wide layout optimized for stakeholder presentations
- **Real-time Updates**: Charts update based on selected filters

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Ensure the data file exists:
   - `data/imports_2024_2025_cleaned.csv`

3. Ensure `commodity_code_mapping.py` is in the same directory as `dashboard.py`

## Running the Dashboard

1. Navigate to the project directory:
```bash
cd Freight_import_data_project
```

2. Run Streamlit:
```bash
streamlit run dashboard.py
```

3. The dashboard will open in your default web browser at `http://localhost:8501`

## Usage

### Navigation
- Use the sidebar to navigate between different analysis sections
- Select filters to focus on specific time periods, months, or countries

### Sections Guide

#### Overview
- Quick view of key metrics (total records, FOB/CIF values, weight)
- Top 5 countries and commodities summary charts

#### Time Series Analysis
- Monthly trends for FOB, CIF, weight, and quantity
- Year-over-year comparison with growth rates

#### Geographic Analysis
- Top countries by value and weight (adjustable slider)
- Australian ports and origin ports analysis
- State-wise import distribution
- Port-country matrix heatmap

#### Commodity Analysis
- Top commodities by value and weight (adjustable slider)
- CIF vs FOB comparison for top commodities
- SITC-based industry analysis

#### Value vs Volume Analysis
- Quadrant visualization: Market Leaders vs Premium Products
- Market leaders by total value
- Premium products by value per tonne

#### Risk Analysis
- Country concentration risk (HHI index)
- Commodity dependence index
- Supplier trend analysis (2024 vs 2025)

#### Transport Mode Analysis
- Value distribution by transport mode
- Weight distribution by transport mode
- Mode comparison charts

#### Key Insights
- Top statistics summary
- Year-over-year growth metrics
- Market concentration indicators

## Data Requirements

- **Data File**: `data/imports_2024_2025_cleaned.csv`
- **Expected Columns**: 
  - month, mode, mode_description, commodity_code, commodity_description
  - ausport_code, ausport_description, osport_code, osport_description
  - state, country_code, country_description
  - weight, valuefob, valuecif, unit_quantity, quantity
  - year, month_number, value_per_tonne_fob, value_per_tonne_cif

## Performance Notes

- Initial data loading may take a few minutes for large datasets (~4.5M rows)
- Data is cached after first load for faster subsequent access
- Charts are rendered dynamically and may take a moment to update with filters

## Troubleshooting

### Data Not Loading
- Ensure `data/imports_2024_2025_cleaned.csv` exists in the `data/` directory
- Check file permissions

### Import Errors
- Ensure `commodity_code_mapping.py` is in the same directory as `dashboard.py`
- Verify all required packages are installed: `pip install -r requirements.txt`

### Memory Issues
- The dashboard uses chunked reading for large datasets
- If memory issues occur, consider reducing the dataset size or increasing system memory

## Customization

### Adding New Sections
1. Create a new function following the pattern `show_section_name(df)`
2. Add the section to the `sections` list in the sidebar
3. Add the corresponding `elif` clause in the main function

### Modifying Charts
- All charts use Plotly Express or Plotly Graph Objects
- Modify chart parameters in the respective section functions
- Refer to Plotly documentation for advanced customization

## Support

For issues or questions, refer to the main analysis notebook `import_data_analysis.ipynb` for detailed explanations of each analysis.

