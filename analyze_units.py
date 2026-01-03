"""
Quick script to analyze unit_quantity and quantity patterns in the imports data
"""
import pandas as pd
import numpy as np

# Load data
print("Loading imports data...")
df = pd.read_csv('data/imports_2024_2025.csv')

print(f"\nTotal records: {len(df):,}")
print(f"\n{'='*60}")
print("1. UNIT_QUANTITY ANALYSIS")
print(f"{'='*60}")

# Check unique unit_quantity values
print("\nUnique unit_quantity values and their counts:")
unit_counts = df['unit_quantity'].value_counts()
print(unit_counts)

print(f"\n{'='*60}")
print("2. QUANTITY STATISTICS BY UNIT TYPE")
print(f"{'='*60}")

# Analyze quantity patterns for each unit type
for unit in df['unit_quantity'].unique():
    unit_data = df[df['unit_quantity'] == unit]
    quantities = pd.to_numeric(unit_data['quantity'], errors='coerce')
    
    print(f"\n{unit}:")
    print(f"  Count: {len(unit_data):,} records")
    print(f"  Min: {quantities.min():,.2f}")
    print(f"  Max: {quantities.max():,.2f}")
    print(f"  Mean: {quantities.mean():,.2f}")
    print(f"  Median: {quantities.median():,.2f}")
    
    # Check for decimals in "Number" type
    if 'Number' in unit or 'number' in unit.lower():
        has_decimals = (quantities % 1 != 0).sum()
        print(f"  Records with decimals: {has_decimals:,} ({(has_decimals/len(unit_data))*100:.2f}%)")
        
        # Show examples of decimal values
        decimal_examples = quantities[quantities % 1 != 0].head(5)
        if len(decimal_examples) > 0:
            print(f"  Example decimal values: {decimal_examples.tolist()}")

print(f"\n{'='*60}")
print("3. WEIGHT ANALYSIS")
print(f"{'='*60}")

# Analyze weight column
weights = pd.to_numeric(df['weight'], errors='coerce')
print(f"\nWeight statistics:")
print(f"  Min: {weights.min():,.2f}")
print(f"  Max: {weights.max():,.2f}")
print(f"  Mean: {weights.mean():,.2f}")
print(f"  Median: {weights.median():,.2f}")

# Check if weights seem to be in tonnes or kg
print(f"\n  Records with weight < 1: {(weights < 1).sum():,} ({(weights < 1).sum()/len(df)*100:.2f}%)")
print(f"  Records with weight < 0.1: {(weights < 0.1).sum():,} ({(weights < 0.1).sum()/len(df)*100:.2f}%)")
print(f"  Records with weight > 1000: {(weights > 1000).sum():,} ({(weights > 1000).sum()/len(df)*100:.2f}%)")

print(f"\n{'='*60}")
print("4. POTENTIAL ISSUES")
print(f"{'='*60}")

# Check for suspicious patterns
issues = []

# Issue 1: Number units with decimals
number_units = df[df['unit_quantity'].str.contains('Number', case=False, na=False)]
if len(number_units) > 0:
    num_quantities = pd.to_numeric(number_units['quantity'], errors='coerce')
    decimal_count = (num_quantities % 1 != 0).sum()
    if decimal_count > 0:
        issues.append(f"⚠️  {decimal_count:,} 'Number' records have decimal quantities")

# Issue 2: Very small weight values (might be in wrong unit)
small_weights = (weights < 0.001).sum()
if small_weights > 0:
    issues.append(f"⚠️  {small_weights:,} records have weight < 0.001 (might be in kg instead of tonnes)")

# Issue 3: Inconsistent unit naming
unit_variations = {}
for unit in df['unit_quantity'].unique():
    base = unit.lower().strip()
    if base not in unit_variations:
        unit_variations[base] = []
    unit_variations[base].append(unit)

inconsistent = {k: v for k, v in unit_variations.items() if len(v) > 1}
if inconsistent:
    issues.append(f"⚠️  Found {len(inconsistent)} unit types with naming variations:")
    for base, variations in list(inconsistent.items())[:5]:
        issues.append(f"     '{base}': {variations}")

# Issue 4: Zero or negative quantities
zero_quantities = (pd.to_numeric(df['quantity'], errors='coerce') <= 0).sum()
if zero_quantities > 0:
    issues.append(f"⚠️  {zero_quantities:,} records have zero or negative quantities")

if issues:
    for issue in issues:
        print(f"\n{issue}")
else:
    print("\n✅ No obvious issues detected!")

print(f"\n{'='*60}")
print("5. SAMPLE RECORDS BY UNIT TYPE")
print(f"{'='*60}")

# Show sample records for each unit type
for unit in df['unit_quantity'].value_counts().head(5).index:
    print(f"\nSample records for '{unit}':")
    samples = df[df['unit_quantity'] == unit][['unit_quantity', 'quantity', 'weight', 'valuefob', 'commodity_description']].head(3)
    print(samples.to_string(index=False))



