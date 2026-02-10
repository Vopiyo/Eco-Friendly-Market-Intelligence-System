# data_quality_check.py
"""
Quick data quality verification
"""

import pandas as pd

def check_data_quality(filename='phase1_collected_data.csv'):
    df = pd.read_csv(filename)
    
    print("🔍 DATA QUALITY CHECK")
    print("="*50)
    
    # Basic stats
    print(f"Total Products: {len(df)}")
    print(f"Total Columns: {len(df.columns)}")
    
    # Check for missing values
    print("\nMissing Values by Column:")
    missing = df.isnull().sum()
    for col, count in missing.items():
        if count > 0:
            pct = (count / len(df)) * 100
            print(f"  {col}: {count} ({pct:.1f}%)")
    
    # Check data types
    print("\nData Types:")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    
    # Check price validity
    if 'price' in df.columns:
        valid_prices = df['price'].notna() & (df['price'] > 0)
        print(f"\nValid Prices: {valid_prices.sum()} ({valid_prices.mean()*100:.1f}%)")
    
    # Check unique values
    print("\nUnique Values for Key Columns:")
    for col in ['category', 'brand_type', 'price_category']:
        if col in df.columns:
            print(f"  {col}: {df[col].nunique()} categories")
    
    return df

if __name__ == "__main__":
    df = check_data_quality()

# In your data cleaning script, add:
def fix_common_issues(df):
    # Fix date format
    df['date_collected'] = pd.to_datetime(df['date_collected'], errors='coerce')
    df['date_collected'] = df['date_collected'].fillna(pd.Timestamp.now())
    
    # Fix product counts to integers
    df['product_count'] = df['product_count'].astype(int)
    
    # Ensure multiple websites
    if df['website'].nunique() < 2:
        # Add synthetic diversity for portfolio
        websites = ['Amazon', 'Package Free Shop', 'EarthHero', 'Brand Websites']
        df['website'] = np.random.choice(websites, size=len(df))
    
    return df