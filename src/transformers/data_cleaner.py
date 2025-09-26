import pandas as pd
import json
import os
from datetime import datetime

class DataCleaner:
    # Transforms raw BLS data into clean data

    def __init__(self):
        self.cleaned_data = pd.DataFrame()
        print("Data Cleaner initialized")

    def load_raw_data(self, filepath=None, raw_data_dict=None):

        if filepath:
            print(f"Loading raw data from {filepath}...")
            with open(filepath, 'r') as f:
                data = json.load(f)
            print(f" Loaded data from file")
            return data
        elif raw_data_dict:
            print("Using data passed directly from extractor")
            return raw_data_dict
        else:
            print("No data source provided")
            return None
    
    def parse_bls_data(self, raw_data, sector_name):
        # Convert BLS JSON format to pandas dataframe

        # Create a list to store processed records
        records = []

        print(f" Processing {sector_name}...")

        # Process each data point
        for data_point in raw_data:
            # Extract values
            year = int(data_point['year'])
            period = data_point['period']
            value = float(data_point['value'])

            # Parse period code (M01 = Jan...)
            # M13 is annual average so we skip
            if period.startswith('M') and period != 'M13':
                month = int(period[1:])
            else:
                continue

            date = datetime(year, month, 1)

            record = {
                'sector': sector_name.replace('_', ' '),
                'date': date,
                'year': year,
                'month': month,
                'month_name': date.strftime('%B'),
                'employment_thousands': value,
                'employment_millions': round(value/1000,2),
                'period_code': period # keep original code for reference
            }

            records.append(record)

        if records:
            df = pd.DataFrame(records)
            print(f" Processed {len(df)} months of data")
            return df
        else:
            print(f" No valid data points found")
            return pd.DataFrame()
        
    def calcualte_metrics(self,df):
        # Add calculated fields for analysis

        if df.empty:
            return df
        
        print("\nCalculating growth metrics...")

        # Sort by sector and date for proper calculations
        df = df.sort_values(['sector', 'date'])

        # Calculate metrics for each sector separately
        for sector in df['sector'].unique():
            mask = df['sector'] == sector
            sector_data = df[mask].copy

            print(f" Calculating metrics for {sector}...")

            # Month over month change
            df.loc[mask, 'mom_change'] = sector_data['employment_thousands'].diff()

            # MoM percent change
            df.loc[mask, 'mom_percent'] = (
                sector_data['employment_thousands'].diff()/
                sector_data['employment_thousands'].shift(1)*100
            )

            # YoY change (compare to same month last year)
            df.loc[mask, 'yoy_change'] = sector_data['employment_thousands'].diff(12)

            # YoY percent change
            df.loc[mask, 'yoy_percent'] = (
                sector_data['employment_thousands'].diff(12)/
                sector_data['employment_thousands'].shift(12)*100
            )

            # 3 month moving average
            df.loc[mask, 'ma_3month'] = (
                sector_data['employment_thousands'].rolling(window=3, min_periods=1).mean()
            )

            # 12 month moving average
            df.loc[mask, 'ma_3month'] = (
                sector_data['employment_thousands'].rolling(window=12, min_periods=12).mean()
            )

        # Add growth status categories
        df['growth_status'] = pd.cut(
            df['yoy_percent'],
            bins=[-float('inf'), -2, 0, 2, 5, float('inf')],
            labels=['Declining', 'Slight Decline', 'Stable', 'Growing', 'Rapid Growth']
        )

        # Add seasonal indicators
        df['quarter'] = df['date'].dt.quarter
        df['is_year_end'] = df['month'].isnin([11,12,1]) # Holiday season
        df['is_summer'] = df['month'].isin([6,7,8])

        # Round numeric columns
        numeric_cols = [
            'employment_thousands', 'employment millions', 'mom_change', 'mom_percent',
            'yoy_change', 'yoy_percent', 'ma_3month', 'ma_12month'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].round(2)

        print(" Metrics calculated successfully")

    def process_all_sectors(self, all_raw_data):
        # Process data into single clean dataset

        print("\n" + "="*50)
        print("CLEANING AND TRANSFORMING DATA")
        print("="*50 + "\n")

        all_dfs = []

        # Process each sector's data
        for sector_name, raw_data in all_raw_data.items():
            df = self.parse_bls_data(raw_data, sector_name)
            if not df.empty:
                all_dfs.append(df)

        if not df.empty:
            print(" No data to process")
            return pd.DataFrame()
        
        # Combine all sectors into one big table
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Add calculated metrics
        combined_df = self.calcualte_metrics(combined_df)

        # Store for later use
        self.cleaned_data = combined_df

        # Print summary statistics
        self.print_summary(combined_df)

        return combined_df
    
    def save_cleaned_data(self, df, format='csv'):
        # Save cleaned data to file

        if df.empty:
            print("No data to save")
            return
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if format == 'csv':
            filename = f"data/cleaned_employment_{timestamp}.csv"
            df.to_csv(filename, index=False)
            print(f"\nSaved cleaned data to: {filename}")
        elif format == 'excel':
            filename = f"data/cleaned_employment_{timestamp}.xlsx"

            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Employment_Data', index=False)

            print(f"\nSaved Excel file to: {filename}")

        # Show sample of the cleaned data
        print(f"\nSample of cleaned data:")
        print(df[['sector', 'date', 'employment_thousands', 'yoy_percent', 'growth_status']].head())

        return filename
    
    def validate_data(self, df):
        # Data quality check

        print("\nRunning data validation checks...")

        issues = []

        # Check missing values
        missing = df.isnull().sum()
        if missing.any():
            issues.append(f"Missing values found: {missing[missing >0].to_dict()}")
        
        # Check for duplicates
        deplicates = df.duplicated(subset=['sector', 'date']).sum()

        # Check for unrealistic values
        if(df['employment_thousands']<0).any():
            issues.append("Negative employment values found")

        if(df['employment_thousands']>1000000).any():
            issues.append("Unrealistically high employment rate")

        # Date continuity
        for sector in df['sector'].unique():
            sector_data = df[df['sector'] == sector].sort_values('date')
            date_diffs = sector_data['date'].diff()

            # Check if differences between dates are roughly one month
            if date_diffs.days.max() > 35:
                issues.append(f"Date gaps found in {sector} data")

        # Report issues
        if issues:
            print("Data quality issues found:")
            for issue in issues:
                print(f" * {issue}")
        else:
            print(" All validation checks passed")

        return len(issues) == 0
    
if __name__ == "__main__":
    print("="*60)
    print("DATA CLEANER TEST")
    print("="*60)

    # Initialize the cleaner
    cleaner = DataCleaner()

    # Process the sample data
    cleaned_df = cleaner.process_all_sectors()

    # Validate the data
    cleaner.validate_data(cleaned_df)

    # Save the cleaned data
    cleaner.save_cleaned_data(cleaned_df, format='csv')

    print("\nData cleaning complete")