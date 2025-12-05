import pandas as pd
import re
from datetime import datetime
import logging
import duckdb
import os

BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{BASE_PATH}/logs/etl_processing.log'),
        logging.StreamHandler()
    ]
)

class ETLProcessor:
    def __init__(self, asset_data_path, master_data_path):
        self.asset_data_path = asset_data_path
        self.master_data_path = master_data_path
        self.invalid_records = []  # List to store invalid records
        self.invalid_city_records = []  # List to store invalid city records
        
    def handle_errors(self):
        logging.info("Handling errors and invalid records...")
        
        # Log records without city match
        if self.invalid_city_records:
            pd.DataFrame(self.invalid_city_records).to_csv(
                f'{BASE_PATH}/logs/invalid_city_records_log.csv', index=False
            )
            logging.info(f"Invalid city records saved: {len(self.invalid_city_records)}")
        
        # Save all invalid records
        if self.invalid_records:
            pd.DataFrame(self.invalid_records).to_csv(
                f'{BASE_PATH}/logs/invalid_records_log.csv', index=False
            )
            logging.info(f"Invalid records saved: {len(self.invalid_records)}")
        
        return self
        
    def load_data(self):
        logging.info("Loading data files...")
        
        self.df_asset = pd.read_excel(self.asset_data_path)
        self.df_master = pd.read_excel(self.master_data_path)
        
        logging.info(f"Asset data loaded: {len(self.df_asset)} records")
        logging.info(f"City master data loaded: {len(self.df_master)} records")
        
        return self
    
    def create_flagging_master(self, value):
        if value is None:
            return None, None
        
        value = str(value).strip().upper()
        
        # special case for Administrative Area
        if 'ADM.' in value:
            value = value.replace('ADM.', '').strip()
        
        # Remove common prefixes
        prefixes = ['KABUPATEN ', 'KOTA ']
        for prefix in prefixes:
            if value.startswith(prefix):
                value = value.replace(prefix, '').strip()
        
        # split if has short name     
        if "(" in value:
            split_value = value.split('(')
            value = split_value[0]
            value2 = split_value[1]
            
            # Remove special characters
            value = re.sub(r'[^\w\s]', '', value)
            value2 = re.sub(r'[^\w\s]', '', value2)
            
            # Remove extra spaces
            value = ''.join(value.split())
            value2 = ''.join(value2.split())
            
            return value, value2
        
        # Remove special characters
        value = re.sub(r'[^\w\s]', '', value)
        
        # Remove extra spaces
        value = ''.join(value.split())
        
        return value, None
    
    def create_flagging_asset(self, value):

        if value is None:
            return None
        
        value = str(value).strip().upper()
        
        value = value.replace(', KOTA', '').strip()
        
        # Remove common prefixes
        prefixes = ['KABUPATEN ', 'KAB ', 'KAB. ']
        for prefix in prefixes:
            if value.startswith(prefix):
                value = value.replace(prefix, '').strip()
        
        # Remove special characters
        value = re.sub(r'[^\w\s]', '', value)
        
        # Remove extra spaces
        value = ''.join(value.split())
        
        return value
    
    def combine_data(self):
        logging.info("Combining data...")
        
        query = """
            SELECT 
                *
            FROM df_asset dr
            LEFT JOIN (
                SELECT 
                    *,
                    flagging1 AS flagging_master
                FROM df_master
                UNION ALL 
                SELECT 
                    *,
                    flagging2 AS flagging_master
                FROM df_master 
                WHERE flagging2 IS NOT NULL
            ) dm ON dr.flagging = dm.flagging_master
            """
            
        # Register the dataframes in DuckDB
        duckdb.register("df_asset", self.df_asset)
        duckdb.register("df_master", self.df_master)
            
        self.combined_df = duckdb.query(query).to_df()
                
        logging.info("Data combined")
        return self
    
    def create_internal_site_id(self):
        
        # Remove null values in critical fields
        clean_combined_df = self.combined_df[self.combined_df['CityCode'].notna()]
        
        # Group by CityCode and sort by Funcloc
        df_sorted = clean_combined_df.sort_values(['CityCode', 'Funcloc']).copy()
        
        # Create sequence number per city
        df_sorted['CitySequence'] = df_sorted.groupby('CityCode').cumcount() + 1
        
        # Create Internal Site ID
        df_sorted['Internal_Site_ID'] = (
            df_sorted['CityCode'].astype(str) + '-' + 
            df_sorted['RegionalCode'].astype(str).str.zfill(2) + '-' + 
            df_sorted['CitySequence'].astype(str).str.zfill(3)
        )
        
        return df_sorted
    
    def validate_data_asset(self):
        logging.info("Starting data asset validation...")
        
        # Check required fields
        required_fields = ['Funcloc', 'Alamat1', 'Alamat2', 'Alamat3', 'Alamat4', 'SiteName']
        for field in required_fields:
            missing = self.df_asset[field].isna().sum()
            if missing > 0:
                logging.warning(f"Missing values in {field}: {missing} records")
        
        # Validate Funcloc format (should be numeric)
        invalid_funcloc = self.df_asset[~self.df_asset['Funcloc'].apply(
            lambda x: str(x).isdigit() if pd.notna(x) else False
        )]
        
        if invalid_funcloc.shape[0] > 0:
            logging.warning(f"Invalid Funcloc format: {invalid_funcloc.shape[0]} records")
            self.invalid_records.extend(invalid_funcloc.to_dict('records'))
            
        # Validate numeric ranges for Funcloc (12 digits)
        self.df_asset['Funcloc_Valid'] = self.df_asset['Funcloc'].apply(
            lambda x: 100000000000 <= int(x) <= 999999999999 if str(x).isdigit() else False
        )
        
        invalid_funcloc_range = self.df_asset[~self.df_asset['Funcloc_Valid']]
        if invalid_funcloc_range.shape[0] > 0:
            logging.warning(f"Funcloc out of acceptable range: {invalid_funcloc_range.shape[0]} records")
            self.invalid_records.extend(invalid_funcloc_range.to_dict('records'))
        
        return self
    
    def clean_data(self):
        logging.info("Starting data cleaning...")
        
        # Remove null values in critical fields
        self.df_asset = self.df_asset.dropna(subset=['Funcloc', 'Alamat4'])
        
        # Standardize text fields
        text_columns = ['Alamat1', 'Alamat2', 'Alamat3', 'Alamat4', 'SiteName']
        for col in text_columns:
            if col in self.df_asset.columns:
                self.df_asset[col] = self.df_asset[col].apply(
                    lambda x: str(x).strip().upper() if pd.notna(x) else None
                )
        
        # Create Flagging
        self.df_asset['flagging'] = self.df_asset['Alamat4'].apply(
            self.create_flagging_asset
        )
        
        self.df_master[['flagging1', 'flagging2']] = self.df_master['City'].apply(
            lambda x: pd.Series(self.create_flagging_master(x))
        )
        
        # Combine data
        self.combine_data()
        
        # Count missing CityCode
        missing_citycode = self.combined_df[self.combined_df['CityCode'].isna()]
        if missing_citycode.shape[0] > 0:
            logging.warning(f"Missing CityCode: {missing_citycode.shape[0]} records")
            self.invalid_city_records.extend(missing_citycode.to_dict('records'))
        
        logging.info("Data cleaning completed")
        return self
    
    def transform_data(self):
        logging.info("Starting data transformation...")

        # Create Internal Site ID
        self.final_df = self.create_internal_site_id()
        
        self.final_df['Kabupaten/Kota'] = self.final_df['City']
        
        logging.info("Data transformation completed")
        return self
    
    def save_results(self):
        logging.info("Saving results...")
        
        # Reorder columns as per expected output
        output_columns = [
            'Internal_Site_ID', 'Kabupaten/Kota', 'CityCode', 'RegionalCode', 
            'Alamat1', 'Alamat2', 'Alamat3', 'Alamat4',
            'SiteName', 'Funcloc'
        ]
        
        self.final_df = self.final_df[output_columns].copy()
        
        # Save to CSV
        self.final_df.to_csv(f'{BASE_PATH}/data/output_data_asset_transformed.csv', index=False, encoding='utf-8-sig')
        logging.info(f"Results saved to {BASE_PATH}/data/output_data_asset_transformed.csv")
        
        # Print summary statistics
        self.print_summary()
        
        return self
    
    def print_summary(self):
        """Print processing summary"""
        total_records = self.combined_df.shape[0]
        valid_city = self.final_df.shape[0]
        invalid_city = total_records - valid_city
        
        print("\n" + "="*60)
        print("ETL PROCESSING SUMMARY")
        print("="*60)
        print(f"Total records processed: {total_records}")
        print(f"Records with valid city match: {valid_city}")
        print(f"Records without city match: {invalid_city}")
        print(f"Success rate: {(valid_city/total_records)*100:.2f}%")
        print("="*60)
        
        # Show sample of results
        print("\nSample of transformed data (first 5 records):")
        print(self.final_df[['Internal_Site_ID', 'Kabupaten/Kota', 'Funcloc']].head())
    
    def run_pipeline(self):
        """Run complete ETL pipeline"""
        logging.info("="*60)
        logging.info("STARTING ETL PIPELINE")
        logging.info("="*60)
        
        try:
            (self.load_data()
                 .validate_data_asset()
                 .clean_data()
                 .transform_data()
                 .handle_errors()
                 .save_results())
            
            logging.info("="*60)
            logging.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logging.info("="*60)
            
        except Exception as e:
            logging.error(f"Pipeline failed with error: {str(e)}")
            raise
        
# Main execution
if __name__ == "__main__":
    # Initialize processor
    processor = ETLProcessor(
        asset_data_path=f'{BASE_PATH}/data/Assessment Data Asset Dummy.xlsx',
        master_data_path=f'{BASE_PATH}/data/City Indonesia.xlsx'
    )
    
    # Run pipeline
    processor.run_pipeline()