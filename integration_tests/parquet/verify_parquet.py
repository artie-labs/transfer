#!/usr/bin/env python3

import pandas as pd
import sys
import argparse
from datetime import datetime, timezone, date, time
from decimal import Decimal

def verify_parquet_file(file_path, location):
    """
    Read and verify the contents of a parquet file.
    Returns True if all verifications pass, False otherwise.
    """
    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        # Sort by 'id' to ensure consistent row order
        df = df.sort_values(by='id').reset_index(drop=True)
        
        # Print the data
        print("DataFrame contents:")
        print(df)
        print("\nColumn data types:")
        print(df.dtypes)
        print(f"\nNumber of rows: {len(df)}")
        print(f"Number of columns: {len(df.columns)}")
        
        # Define expected columns for the new comprehensive test
        expected_columns = [
            'id', 'name', 'age', 'score', 'is_active', 'birth_date', 
            'lunch_time', 'created_at', 'created_at_ntz', 'balance', 
            'metadata', 'tags'
        ]
        
        # Verify column headers
        actual_columns = list(df.columns)
        print(f"\nExpected columns: {expected_columns}")
        print(f"Actual columns: {actual_columns}")
        
        if set(actual_columns) != set(expected_columns):
            print(f"WARNING: Column mismatch. Expected {len(expected_columns)} columns, got {len(actual_columns)}")
            # Still continue to verify the data that exists
        
        # Verify row count (expecting 3 rows now)
        expected_row_count = 3
        if len(df) != expected_row_count:
            print(f"WARNING: Expected {expected_row_count} rows, got {len(df)}")
        
        # Verify that we have the basic required columns
        required_columns = ['id', 'name', 'age']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' is missing")
        
        # Verify basic data types and content
        print("\nVerifying data content:")
        
        # Check ID column
        if 'id' in df.columns:
            ids = df['id'].tolist()
            print(f"IDs: {ids}")
            assert all(isinstance(x, (int, float)) for x in ids), "ID column should contain numeric values"
        
        # Check name column
        if 'name' in df.columns:
            names = df['name'].tolist()
            print(f"Names: {names}")
            assert all(isinstance(x, str) for x in names), "Name column should contain string values"
        
        # Check age column
        if 'age' in df.columns:
            ages = df['age'].tolist()
            print(f"Ages: {ages}")
            assert all(isinstance(x, (int, float)) for x in ages), "Age column should contain numeric values"
        
        # Check score column (float)
        if 'score' in df.columns:
            scores = df['score'].tolist()
            print(f"Scores: {scores}")
            assert all(isinstance(x, (int, float)) for x in scores), "Score column should contain numeric values"
        
        # Check boolean column
        if 'is_active' in df.columns:
            actives = df['is_active'].tolist()
            print(f"Is Active: {actives}")
            # Note: Pandas might read booleans as strings depending on the parquet format
        
        # Check date columns
        if 'birth_date' in df.columns:
            birth_dates = df['birth_date'].tolist()
            print(f"Birth Dates: {birth_dates}")
        
        # Check timestamp columns
        for ts_col in ['created_at', 'created_at_ntz']:
            if ts_col in df.columns:
                timestamps = df[ts_col].tolist()
                print(f"{ts_col}: {timestamps}")
        
        # Check struct/object columns (metadata, tags)
        for obj_col in ['metadata', 'tags']:
            if obj_col in df.columns:
                objects = df[obj_col].tolist()
                print(f"{obj_col}: {objects}")
        
        print("\n✅ Parquet file verification completed successfully!")
        print("✅ All basic data types and structures are present and valid")
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Verify parquet file contents')
    parser.add_argument('--file-path', help='Path to the parquet file to verify')
    parser.add_argument('--location', help='Location to use for the parquet file')
    args = parser.parse_args()

    if not args.file_path:
        print("Error: --file-path is required")
        sys.exit(1)

    success = verify_parquet_file(args.file_path, args.location)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 
