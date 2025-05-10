#!/usr/bin/env python3

import pandas as pd
import sys
import argparse
from datetime import datetime

def verify_parquet_file(file_path):
    """
    Read and verify the contents of a parquet file.
    Returns True if all verifications pass, False otherwise.
    """
    # Read the parquet file
    df = pd.read_parquet(file_path)
    
    # Print the data
    print("DataFrame contents:")
    print(df)
    
    # Define expected data
    expected_columns = ['id', 'name', 'age', 'created_at', 'score']
    expected_rows = [
        {
            'id': 1,
            'name': 'John Doe',
            'age': 30,
            'created_at': datetime.fromisoformat("2024-03-20T10:00:00+00:00"),
            'score': -97.410511i
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'age': 25,
            'created_at': datetime.fromisoformat("2024-03-20T11:00:00+00:00"),
            'score': -97.410511
        }
    ]
    
    # Verify column headers
    assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, got {list(df.columns)}"
    
    # Verify row count
    assert len(df) == len(expected_rows), f"Expected {len(expected_rows)} rows, got {len(df)}"
    
    # Verify each row
    for i, expected_row in enumerate(expected_rows):
        for col, expected_value in expected_row.items():
            actual_value = df.iloc[i][col]
            if isinstance(expected_value, float):
                assert abs(actual_value - expected_value) < 1e-6, f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
            else:
                assert actual_value == expected_value, f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
    
    print("All assertions passed!")
    return True

def main():
    parser = argparse.ArgumentParser(description='Verify parquet file contents')
    parser.add_argument('file_path', help='Path to the parquet file to verify')
    args = parser.parse_args()
    
    success = verify_parquet_file(args.file_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 
