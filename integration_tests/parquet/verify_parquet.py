#!/usr/bin/env python3

import pandas as pd
import sys
import argparse
from datetime import datetime, timezone
from decimal import Decimal

def verify_parquet_file(file_path, location):
    """
    Read and verify the contents of a parquet file.
    Returns True if all verifications pass, False otherwise.
    """
    # Read the parquet file
    df = pd.read_parquet(file_path)
    
    # Sort by 'id' to ensure consistent row order
    df = df.sort_values(by='id').reset_index(drop=True)
    
    # Print the data
    print("DataFrame contents:")
    print(df)
    print("Column data types:")
    print(df.dtypes)
    
    # Define expected data
    expected_columns = ['id', 'name', 'age', 'created_at', 'created_at_ntz', 'score']
    expected_rows = [
        {
            'id': 1,
            'name': 'John Doe',
            'age': 30,
            'created_at': datetime.fromisoformat("2024-03-20 06:00:00.111") if location else datetime.fromisoformat("2024-03-20 10:00:00.111").replace(tzinfo=timezone.utc),
            'created_at_ntz': datetime.fromisoformat("2024-03-20 06:00:00.111") if location else datetime.fromisoformat("2024-03-20 10:00:00.111").replace(tzinfo=timezone.utc),
            'score': Decimal('-97.410511')
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'age': 25,
            'created_at': datetime.fromisoformat("2024-03-20 07:00:00.555") if location else datetime.fromisoformat("2024-03-20 11:00:00.555").replace(tzinfo=timezone.utc),
            'created_at_ntz': datetime.fromisoformat("2024-03-20 07:00:00.444") if location else datetime.fromisoformat("2024-03-20 11:00:00.444").replace(tzinfo=timezone.utc),
            'score': Decimal('99.410511')
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
            assert actual_value == expected_value, f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
    
    print("All assertions passed!")
    return True

def main():
    parser = argparse.ArgumentParser(description='Verify parquet file contents')
    parser.add_argument('--file-path', help='Path to the parquet file to verify')
    parser.add_argument('--location', help='Location to use for the parquet file')
    args = parser.parse_args()

    success = verify_parquet_file(args.file_path, args.location)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 
