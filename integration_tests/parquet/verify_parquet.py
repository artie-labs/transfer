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
    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        # Print the data
        print("DataFrame contents:")
        print(df)
        
        # Verify the data
        assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
        assert list(df.columns) == ['id', 'name', 'age', 'created_at'], f"Expected columns ['id', 'name', 'age', 'created_at'], got {list(df.columns)}"
        
        # Verify first row
        assert df.iloc[0]['id'] == 1, f"Expected id=1, got {df.iloc[0]['id']}"
        assert df.iloc[0]['name'] == "John Doe", f"Expected name='John Doe', got {df.iloc[0]['name']}"
        assert df.iloc[0]['age'] == 30, f"Expected age=30, got {df.iloc[0]['age']}"
        assert pd.to_datetime(df.iloc[0]['created_at']).isoformat() == "2024-03-20T10:00:00+00:00", f"Expected created_at='2024-03-20T10:00:00Z', got {df.iloc[0]['created_at']}"
        
        # Verify second row
        assert df.iloc[1]['id'] == 2, f"Expected id=2, got {df.iloc[1]['id']}"
        assert df.iloc[1]['name'] == "Jane Smith", f"Expected name='Jane Smith', got {df.iloc[1]['name']}"
        assert df.iloc[1]['age'] == 25, f"Expected age=25, got {df.iloc[1]['age']}"
        assert pd.to_datetime(df.iloc[1]['created_at']).isoformat() == "2024-03-20T11:00:00+00:00", f"Expected created_at='2024-03-20T11:00:00Z', got {df.iloc[1]['created_at']}"
        
        print("\nAll verifications passed!")
        return True
        
    except Exception as e:
        print(f"\nVerification failed: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Verify parquet file contents')
    parser.add_argument('file_path', help='Path to the parquet file to verify')
    args = parser.parse_args()
    
    success = verify_parquet_file(args.file_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 
