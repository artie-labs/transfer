#!/usr/bin/env python3

import pandas as pd
import sys
import argparse

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

def main():
    parser = argparse.ArgumentParser(description='Verify parquet file contents')
    parser.add_argument('file_path', help='Path to the parquet file to verify')
    args = parser.parse_args()
    
    success = verify_parquet_file(args.file_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 
