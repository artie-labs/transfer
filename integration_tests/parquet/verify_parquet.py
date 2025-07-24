#!/usr/bin/env python3

import pandas as pd
import sys
import argparse
from datetime import datetime, timezone, date, time
from decimal import Decimal
import json

def verify_parquet_file(file_path, location):
    """
    Read and verify the contents of a comprehensive parquet file.
    Returns True if all verifications pass, False otherwise.
    """
    # Read the parquet file
    df = pd.read_parquet(file_path)
    
    # Sort by 'id' to ensure consistent row order
    df = df.sort_values(by='id').reset_index(drop=True)
    
    # Print the data
    print("DataFrame contents:")
    print(df)
    print("\nColumn data types:")
    print(df.dtypes)
    print(f"\nDataFrame shape: {df.shape}")
    
    # Define expected columns
    expected_columns = [
        'id', 'name', 'age', 'is_active', 'score',
        'birth_date', 'lunch_time', 'created_at', 'updated_at',
        'decimal_small', 'decimal_large', 'decimal_max',
        'description', 'big_integer', 'unicode_text', 'empty_string', 'complex_json_string'
    ]
    
    # Define expected data for comprehensive test
    expected_rows = [
        {
            'id': 1,
            'name': 'John Doe',
            'age': 30,
            'is_active': True,
            'score': 98.5,
            'birth_date': date(1993, 5, 15),
            'lunch_time': time(12, 30, 45),  # time values are not timezone-adjusted in parquet
            'created_at': datetime.fromisoformat("2024-03-20 06:00:00.111") if location else datetime.fromisoformat("2024-03-20 10:00:00.111").replace(tzinfo=timezone.utc),
            'updated_at': datetime.fromisoformat("2024-03-20 06:00:00.111") if location else datetime.fromisoformat("2024-03-20 10:00:00.111").replace(tzinfo=timezone.utc),
            'decimal_small': Decimal('123.45'),
            'decimal_large': Decimal('1234567890.1234567890'),
            'decimal_max': Decimal('123456789012345.123456789012345'),
            'description': 'A premium user from the west coast',
            'big_integer': 9223372036854775807,
            'unicode_text': 'Hello 世界 🌍 émojis and ünicödé',
            'empty_string': '',
            'complex_json_string': '{"tags":["user","premium","active"],"metadata":{"country":"US","region":"west","score":100}}',
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'age': 0,
            'is_active': False,
            'score': 0.0,
            'birth_date': date(2000, 1, 1),
            'lunch_time': time(0, 0, 0),  # time values are not timezone-adjusted in parquet
            'created_at': datetime.fromisoformat("2024-03-20 07:00:00.555") if location else datetime.fromisoformat("2024-03-20 11:00:00.555").replace(tzinfo=timezone.utc),
            'updated_at': datetime.fromisoformat("2024-03-20 07:00:00.444") if location else datetime.fromisoformat("2024-03-20 11:00:00.444").replace(tzinfo=timezone.utc),
            'decimal_small': Decimal('0.00'),
            'decimal_large': Decimal('-999.9999999999'),
            'decimal_max': Decimal('-1.000000000000001'),
            'description': 'User with edge case values',
            'big_integer': -9223372036854775808,
            'unicode_text': 'Special chars: !@#$%^&*()_+-={}[]|\\:;"\'<>?,./ \t\n',
            'empty_string': '',
            'complex_json_string': '{"tags":[],"metadata":{}}',
        },
        {
            'id': 3,
            'name': 'Bob Wilson',
            'age': -1,
            'is_active': True,
            'score': -45.67,
            'birth_date': date(1970, 1, 1),
            'lunch_time': time(23, 59, 59),  # time values are not timezone-adjusted in parquet
            'created_at': datetime.fromisoformat("1969-12-31 19:00:00.000") if location else datetime.fromisoformat("1970-01-01 00:00:00.000").replace(tzinfo=timezone.utc),
            'updated_at': datetime.fromisoformat("2099-12-31 18:59:59.999") if location else datetime.fromisoformat("2099-12-31 23:59:59.999").replace(tzinfo=timezone.utc),
            'decimal_small': Decimal('-99.99'),
            'decimal_large': Decimal('999999999.9999999999'),
            'decimal_max': Decimal('999999999999999.999999999999999'),
            'description': 'Testing negative values and edge cases',
            'big_integer': 1,
            'unicode_text': '中文 العربية русский 한국어 日本語',
            'empty_string': '',
            'complex_json_string': '{"tags":["test","negative","special"],"nested":{"level":2,"test":true}}',
        },
        {
            'id': 4,
            'name': 'Alice Johnson',
            'age': 25,
            'is_active': True,
            'score': 75.25,
            'birth_date': date(1999, 2, 28),
            'lunch_time': time(12, 0, 0, 123000),  # time values are not timezone-adjusted in parquet, with milliseconds
            'created_at': datetime.fromisoformat("2024-02-29 07:00:00.123") if location else datetime.fromisoformat("2024-02-29 12:00:00.123").replace(tzinfo=timezone.utc),
            'updated_at': datetime.fromisoformat("2024-02-29 07:00:00.123") if location else datetime.fromisoformat("2024-02-29 12:00:00.123").replace(tzinfo=timezone.utc),
            'decimal_small': Decimal('12.34'),
            'decimal_large': Decimal('0.0000000001'),
            'decimal_max': Decimal('0.000000000000001'),
            'description': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.',
            'big_integer': 42,
            'unicode_text': '🎉🎊🎈🎁🎂🍰🍪🍫🍬🍭🍮🍯🍼🥛🍵☕🧃🥤🧋',
            'empty_string': '',
            'complex_json_string': '{"tags":["looooooooooooooooooooooooooooooooooong","user","test"],"complex":{"nested":{"deep":{"array":[1,"two",3.14,true],"level":4}}},"array_of_objects":[{"id":1,"name":"first"},{"id":2,"name":"second"}]}',
        },
        {
            'id': 5,
            'name': 'Charlie Brown',
            'age': 999,
            'is_active': False,
            'score': 100.0,
            'birth_date': date(1900, 1, 1),
            'lunch_time': time(1, 23, 45),  # time values are not timezone-adjusted in parquet
            'created_at': datetime.fromisoformat("2024-12-31 18:59:59.999") if location else datetime.fromisoformat("2024-12-31 23:59:59.999").replace(tzinfo=timezone.utc),
            'updated_at': datetime.fromisoformat("2024-12-31 18:59:59.999") if location else datetime.fromisoformat("2024-12-31 23:59:59.999").replace(tzinfo=timezone.utc),
            'decimal_small': Decimal('999.99'),
            'decimal_large': Decimal('-1234567890.1234567890'),
            'decimal_max': Decimal('-99999999999999.999999999999999'),
            'description': 'Testing maximum and minimum decimal values with edge cases',
            'big_integer': 0,
            'unicode_text': 'Mixed: ABC123 αβγ ✓✗ ←→↑↓ ♠♣♥♦',
            'empty_string': '',
            'complex_json_string': '{"tags":["edge","case","testing","decimal","precision"],"config":{"debug":true,"verbose":false},"version":"1.0"}',
        }
    ]
    
    # For the comprehensive test with 8 rows, we only verify the first 5 unless all 8 are present
    if len(df) == 5:
        print("Found 5 rows - using basic comprehensive test verification")
    elif len(df) == 8:
        print("Found 8 rows - using extended comprehensive test verification")
        # Add expected data for rows 6-8
        expected_rows.extend([
            {
                'id': 6,
                'name': 'Diana Prince',
                'age': 28,
                'is_active': True,
                'score': 3.1415927,  # float32 precision
                'birth_date': date(1996, 6, 6),
                'lunch_time': time(13, 37, 42, 999000),  # max milliseconds
                'created_at': datetime.fromisoformat("2000-02-28 19:00:01.001") if location else datetime.fromisoformat("2000-02-29 00:00:01.001").replace(tzinfo=timezone.utc),
                'updated_at': datetime.fromisoformat("2000-02-28 19:00:01.001") if location else datetime.fromisoformat("2000-02-29 00:00:01.001").replace(tzinfo=timezone.utc),
                'decimal_small': Decimal('1.01'),
                'decimal_large': Decimal('99999999.9999999999'),
                'decimal_max': Decimal('999999999999999.000000000000001'),
                'description': 'Testing float precision and very small decimal differences',
                'big_integer': 1000000000000000000,
                'unicode_text': 'Math: π≈3.14159, ∑∞ √∞ ∫∂ ≠≤≥±×÷∙',
                'empty_string': '',
                'complex_json_string': '{"precision":{"float32":3.14159265359,"double":3.141592653589793},"scientific":{"large":"1.23e+10","small":"1.23e-10"}}',
            },
            {
                'id': 7,
                'name': 'Eve Adams',
                'age': 100,
                'is_active': False,
                'score': 0.001,
                'birth_date': date(1924, 2, 29),
                'lunch_time': time(23, 59, 59, 999000),  # last millisecond
                'created_at': datetime.fromisoformat("2038-01-18 22:14:07.999") if location else datetime.fromisoformat("2038-01-19 03:14:07.999").replace(tzinfo=timezone.utc),
                'updated_at': datetime.fromisoformat("2038-01-18 22:14:07.999") if location else datetime.fromisoformat("2038-01-19 03:14:07.999").replace(tzinfo=timezone.utc),
                'decimal_small': Decimal('-0.01'),
                'decimal_large': Decimal('-0.0000000001'),
                'decimal_max': Decimal('-0.000000000000001'),
                'description': 'Testing date/time edge cases and century boundaries',
                'big_integer': -1000000000000000000,
                'unicode_text': 'Legacy encoding: ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ',
                'empty_string': '',
                'complex_json_string': '{"century":{"year":1924,"leap":true},"millennium":{"y2k":2000,"y2038":2038},"unicode":{"legacy":"ÀÁÂ","modern":"🚀🌟"}}',
            },
            {
                'id': 8,
                'name': 'Frank Miller',
                'age': 1,
                'is_active': True,
                'score': 0.0,  # -0.0 becomes 0.0
                'birth_date': date(2023, 12, 31),
                'lunch_time': time(0, 0, 0, 1000),  # first millisecond
                'created_at': datetime.fromisoformat("1900-12-31 19:00:00.000") if location else datetime.fromisoformat("1901-01-01 00:00:00.000").replace(tzinfo=timezone.utc),
                'updated_at': datetime.fromisoformat("1900-12-31 19:00:00.000") if location else datetime.fromisoformat("1901-01-01 00:00:00.000").replace(tzinfo=timezone.utc),
                'decimal_small': Decimal('99.99'),
                'decimal_large': Decimal('1.0000000000'),
                'decimal_max': Decimal('1.000000000000000'),
                'description': 'Testing string with quotes "and" \'various\' `backticks` and [brackets] {braces} <angles>',
                'big_integer': 123456789,
                'unicode_text': 'Code: control chars and spaces     ',
                'empty_string': '',
                'complex_json_string': '{"quotes":{"double":"\\"hello\\"","single":"\'world\'","backtick":"\\`code\\`"},"symbols":{"brackets":"[array]","braces":"{object}","angles":"<tag>"}}',
            }
        ])
    else:
        print(f"Unexpected number of rows: {len(df)}. Expected 5 or 8.")
        return False
    
    # Verify column headers
    assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, got {list(df.columns)}"
    
    # Verify row count
    assert len(df) == len(expected_rows), f"Expected {len(expected_rows)} rows, got {len(df)}"
    
    print("\nStarting detailed verification...")
    
    # Verify each row
    for i, expected_row in enumerate(expected_rows):
        print(f"\nVerifying row {i+1} (id={expected_row['id']})...")
        for col, expected_value in expected_row.items():
            actual_value = df.iloc[i][col]
            
            # Handle special cases for different data types
            if col in ['complex_json_string']:
                # For JSON string columns, parse and compare
                try:
                    expected_json = json.loads(expected_value)
                    actual_json = json.loads(str(actual_value))
                    assert actual_json == expected_json, f"Row {i}, Column {col}: Expected {expected_json}, got {actual_json}"
                except json.JSONDecodeError as e:
                    print(f"Warning: JSON parsing failed for {col}: {e}")
                    assert str(actual_value) == str(expected_value), f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
            elif col in ['decimal_small', 'decimal_large', 'decimal_max']:
                # For decimal columns, compare as strings to handle precision
                assert str(actual_value) == str(expected_value), f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
            elif col in ['birth_date', 'lunch_time']:
                # For date/time columns, handle type conversions
                if pd.isna(actual_value):
                    assert expected_value is None, f"Row {i}, Column {col}: Expected {expected_value}, got None"
                else:
                    if col == 'birth_date':
                        if hasattr(actual_value, 'date'):
                            actual_value = actual_value.date()
                    elif col == 'lunch_time':
                        if hasattr(actual_value, 'time'):
                            actual_value = actual_value.time()
                    assert actual_value == expected_value, f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
            elif col in ['created_at', 'updated_at']:
                # For timestamp columns, handle timezone conversions
                if pd.isna(actual_value):
                    assert expected_value is None, f"Row {i}, Column {col}: Expected {expected_value}, got None"
                else:
                    # Convert to UTC for comparison if needed
                    if hasattr(actual_value, 'tz_localize') and actual_value.tz is None:
                        actual_value = actual_value.tz_localize('UTC')
                    elif hasattr(actual_value, 'tz_convert'):
                        actual_value = actual_value.tz_convert('UTC')
                    
                    if hasattr(expected_value, 'replace') and expected_value.tzinfo is None:
                        expected_value = expected_value.replace(tzinfo=timezone.utc)
                    
                    # Compare timestamps with some tolerance for microseconds
                    if hasattr(actual_value, 'replace') and hasattr(expected_value, 'replace'):
                        # Truncate to milliseconds for comparison
                        actual_ms = actual_value.replace(microsecond=(actual_value.microsecond // 1000) * 1000)
                        expected_ms = expected_value.replace(microsecond=(expected_value.microsecond // 1000) * 1000)
                        assert actual_ms == expected_ms, f"Row {i}, Column {col}: Expected {expected_ms}, got {actual_ms}"
                    else:
                        assert actual_value == expected_value, f"Row {i}, Column {col}: Expected {expected_value}, got {actual_value}"
            else:
                # For all other columns, direct comparison
                assert actual_value == expected_value, f"Row {i}, Column {col}: Expected {expected_value} (type: {type(expected_value)}), got {actual_value} (type: {type(actual_value)})"
            
            print(f"  ✓ {col}: {actual_value}")
    
    print("\n🎉 All assertions passed! Comprehensive test successful!")
    
    # Additional verification: check data types
    print("\nData type verification:")
    type_checks = {
        'id': 'int64',
        'age': 'int64',
        'is_active': 'bool',
        'score': 'float64',
        'big_integer': 'int64',
    }
    
    for col, expected_dtype in type_checks.items():
        actual_dtype = str(df[col].dtype)
        print(f"  {col}: {actual_dtype} (expected: {expected_dtype})")
        # Note: We're being flexible with exact dtype matching since parquet can have variations
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Verify comprehensive parquet file contents')
    parser.add_argument('--file-path', help='Path to the parquet file to verify')
    parser.add_argument('--location', help='Location to use for the parquet file')
    args = parser.parse_args()

    try:
        success = verify_parquet_file(args.file_path, args.location)
        print(f"\n✅ Verification {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Verification FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 
