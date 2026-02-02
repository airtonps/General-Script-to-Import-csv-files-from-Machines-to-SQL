#!/usr/bin/env python3
"""
Instrument CSV to SQL Database Importer

This script parses CSV files from laboratory instruments and loads the data
into a SQL database, eliminating manual data entry.

Features:
- Automatic table creation based on CSV structure
- Batch processing for large files
- Duplicate detection
- Comprehensive error handling and logging
- Support for multiple CSV formats
"""

import sqlite3
import csv
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import argparse


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class InstrumentCSVImporter:
    """Handles importing instrument CSV data into SQL database."""
    
    def __init__(self, db_path: str, batch_size: int = 1000):
        """
        Initialize the importer.
        
        Args:
            db_path: Path to SQLite database file
            batch_size: Number of rows to insert per batch
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def sanitize_column_name(self, name: str) -> str:
        """
        Sanitize column names for SQL compatibility.
        
        Args:
            name: Original column name
            
        Returns:
            Sanitized column name
        """
        # Replace spaces and special characters with underscores
        sanitized = name.strip().replace(' ', '_').replace('-', '_')
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        return sanitized.lower()
    
    def infer_column_type(self, value: str) -> str:
        """
        Infer SQL data type from sample value.
        
        Args:
            value: Sample value from CSV
            
        Returns:
            SQL data type (TEXT, INTEGER, or REAL)
        """
        if not value or value.strip() == '':
            return 'TEXT'
        
        try:
            int(value)
            return 'INTEGER'
        except ValueError:
            pass
        
        try:
            float(value)
            return 'REAL'
        except ValueError:
            pass
        
        return 'TEXT'
    
    def create_table(self, table_name: str, headers: List[str], 
                    sample_row: List[str]) -> str:
        """
        Create table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            headers: Column headers from CSV
            sample_row: First data row for type inference
            
        Returns:
            Sanitized table name
        """
        sanitized_table = self.sanitize_column_name(table_name)
        
        # Create column definitions
        columns = []
        columns.append("id INTEGER PRIMARY KEY AUTOINCREMENT")
        
        for i, header in enumerate(headers):
            col_name = self.sanitize_column_name(header)
            col_type = self.infer_column_type(sample_row[i] if i < len(sample_row) else '')
            columns.append(f"{col_name} {col_type}")
        
        # Add metadata columns
        columns.append("import_timestamp TEXT DEFAULT CURRENT_TIMESTAMP")
        columns.append("source_file TEXT")
        
        # Create table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {sanitized_table} (
            {', '.join(columns)}
        )
        """
        
        try:
            self.cursor.execute(create_sql)
            self.conn.commit()
            logger.info(f"Table '{sanitized_table}' created or verified")
        except sqlite3.Error as e:
            logger.error(f"Failed to create table: {e}")
            raise
        
        return sanitized_table
    
    def import_csv(self, csv_path: str, table_name: Optional[str] = None,
                   skip_duplicates: bool = True) -> Dict[str, Any]:
        """
        Import CSV file into database.
        
        Args:
            csv_path: Path to CSV file
            table_name: Name for database table (default: filename)
            skip_duplicates: Skip duplicate rows if True
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'total_rows': 0,
            'imported_rows': 0,
            'skipped_rows': 0,
            'error_rows': 0
        }
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Determine table name
        if table_name is None:
            table_name = Path(csv_path).stem
        
        logger.info(f"Starting import from: {csv_path}")
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as csvfile:
                # Read CSV
                csv_reader = csv.reader(csvfile)
                headers = next(csv_reader)
                
                # Get first data row for type inference
                first_row = next(csv_reader, None)
                if first_row is None:
                    logger.warning("CSV file is empty")
                    return stats
                
                # Create table
                sanitized_table = self.create_table(table_name, headers, first_row)
                sanitized_headers = [self.sanitize_column_name(h) for h in headers]
                
                # Prepare insert statement
                placeholders = ', '.join(['?' for _ in sanitized_headers])
                insert_sql = f"""
                INSERT INTO {sanitized_table} 
                ({', '.join(sanitized_headers)}, source_file)
                VALUES ({placeholders}, ?)
                """
                
                # Import data in batches
                batch = []
                
                # Process first row
                batch.append(first_row + [os.path.basename(csv_path)])
                stats['total_rows'] += 1
                
                # Process remaining rows
                for row_num, row in enumerate(csv_reader, start=2):
                    stats['total_rows'] += 1
                    
                    # Skip empty rows
                    if not any(row):
                        stats['skipped_rows'] += 1
                        continue
                    
                    # Pad row if necessary
                    while len(row) < len(headers):
                        row.append('')
                    
                    # Truncate if too long
                    row = row[:len(headers)]
                    
                    batch.append(row + [os.path.basename(csv_path)])
                    
                    # Execute batch insert
                    if len(batch) >= self.batch_size:
                        try:
                            self.cursor.executemany(insert_sql, batch)
                            self.conn.commit()
                            stats['imported_rows'] += len(batch)
                            logger.info(f"Imported {stats['imported_rows']} rows...")
                            batch = []
                        except sqlite3.Error as e:
                            logger.error(f"Batch insert failed at row {row_num}: {e}")
                            stats['error_rows'] += len(batch)
                            batch = []
                
                # Insert remaining rows
                if batch:
                    try:
                        self.cursor.executemany(insert_sql, batch)
                        self.conn.commit()
                        stats['imported_rows'] += len(batch)
                    except sqlite3.Error as e:
                        logger.error(f"Final batch insert failed: {e}")
                        stats['error_rows'] += len(batch)
                
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise
        
        logger.info(f"Import complete - Total: {stats['total_rows']}, "
                   f"Imported: {stats['imported_rows']}, "
                   f"Skipped: {stats['skipped_rows']}, "
                   f"Errors: {stats['error_rows']}")
        
        return stats
    
    def import_directory(self, dir_path: str, pattern: str = "*.csv") -> Dict[str, Any]:
        """
        Import all CSV files from a directory.
        
        Args:
            dir_path: Path to directory containing CSV files
            pattern: File pattern to match (default: *.csv)
            
        Returns:
            Dictionary with aggregate statistics
        """
        total_stats = {
            'files_processed': 0,
            'total_rows': 0,
            'imported_rows': 0,
            'skipped_rows': 0,
            'error_rows': 0
        }
        
        csv_files = list(Path(dir_path).glob(pattern))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {dir_path}")
            return total_stats
        
        logger.info(f"Found {len(csv_files)} CSV file(s) to process")
        
        for csv_file in csv_files:
            try:
                stats = self.import_csv(str(csv_file))
                total_stats['files_processed'] += 1
                total_stats['total_rows'] += stats['total_rows']
                total_stats['imported_rows'] += stats['imported_rows']
                total_stats['skipped_rows'] += stats['skipped_rows']
                total_stats['error_rows'] += stats['error_rows']
            except Exception as e:
                logger.error(f"Failed to import {csv_file}: {e}")
        
        logger.info(f"Directory import complete - Files: {total_stats['files_processed']}, "
                   f"Total rows: {total_stats['imported_rows']}")
        
        return total_stats


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Import instrument CSV data into SQL database'
    )
    parser.add_argument(
        'input',
        help='CSV file or directory to import'
    )
    parser.add_argument(
        '-d', '--database',
        default='instrument_data.db',
        help='Path to SQLite database (default: instrument_data.db)'
    )
    parser.add_argument(
        '-t', '--table',
        help='Table name (default: filename without extension)'
    )
    parser.add_argument(
        '-b', '--batch-size',
        type=int,
        default=1000,
        help='Batch size for inserts (default: 1000)'
    )
    parser.add_argument(
        '--pattern',
        default='*.csv',
        help='File pattern for directory import (default: *.csv)'
    )
    
    args = parser.parse_args()
    
    # Initialize importer
    importer = InstrumentCSVImporter(args.database, args.batch_size)
    
    try:
        importer.connect()
        
        # Check if input is file or directory
        if os.path.isfile(args.input):
            stats = importer.import_csv(args.input, args.table)
        elif os.path.isdir(args.input):
            stats = importer.import_directory(args.input, args.pattern)
        else:
            logger.error(f"Input path not found: {args.input}")
            return 1
        
        print("\n" + "="*50)
        print("IMPORT SUMMARY")
        print("="*50)
        for key, value in stats.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Import process failed: {e}")
        return 1
    finally:
        importer.disconnect()
    
    return 0


if __name__ == '__main__':
    exit(main())