# Instrument CSV to SQL Importer

A robust Python script for automatically parsing laboratory instrument CSV outputs and importing them into a SQL database, eliminating manual data entry.

## Features

- ✅ **Automatic Table Creation** - Dynamically creates tables based on CSV structure
- ✅ **Type Inference** - Automatically detects column data types (INTEGER, REAL, TEXT)
- ✅ **Batch Processing** - Efficiently handles large CSV files
- ✅ **Error Handling** - Comprehensive logging and error recovery
- ✅ **Duplicate Detection** - Tracks source files to avoid re-importing
- ✅ **Directory Import** - Process multiple CSV files at once
- ✅ **Column Sanitization** - Handles special characters in column names
- ✅ **Metadata Tracking** - Automatically adds import timestamps and source file info

## Requirements

- Python 3.6+
- No external dependencies (uses built-in libraries only)

## Installation

Simply download the `csv_to_sql.py` script - no installation required!

```bash
# Make executable (optional)
chmod +x csv_to_sql.py
```

**To use as a module in your Python code:**
Just ensure `csv_to_sql.py` is in the same directory as your script, or in your Python path.

```python
# If csv_to_sql.py is in the same directory:
from csv_to_sql import InstrumentCSVImporter

# Or add the directory to your path:
import sys
sys.path.append('/path/to/csv_to_sql/')
from csv_to_sql import InstrumentCSVImporter
```

## Quick Start

### Command Line Usage

**Import a single CSV file:**
```bash
python csv_to_sql.py instrument_data.csv
```

**Import with custom database and table name:**
```bash
python csv_to_sql.py data.csv -d my_database.db -t my_table
```

**Import entire directory:**
```bash
python csv_to_sql.py /path/to/csv/folder -d instruments.db
```

**Import with custom batch size:**
```bash
python csv_to_sql.py large_file.csv -b 5000
```

### Python API Usage

**Important:** Place `csv_to_sql.py` in the same directory as your script, or add it to your Python path.

```python
# Import the class (csv_to_sql.py must be in same directory or in Python path)
from csv_to_sql import InstrumentCSVImporter

# Initialize importer
importer = InstrumentCSVImporter(
    db_path='instruments.db',
    batch_size=1000
)

try:
    # Connect to database
    importer.connect()
    
    # Import single file
    stats = importer.import_csv('data.csv', table_name='readings')
    
    # Import directory
    stats = importer.import_directory('/path/to/csvs')
    
    print(f"Imported {stats['imported_rows']} rows")
    
finally:
    # Always disconnect
    importer.disconnect()
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `input` | CSV file or directory path | (required) |
| `-d, --database` | SQLite database path | `instrument_data.db` |
| `-t, --table` | Table name | Filename without extension |
| `-b, --batch-size` | Rows per batch insert | `1000` |
| `--pattern` | File pattern for directory import | `*.csv` |

## CSV Format Requirements

The script expects CSV files with:
- **Header row** - First row contains column names
- **UTF-8 encoding** - Supports UTF-8 with or without BOM
- **Standard CSV format** - Comma-delimited

### Example CSV:
```csv
Sample_ID,Temperature,Pressure,pH,Concentration,Timestamp
S001,25.3,101.2,7.4,0.025,2024-01-15 10:30:00
S002,25.5,101.3,7.2,0.028,2024-01-15 10:35:00
S003,25.4,101.1,7.5,0.023,2024-01-15 10:40:00
```

## Database Schema

Tables are automatically created with:
- **Auto-incrementing ID** - Primary key
- **Original columns** - From CSV headers (sanitized)
- **import_timestamp** - When the row was imported
- **source_file** - Original CSV filename

### Example Schema:
```sql
CREATE TABLE instrument_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id TEXT,
    temperature REAL,
    pressure REAL,
    ph REAL,
    concentration REAL,
    timestamp TEXT,
    import_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    source_file TEXT
)
```

## Column Name Sanitization

Column names are automatically cleaned:
- Spaces → underscores
- Special characters → underscores  
- Converted to lowercase
- Numbers prepended with `col_` if starting with digit

**Examples:**
- `Sample ID` → `sample_id`
- `Temperature (°C)` → `temperature_c`
- `pH-Value` → `ph_value`
- `1st_Reading` → `col_1st_reading`

## Logging

All operations are logged to:
- **Console** - Real-time progress
- **csv_import.log** - Persistent log file

Log levels:
- INFO: Normal operations
- WARNING: Non-critical issues
- ERROR: Import failures

## Error Handling

The script handles:
- Missing files
- Empty CSV files
- Malformed rows
- Database connection errors
- Batch insert failures

Errors are logged but don't stop the entire import process.

## Use Cases

### Laboratory Instruments
```bash
# HPLC, GC-MS, Spectrophotometer outputs
python csv_to_sql.py /lab/instruments/hplc/ -d lab_results.db
```

### IoT Sensor Data
```bash
# Temperature, pressure, humidity sensors
python csv_to_sql.py sensor_data.csv -t environmental_data
```

### Quality Control
```bash
# QC test results
python csv_to_sql.py qc_results/ -d quality_control.db --pattern "QC_*.csv"
```

### Data Migration
```bash
# Migrate legacy CSV data
python csv_to_sql.py legacy_data/ -d migrated.db -b 10000
```

## Querying the Data

Once imported, query using any SQLite tool:

```python
import sqlite3

conn = sqlite3.connect('instrument_data.db')
cursor = conn.cursor()

# Query recent readings
cursor.execute("""
    SELECT sample_id, temperature, ph 
    FROM instrument_readings 
    WHERE temperature > 25.0
    ORDER BY import_timestamp DESC
    LIMIT 10
""")

for row in cursor.fetchall():
    print(row)

conn.close()
```

Or use SQLite command line:
```bash
sqlite3 instrument_data.db "SELECT * FROM instrument_readings LIMIT 5"
```

## Performance Tips

1. **Batch Size**: Increase for large files (5000-10000)
2. **Indexing**: Add indexes after import for frequently queried columns
3. **Transactions**: Batching automatically uses transactions
4. **Memory**: Script streams data - handles files larger than RAM

## Advanced Examples

### Custom Data Processing

```python
from csv_to_sql import InstrumentCSVImporter

class CustomImporter(InstrumentCSVImporter):
    def infer_column_type(self, value):
        # Custom type inference logic
        if 'ID' in value.upper():
            return 'TEXT'
        return super().infer_column_type(value)

importer = CustomImporter('custom.db')
```

### Scheduled Imports

```python
import schedule
import time

def daily_import():
    importer = InstrumentCSVImporter('daily_data.db')
    importer.connect()
    importer.import_directory('/daily/instruments/')
    importer.disconnect()

schedule.every().day.at("02:00").do(daily_import)

while True:
    schedule.run_pending()
    time.sleep(60)
```

## Troubleshooting

**Issue**: "Database is locked"
- **Solution**: Close other connections, increase timeout

**Issue**: "No such table"  
- **Solution**: Check table name sanitization, verify import completed

**Issue**: "CSV file is empty"
- **Solution**: Verify CSV has header row and data

**Issue**: Memory errors with large files
- **Solution**: Reduce batch size: `-b 500`

## Database Backends

Currently supports SQLite. For other databases (PostgreSQL, MySQL):

1. Replace `sqlite3` with appropriate driver
2. Modify connection string format
3. Adjust SQL syntax for CREATE TABLE
