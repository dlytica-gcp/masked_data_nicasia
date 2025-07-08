# PostgreSQL CSV Loader

This tool loads CSV files from multiple folders into a PostgreSQL database with automatic schema creation and table mapping.

## Features

- ✅ Loads CSV files from multiple folders into PostgreSQL
- ✅ Automatic schema creation for organized data structure
- ✅ Handles large files with chunked processing
- ✅ Cleans and standardizes column names
- ✅ Comprehensive error handling and logging
- ✅ Processing statistics and progress tracking
- ✅ Configurable database settings

## Folder Structure

The tool processes CSV files from these folders:
- `card/` → Schema: `card_data`
- `crmuser.v2/` → Schema: `crm_data`
- `custom/` → Schema: `custom_data`
- `mobile_banking/` → Schema: `mobile_data`
- `tbaadm/` → Schema: `tba_admin`

## Setup Instructions

### 1. Install Dependencies

**Option A: Using setup script (Windows)**
```cmd
setup.bat
```

**Option B: Manual installation**
```cmd
pip install -r requirements.txt
```

### 2. Configure Database Settings

1. Copy the configuration template:
   ```cmd
   copy config_template.py config.py
   ```

2. Edit `config.py` with your PostgreSQL credentials:
   ```python
   DATABASE_CONFIG = {
    'host': 'datanature-k3s-vm.eastus.cloudapp.azure.com',
    'port': 30100,
    'database': 'ai360',
    'username': 'postgres',
    'password': 'postgres'
   }
   ```

### 3. Run the Loader

**Option A: Enhanced loader (recommended)**
```cmd
python csv_loader.py
```

**Option B: Basic loader**
```cmd
python remove.py
```

## Database Schema

The tool creates the following schema structure:

```
your_database/
├── card_data/
│   ├── account_masked
│   ├── account_type_masked
│   ├── atm_terminal_masked
│   ├── card_masked
│   ├── customer_cardholder_masked
│   └── merchant_masked
├── crm_data/
│   ├── account_masked
│   ├── address_masked
│   ├── categories_masked
│   └── ... (other CRM tables)
├── custom_data/
│   ├── c_loan_pen_int_masked
│   ├── c_oda_pen_int_masked
│   └── ... (other custom tables)
├── mobile_data/
│   └── customer_masked
└── tba_admin/
    ├── account_lien_table
    ├── acct_nomination_table
    └── ... (other admin tables)
```

## Configuration Options

### Database Configuration
```python
DATABASE_CONFIG = {
    'host': 'localhost',        # PostgreSQL host
    'port': 5432,              # PostgreSQL port
    'database': 'your_db',     # Database name
    'username': 'your_user',   # Username
    'password': 'your_pass'    # Password
}
```

### Advanced Configuration
```python
ADVANCED_CONFIG = {
    'chunk_size': 10000,       # Rows per chunk for large files
    'encoding': 'utf-8',       # CSV file encoding
    'create_schemas': True     # Auto-create schemas
}
```

## File Processing

### Column Name Cleaning
- Removes special characters and spaces
- Converts to lowercase
- Replaces with underscores

Example: `Customer Name (ID)` → `customer_name_id`

### Data Type Handling
- Automatic data type inference
- NULL value handling
- String conversion for problematic fields

### Error Handling
- Continues processing if individual files fail
- Logs all errors to `csv_loader.log`
- Provides detailed statistics

## Logging

The tool creates detailed logs in:
- Console output (real-time)
- `csv_loader.log` file

Log levels include:
- INFO: General processing information
- WARNING: Non-critical issues
- ERROR: Failed operations

## Example Output

```
2025-07-02 10:30:15 - INFO - Starting PostgreSQL CSV Loader
2025-07-02 10:30:15 - INFO - Successfully connected to PostgreSQL database: postgres_dump
==================================================
Processing folder: card
Schema: card_data
==================================================
2025-07-02 10:30:16 - INFO - Found 6 CSV files
[1/6] Processing: account_masked.csv
2025-07-02 10:30:16 - INFO -  Successfully loaded 1,250 rows into card_data.account_masked
```

## Troubleshooting

### Common Issues

1. **Connection Error**
   - Check database credentials in `config.py`
   - Ensure PostgreSQL is running
   - Verify network connectivity

2. **Permission Error**
   - Ensure database user has CREATE privileges
   - Check schema creation permissions

3. **CSV Encoding Issues**
   - Try different encodings in `ADVANCED_CONFIG`
   - Common alternatives: 'latin-1', 'cp1252'

4. **Memory Issues**
   - Reduce `chunk_size` in configuration
   - Process folders individually

### Dependencies
- Python 3.7+
- pandas
- psycopg2-binary
- SQLAlchemy

## License

This tool is provided as-is for database migration purposes.
