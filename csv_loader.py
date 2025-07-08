"""
Enhanced CSV to PostgreSQL Loader with Configuration Support
"""
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
import sys

# Try to import configuration
try:
    from config import DATABASE_CONFIG, ADVANCED_CONFIG, FOLDER_SCHEMA_MAPPING
except ImportError:
    # Fallback to default configuration
    DATABASE_CONFIG = {
    'host': 'datanature-k3s-vm.eastus.cloudapp.azure.com',
    'port': 30100,
    'database': 'ai360',
    'username': 'postgres',
    'password': 'postgres'
    }
    ADVANCED_CONFIG = {
        'chunk_size': 10000,
        'encoding': 'utf-8',
        'create_schemas': True,
    }
    FOLDER_SCHEMA_MAPPING = {
        # 'card': 'card_data',
        # 'crmuser': 'crmuser',
        # 'custom': 'custom',
        # 'mobile_banking': 'mobile_data',
        # 'tbaadm': 'tbaadm',
        'qr':'offline_datasync'
    }

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_loader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EnhancedPostgresCSVLoader:
    def __init__(self, config=None):
        """Initialize with configuration"""
        self.config = config or DATABASE_CONFIG
        self.advanced_config = ADVANCED_CONFIG
        self.folder_mapping = FOLDER_SCHEMA_MAPPING
        self.engine = None
        self.stats = {
            'tables_created': 0,
            'rows_inserted': 0,
            'files_processed': 0,
            'errors': 0
        }
        
    def connect(self):
        """Create connection to PostgreSQL database"""
        try:
            connection_string = (
                f"postgresql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(connection_string, echo=False)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully connected to PostgreSQL database: {self.config['database']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_schema(self, schema_name):
        """Create schema if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                conn.commit()
            logger.info(f"Schema '{schema_name}' ready")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            return False
    
    def get_table_info(self, csv_file_path):
        """Analyze CSV file and return table information"""
        try:
            # Read first few rows to analyze structure
            sample_df = pd.read_csv(csv_file_path, nrows=100, encoding=self.advanced_config['encoding'])
            
            info = {
                'columns': len(sample_df.columns),
                'sample_rows': len(sample_df),
                'column_names': list(sample_df.columns),
                'data_types': dict(sample_df.dtypes)
            }
            return info
        except Exception as e:
            logger.error(f"Failed to analyze CSV file {csv_file_path}: {e}")
            return None
    
    def clean_column_names(self, df):
        """Clean and standardize column names"""
        # Replace special characters and spaces with underscores
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        # Remove multiple consecutive underscores
        df.columns = df.columns.str.replace(r'_+', '_', regex=True)
        # Remove leading/trailing underscores
        df.columns = df.columns.str.strip('_')
        # Convert to lowercase
        df.columns = df.columns.str.lower()
        return df
    
    def load_csv_to_postgres(self, csv_file_path, table_name, schema=None):
        """Load CSV file into PostgreSQL table with enhanced error handling"""
        try:
            csv_file_path = Path(csv_file_path)
            logger.info(f"Processing: {csv_file_path.name}")
            
            # Get file info
            file_info = self.get_table_info(csv_file_path)
            if not file_info:
                return False
            
            logger.info(f"File info - Columns: {file_info['columns']}, Sample rows: {file_info['sample_rows']}")
            
            # Read CSV file in chunks if large
            chunk_size = self.advanced_config['chunk_size']
            total_rows = 0
            chunk_number = 0
            
            # Process file in chunks
            for chunk_df in pd.read_csv(csv_file_path, 
                                      encoding=self.advanced_config['encoding'],
                                      chunksize=chunk_size):
                chunk_number += 1
                
                # Clean column names
                chunk_df = self.clean_column_names(chunk_df)
                
                # Handle missing values
                chunk_df = chunk_df.fillna('')
                
                # Convert problematic data types
                for col in chunk_df.columns:
                    if chunk_df[col].dtype == 'object':
                        chunk_df[col] = chunk_df[col].astype(str)
                
                # Insert data
                if_exists = 'replace' if chunk_number == 1 else 'append'
                
                chunk_df.to_sql(
                    table_name, 
                    self.engine, 
                    schema=schema,
                    if_exists=if_exists, 
                    index=False, 
                    method='multi'
                )
                
                total_rows += len(chunk_df)
                logger.info(f"Loaded chunk {chunk_number} ({len(chunk_df)} rows)")
            
            self.stats['tables_created'] += 1 if chunk_number > 0 else 0
            self.stats['rows_inserted'] += total_rows
            self.stats['files_processed'] += 1
            
            logger.info(f" Successfully loaded {total_rows} rows into {schema}.{table_name}" if schema else f" Successfully loaded {total_rows} rows into {table_name}")
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f" Failed to load {csv_file_path}: {e}")
            return False
    
    def process_folder(self, folder_path, schema_name=None):
        """Process all CSV files in a folder"""
        folder_path = Path(folder_path)
        if not folder_path.exists():
            logger.error(f"Folder does not exist: {folder_path}")
            return
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing folder: {folder_path.name}")
        logger.info(f"Schema: {schema_name}")
        logger.info(f"{'='*50}")
        
        # Create schema if specified
        if schema_name and self.advanced_config['create_schemas']:
            self.create_schema(schema_name)
        
        # Get all CSV files
        csv_files = list(folder_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {folder_path}")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"\n[{i}/{len(csv_files)}] Processing: {csv_file.name}")
            
            # Generate table name from file name
            table_name = csv_file.stem.replace('-', '_').replace(' ', '_').lower()
            table_name = table_name.replace('.', '_')  # Handle files like "crmuser.v2"
            
            # Load CSV into database
            self.load_csv_to_postgres(csv_file, table_name, schema=schema_name)
    
    def process_all_folders(self, base_path='.'):
        """Process all folders containing CSV files"""
        base_path = Path(base_path)
        logger.info(f"Starting CSV loading process from: {base_path.absolute()}")
        
        for folder_name, schema_name in self.folder_mapping.items():
            folder_path = base_path / folder_name
            if folder_path.exists():
                self.process_folder(folder_path, schema_name)
            else:
                logger.warning(f"Folder not found: {folder_path}")
    
    def print_statistics(self):
        """Print loading statistics"""
        logger.info(f"\n{'='*50}")
        logger.info("LOADING STATISTICS")
        logger.info(f"{'='*50}")
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Tables created: {self.stats['tables_created']}")
        logger.info(f"Total rows inserted: {self.stats['rows_inserted']:,}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info(f"{'='*50}")
    
    def close_connection(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
        logger.info("Database connection closed")

def main():
    """Main function to execute the CSV loading process"""
    logger.info("Starting PostgreSQL CSV Loader")
    logger.info(f"Configuration: {DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}")
    
    # Initialize loader
    loader = EnhancedPostgresCSVLoader()
    
    try:
        # Connect to database
        if not loader.connect():
            logger.error("Failed to connect to database. Please check your configuration.")
            return
        
        # Process all folders
        current_directory = Path(__file__).parent
        loader.process_all_folders(current_directory)
        
        # Print statistics
        loader.print_statistics()
        
        logger.info(" CSV loading process completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
    finally:
        # Close connection
        loader.close_connection()

if __name__ == "__main__":
    main()
