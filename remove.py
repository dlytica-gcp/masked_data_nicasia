import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresCSVLoader:
    def __init__(self, host='localhost', port=5432, database='your_database', 
                 username='your_username', password='your_password'):
        """
        Initialize PostgreSQL connection parameters
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.engine = None
        self.connection = None
        
    def connect(self):
        """
        Create connection to PostgreSQL database
        """
        try:
            # Create SQLAlchemy engine
            connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)
            
            # Test connection
            self.connection = self.engine.connect()
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_table_from_csv(self, df, table_name, schema=None):
        """
        Create table based on CSV structure
        """
        try:
            # Clean column names (remove special characters, spaces)
            df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
            df.columns = df.columns.str.lower()
            
            # Create table with data types inferred from pandas
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Drop table if exists and create new one
            df.head(0).to_sql(table_name, self.engine, schema=schema, 
                             if_exists='replace', index=False)
            logger.info(f"Created table: {full_table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def load_csv_to_postgres(self, csv_file_path, table_name, schema=None, 
                           chunk_size=10000, if_exists='replace'):
        """
        Load CSV file into PostgreSQL table
        """
        try:
            logger.info(f"Loading {csv_file_path} into table {table_name}")
            
            # Read CSV file
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            
            # Clean column names
            df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
            df.columns = df.columns.str.lower()
            
            # Handle missing values
            df = df.fillna('')
            
            # Load data in chunks for large files
            total_rows = len(df)
            if total_rows > chunk_size:
                logger.info(f"Loading {total_rows} rows in chunks of {chunk_size}")
                for i in range(0, total_rows, chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    chunk.to_sql(table_name, self.engine, schema=schema,
                               if_exists='append' if i > 0 else if_exists, 
                               index=False, method='multi')
                    logger.info(f"Loaded chunk {i//chunk_size + 1}/{(total_rows//chunk_size) + 1}")
            else:
                df.to_sql(table_name, self.engine, schema=schema,
                         if_exists=if_exists, index=False, method='multi')
            
            logger.info(f"Successfully loaded {total_rows} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {csv_file_path}: {e}")
            return False
    
    def process_folder(self, folder_path, schema_name=None):
        """
        Process all CSV files in a folder
        """
        folder_path = Path(folder_path)
        if not folder_path.exists():
            logger.error(f"Folder does not exist: {folder_path}")
            return
        
        logger.info(f"Processing folder: {folder_path}")
        
        # Get all CSV files in the folder
        csv_files = list(folder_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {folder_path}")
            return
        
        for csv_file in csv_files:
            # Generate table name from file name
            table_name = csv_file.stem.replace('-', '_').replace(' ', '_').lower()
            
            # Load CSV into database
            self.load_csv_to_postgres(csv_file, table_name, schema=schema_name)
    
    def process_all_folders(self, base_path='.'):
        """
        Process all folders containing CSV files
        """
        base_path = Path(base_path)
        
        # Define folder to schema mapping
        folder_schema_mapping = {
            'card': 'card_data',
            'crmuser.v2': 'crm_data',
            'custom': 'custom_data',
            'mobile_banking': 'mobile_data',
            'tbaadm': 'tba_admin'
        }
        
        for folder_name, schema_name in folder_schema_mapping.items():
            folder_path = base_path / folder_name
            if folder_path.exists():
                # Create schema if it doesn't exist
                self.create_schema(schema_name)
                self.process_folder(folder_path, schema_name)
            else:
                logger.warning(f"Folder not found: {folder_path}")
    
    def create_schema(self, schema_name):
        """
        Create schema if it doesn't exist
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                conn.commit()
            logger.info(f"Schema '{schema_name}' created or already exists")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
    
    def close_connection(self):
        """
        Close database connection
        """
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connection closed")

def main():
    """
    Main function to execute the CSV loading process
    """
    # Database configuration - UPDATE THESE VALUES
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database_name',
        'username': 'your_username',
        'password': 'your_password'
    }
    
    # Initialize loader
    loader = PostgresCSVLoader(**DB_CONFIG)
    
    try:
        # Connect to database
        if not loader.connect():
            logger.error("Failed to connect to database. Please check your configuration.")
            return
        
        # Process all folders
        current_directory = Path(__file__).parent
        loader.process_all_folders(current_directory)
        
        logger.info("CSV loading process completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
    
    finally:
        # Close connection
        loader.close_connection()

if __name__ == "__main__":
    main()