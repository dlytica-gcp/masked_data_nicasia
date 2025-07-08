# Database Configuration
# Copy this file to config.py and update with your actual database credentials

DATABASE_CONFIG = {
    'host': 'datanature-k3s-vm.eastus.cloudapp.azure.com',
    'port': 30100,
    'database': 'ai360',
    'username': 'postgres',
    'password': 'postgres'
}

# Optional: Advanced settings
ADVANCED_CONFIG = {
    'chunk_size': 10000,  # Number of rows to process at once for large files
    'encoding': 'utf-8',  # CSV file encoding
    'create_schemas': True,  # Whether to create schemas automatically
}

# Schema mapping for different folders
FOLDER_SCHEMA_MAPPING = {
    'card': 'card_data',
    # 'crmuser': 'crmuser', 
    # 'custom': 'custom',
    'mobile_banking': 'mobile_banking',
    # 'tbaadm': 'tbaadm'
}
