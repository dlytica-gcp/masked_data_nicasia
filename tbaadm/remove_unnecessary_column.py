import os
import pandas as pd


base_dir = "tbaadm"
columns_to_drop = ['_airbyte_ab_id', '_airbyte_emitted_at', 'source_file_path', '_airbyte_additional_properties']


for subdir in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, subdir)
    
    
    if os.path.isdir(folder_path):
        
        for file in os.listdir(folder_path):
            if file.endswith(".xlsx"):
                file_path = os.path.join(folder_path, file)
                try:
                    
                    df = pd.read_excel(file_path)

                    
                    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

                   
                    df.to_excel(file_path, index=False)

                    print(f"Cleaned and updated: {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
