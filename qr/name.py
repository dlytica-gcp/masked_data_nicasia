import os
import glob

def generate_airbyte_names():
    """
    Generate names with 'airbyte_raw_' prefix based on existing CSV files
    """
    # Get the current directory
    current_dir = os.getcwd()
    print(f"Current directory: {current_dir}")
    print("\nGenerating names with 'airbyte_raw_' prefix:")
    print("-" * 60)
    
    # Find all CSV files in the directory
    csv_files = glob.glob("*.csv")
    
    # Generate names with airbyte_raw_ prefix
    airbyte_names = []
    
    if csv_files:
        csv_files.sort()  # Sort alphabetically
        for file in csv_files:
            # Remove .csv extension and add airbyte_raw_ prefix
            base_name = file.replace('.csv', '')
            airbyte_name = f"airbyte_raw_{base_name}"
            airbyte_names.append(airbyte_name)
            
        for i, name in enumerate(airbyte_names, 1):
            print(f"{i:2d}. {name}")
        print(f"\nTotal names generated: {len(airbyte_names)}")
    else:
        print("No CSV files found in the current directory.")
    
    return airbyte_names


def export_to_txt_file():
    """
    Export the generated airbyte_raw_ names to name.txt
    """
    # Find all CSV files in the directory
    csv_files = glob.glob("*.csv")
    
    try:
        with open("name.txt", "w") as txt_file:
            txt_file.write("Names with prefix 'airbyte_raw_'\n")
            txt_file.write("=" * 40 + "\n\n")
            
            if csv_files:
                csv_files.sort()  # Sort alphabetically
                
                for file in csv_files:
                    # Remove .csv extension and add airbyte_raw_ prefix
                    base_name = file.replace('.csv', '')
                    airbyte_name = f"airbyte_raw_{base_name}"
                    txt_file.write(f"- name: {airbyte_name}\n")
                
                txt_file.write(f"\nTotal names generated: {len(csv_files)}\n")
                
                # Add original file names for reference
                txt_file.write("\n" + "=" * 40 + "\n")
                txt_file.write("ORIGINAL CSV FILES\n")
                txt_file.write("=" * 40 + "\n\n")
                
                for i, file in enumerate(csv_files, 1):
                    txt_file.write(f"{i:2d}. {file}\n")
                    
            else:
                txt_file.write("No CSV files found in the directory\n")
        
        print(f"\nResults exported to 'name.txt' successfully!")
        
    except Exception as e:
        print(f"Error writing to file: {e}")

if __name__ == "__main__":
    # Run the functions
    generate_airbyte_names()
    export_to_txt_file()