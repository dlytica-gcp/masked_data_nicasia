import os
import shutil

# Set your target directory where the Excel files are located
directory = r"qr"  

# Loop through all files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".xlsx"):
        file_path = os.path.join(directory, filename)

        # Remove file extension to get folder name
        folder_name = filename.rsplit(".", 1)[0]
        folder_path = os.path.join(directory, folder_name)

        # Create the folder if it doesn't exist
        os.makedirs(folder_path, exist_ok=True)

        # Move the file into the new folder
        new_file_path = os.path.join(folder_path, filename)
        shutil.move(file_path, new_file_path)

        print(f"Moved '{filename}' to '{folder_path}'")
