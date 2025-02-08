
from zipfile import ZipFile
import requests
import os
from utils.helpers import set_url, set_file_name, load_config


def download_file(data_date: str, file_name: str) -> None:
    """Downloads a file from a generated URL."""
    url = set_url(data_date, file_name)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(file_name, "wb") as file:
            file.write(response.content)

def extract_files(file_name: str, data_base: str) -> None:
    """Extracts files from a ZIP archive to a specific directory."""
    with ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(data_base)

def rename_extracted_file(data_base: str) -> None:
    """Renames the file found in the directory to a new name."""
    original_directory = os.getcwd()
    data_type=".csv"
    new_file_name = set_file_name(data_base, data_type)
    try:
        os.chdir(data_base)
        for file in os.listdir():
            os.rename(file, new_file_name)
    finally:
        os.chdir(original_directory)

def run_extraction_job() -> None:
    """Runs the extraction job based on configuration."""
    config = load_config("collect_cnpj/extract/")
    data_date = config["data_date"]
    base_folders = config["base_folders"]
    
    for base_folder in base_folders:
        zip_file = set_file_name(base_folder, ".zip")
        url = set_url(data_date, zip_file)
        
        print(f"Downloading {zip_file}...")
        download_file(url, zip_file)
        
        print(f"Extracting {zip_file} to {base_folder}...")
        extract_files(zip_file, base_folder)

        print(f"Renaming {base_folder}...")
        rename_extracted_file(base_folder)
    
    print("Extraction job completed successfully.")


if __name__ == "__main__":
    run_extraction_job()

