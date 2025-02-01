
from zipfile import ZipFile
import requests
import os
from utils.helpers import set_url, set_file_name


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

def main() -> None:
    """Orchestrates the process of downloading, extracting, and renaming files."""
    data_date = "2024-11/"
    data_base = "Socios9"
    data_type = ".zip"
    file_name = set_file_name(data_base, data_type)
    download_file(data_date, file_name)
    extract_files(file_name, data_base)
    rename_extracted_file(data_base)

if __name__ == "__main__":
    main()