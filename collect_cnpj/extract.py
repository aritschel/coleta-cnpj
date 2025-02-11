from zipfile import ZipFile
import requests
import os
from utils.helpers import set_url, set_file_name, init_layer
from utils.exec_db import load_to_postgres

ZIP_PATH, CSV_PATH = init_layer("bronze")


def download_file(file_name: str) -> None:
    """
    Downloads a file from a generated URL.

    Args:
    file_name (str): The name of the file to be downloaded.
    """
    url = set_url(file_name)
    zip_file_path = os.path.join(ZIP_PATH, file_name)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(zip_file_path, "wb") as file:
            file.write(response.content)


def extract_files(file_name: str) -> None:
    """
    Extracts files from a ZIP archive to the bronze directory.

    Args:
    file_name (str): The name of the ZIP file to be extracted.
    """
    zip_file_path = os.path.join(ZIP_PATH, file_name)
    with ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(CSV_PATH)


def rename_extracted_file(data_base: str, file_index: str) -> str:
    """
    Renames the extracted CSV file to maintain a standard name.

    Args:
    data_base (str): The name of the database.
    file_index (str): The index of the file.

    Returns:
    str: The new name of the CSV file.
    """
    original_directory = os.getcwd()
    try:
        os.chdir(CSV_PATH)
        for file in os.listdir():
            if not file.endswith(".csv"):
                new_name = set_file_name(data_base, ".csv", file_index)
                os.rename(file, new_name)
    finally:
        os.chdir(original_directory)


def run_extraction_job() -> None:
    """
    Runs the extraction job for multiple databases.
    """
    data_bases = ["Socios", "Empresas"]
    for db in data_bases:
        for i in range(1):
            # zip_file = set_file_name(db, ".zip", str(i))
            csv_file = set_file_name(db, ".csv", str(i))
            # download_file(zip_file)
            # extract_files(zip_file)
            # rename_extracted_file(db, str(i))
            load_to_postgres(db, csv_file, CSV_PATH)


if __name__ == "__main__":
    run_extraction_job()
