
from zipfile import ZipFile
import requests
import os
from utils.helpers import set_url, set_file_name


data_date = "2024-11/"

data_base = "Socios9"
data_type = ".zip"
file_name = set_file_name(data_base, data_type)

def download_file(data_date: str, file_name: str):
    url = set_url(data_date, file_name)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(file_name, "wb") as file:
            file.write(response.content)

def extract_files(file_name: str, data_base: str):
    zip = ZipFile(file_name)
    zip.extractall(data_base)
    zip.close()


def change_file_name(data_base: str):
    diretorio_original = os.getcwd()
    data_type=".csv"
    file_name = set_file_name(data_base, data_type)
    try:
        os.chdir(data_base)
        for arquivo in os.listdir():
            os.rename(arquivo, file_name)
            break
    finally:
        os.chdir(diretorio_original)

# print(url)
#extract_files(data_type, data_base, file_name)

change_file_name(data_base)
# diretorio_original = os.getcwd()

# change_file_name()