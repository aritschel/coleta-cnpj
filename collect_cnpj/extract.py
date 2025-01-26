
import zipfile
import requests
import os


endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

data_date = "2024-11/"

data_name = "Empresas9"

data_type = ".zip"

url = f"{endpoint}{data_date}{data_name}{data_type}"

response = requests.get(url, stream=True)

if response.status_code == 200:
    with open(f"{data_type}", "wb") as file:
        file.write(response.content)

def extract_files():
    zip = zipfile.ZipFile(f"{data_type}")
    zip.extractall(f"empresa")
    zip.close()        


extract_files()

diretorio_original = os.getcwd()

def change_file_name():
    try:
        os.chdir("empresa")
        for arquivo in os.listdir():
            os.rename(arquivo, "empresa.csv")
            break
    finally:
        os.chdir(diretorio_original)

