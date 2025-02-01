def set_file_name(data_base: str, data_type: str) -> str:
    return data_base+data_type

def set_url(endpoint: str, data_date: str, file_name: str) -> str:
    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    return endpoint+data_date+file_name