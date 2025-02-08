from pyspark.sql import DataFrame
import yaml


def set_file_name(data_base: str, data_type: str) -> str:
    return data_base+data_type

def set_url(data_date: str, file_name: str) -> str:
    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    return endpoint+data_date+file_name

def get_csv_data(spark_session, csv_path: str, csv_schema: list) -> DataFrame:
    df = spark_session.read.csv(
    path=csv_path,
    schema=csv_schema,
    header=False,
    sep=";", 
)
    return df

def set_parquet_write(df: DataFrame, parquet_path: str):
    return df.write.mode("overwrite").parquet(path=parquet_path)

def set_data_path(data_base):
   return f"{data_base}/{data_base}.csv"

def load_config(path) -> dict:
    """Loads configuration from a YAML file."""
    with open(f"{path}config.yaml", "r") as file:
        return yaml.safe_load(file)