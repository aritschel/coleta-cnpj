from pyspark.sql import SparkSession, DataFrame
import yaml
import duckdb
import os
from typing import Dict


def set_file_name(data_base: str,
                  data_type: str,
                  data_value: str = "8") -> str:
    """Generate a standardized file name."""
    return f"{data_base}{data_value}{data_type}"


def set_url(file_name: str,
            data_date: str = "2024-11/") -> str:
    """Constructs the URL for downloading files."""
    BASE_URL = (
        "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    )
    return f"{BASE_URL}{data_date}{file_name}"


def get_csv_data(spark_session: SparkSession,
                 csv_path: str,
                 csv_schema: list) -> DataFrame:
    """Loads CSV data into a Spark DataFrame."""
    return spark_session.read.csv(
        path=csv_path,
        schema=csv_schema,
        header=False,
        sep=";"
    )


def write_parquet(df: DataFrame,
                  parquet_path: str) -> None:
    """Writes a DataFrame to a Parquet file."""
    df.write.mode("overwrite").parquet(path=parquet_path)


def get_data_path(data_base: str) -> str:
    """Returns the standardized data path for a given base."""
    return os.path.join(data_base, f"{data_base}.csv")


def load_config(path: str) -> Dict:
    """Loads configuration settings from a YAML file."""
    config_file = os.path.join(path, "config.yaml")
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def load_to_duckdb(data_base: str,
                   file_type: str,
                   file_name: str) -> None:
    """Loads multiple CSV files into a single DuckDB table."""
    BRONZE_PATH = "bronze"
    DUCKDB_FILE = os.path.join(BRONZE_PATH, "database.duckdb")

    connection = duckdb.connect(database=DUCKDB_FILE)
    table_name = data_base.lower()

    try:

        file_path = os.path.join("bronze/csv/", file_name)

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        FROM read_csv_auto('{file_path}', encoding='latin-1',
        strict_mode=false)
        """
        connection.execute(query)

        print(f"Data from {data_base} loaded into DuckDB table {table_name}")

    except Exception as e:
        print(f"Error loading {data_base} into DuckDB: {e}")
    finally:
        connection.close()


def define_paths(layer: str,
                 folder_name: str) -> str:
    """Defines the paths for ZIP and CSV files."""
    return os.path.join(layer, folder_name)


def init_layer(layer: str) -> None:
    """Initializes the directories for ZIP and CSV files."""
    if layer == "bronze":
        ZIP_PATH = define_paths(layer, "zip")
        CSV_PATH = define_paths(layer, "csv")
        os.makedirs(ZIP_PATH, exist_ok=True)
        os.makedirs(CSV_PATH, exist_ok=True)
        return ZIP_PATH, CSV_PATH


# def load_from_duckdb(spark: SparkSession,
#                      table_name: str) -> DataFrame:
#     """Lê os dados do DuckDB diretamente no Spark."""
#     return spark.read \
#         .format("jdbc") \
#         .option("url", "jdbc:duckdb:/bronze/database.duckdb") \
#         .option("dbtable", "empresas") \
#         .option("driver", "org.duckdb.DuckDBDriver") \
#         .load()


# def write_to_duckdb(df: DataFrame, table_name: str) -> None:
#     """Escreve os dados transformados no DuckDB (Silver)
#     diretamente no Spark."""
#     df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:duckdb:bronze/database.duckdb") \
#         .option("dbtable", table_name) \
#         .option("driver", "org.duckdb.DuckDBDriver") \
#         .mode("overwrite") \
#         .save()


