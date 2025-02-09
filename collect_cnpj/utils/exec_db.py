import os
from utils.spark_session import init_spark, write_to_jdbc, read_from_jdbc
from utils.schema import get_schema


def load_to_postgres(data_base: str, file_name: str, layer: str) -> None:
    """
    Load a CSV file into a PostgreSQL table using Spark DataFrame.

    Args:
    data_base (str): The name of the PostgreSQL database table.
    file_name (str): The name of the CSV file to be loaded.
    schema_name (str): The name of the schema to be used for the CSV file.
    """
    csv_path = os.path.join(layer, file_name)
    spark = init_spark("LoadToPostgres")
    schema = get_schema(data_base)
    df = spark.read.csv(csv_path, header=False, schema=schema, sep=";")
    write_to_jdbc(df, data_base)


def load_from_postgres(query: str) -> None:
    """
    Load data from a PostgreSQL table into a Spark DataFrame.

    Args:
    query (str): The SQL query to execute.
    """
    df = read_from_jdbc(query)
    df.show()
