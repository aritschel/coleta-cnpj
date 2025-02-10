from pyspark.sql import SparkSession
import os


jdbc_url = os.getenv(
    "JDBC_URL", "jdbc:postgresql://localhost:5432/sparkdb"
)
properties = {
    "user": os.getenv("DB_USER", "spark"),
    "password": os.getenv("DB_PASSWORD", "spark"),
    "driver": "org.postgresql.Driver"
}


def init_spark(app_name: str) -> SparkSession:
    """
    Initialize Spark session with specific configurations.

    Args:
    app_name (str): Name of the Spark application.

    Returns:
    SparkSession: Initialized Spark session.
    """
    spark = (SparkSession.builder
             .config("spark.jars", ".jars/postgresql-42.5.0.jar")
             .config("spark.driver.memory", "30g")
             .config("spark.executor.memory", "30g")
             .config("spark.num.executors", "30")
             .appName(app_name)
             .getOrCreate())
    spark.conf.set(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources."
        "SQLHadoopMapReduceCommitProtocol"
    )
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


def write_to_jdbc(df, data_base: str, partition_column: str = None) -> None:
    """
    Write a Spark DataFrame to a PostgreSQL table.

    Args:
    df (DataFrame): The Spark DataFrame to be written.
    data_base (str): The name of the PostgreSQL database table.
    """
    writer = df.write.jdbc(
        url=jdbc_url, table=f"public.{data_base}", mode="append",
        properties=properties
    )

    if partition_column:
        writer = writer.partitionBy(partition_column)

    writer.save()


def read_from_jdbc(query: str) -> None:
    """
    Read data from a PostgreSQL table into a Spark DataFrame.

    Args:
    query (str): The SQL query to execute.

    Returns:
    DataFrame: The resulting Spark DataFrame.
    """
    spark = init_spark("ReadFromPostgres")
    jdbc_url = os.getenv(
        "JDBC_URL", "jdbc:postgresql://localhost:5432/sparkdb"
    )
    properties = {
        "user": os.getenv("DB_USER", "spark"),
        "password": os.getenv("DB_PASSWORD", "spark"),
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({query}) as query",
        properties=properties
    )
    return df
