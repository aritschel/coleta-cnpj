from pyspark.sql import SparkSession

def init_spark(app_name: str) -> SparkSession:
    """
    Initialize Spark session with specific configurations.

    Args:
    app_name (str): Name of the Spark application.

    Returns:
    SparkSession: Initialized Spark session.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "/coleta-cnpj/repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.1.0/duckdb_jdbc-1.1.0.jar") \
        .appName(app_name) \
        .getOrCreate()

    spark.conf.set("spark.sql.sources.commitProtocolClass", 
                   "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    
    return spark