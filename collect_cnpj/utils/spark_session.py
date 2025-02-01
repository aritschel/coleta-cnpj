from pyspark.sql import SparkSession

def init_spark(session_name: str):
    return SparkSession.builder.appName(session_name).getOrCreate()
