from utils.schema import empresas_schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("empresas").getOrCreate()

empresas_df = spark.read.csv(
    path="/workspaces/coleta-cnpj/empresa/empresa.csv",
    schema=empresas_schema,
    header=False,
    sep=";", 
)

empresas_df.show()
