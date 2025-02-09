from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

empresas_schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificacao_responsavel", IntegerType(), True),
    StructField("capital_social", StringType(), True),
    StructField("cod_porte", StringType(), True)
])
# Criando uma sessão Spark
spark = SparkSession.builder.appName("Exemplo").getOrCreate()

# Criando um DataFrame simples
df = spark.read.csv("bronze/csv/Empresas9.csv", schema=empresas_schema, header=False, sep=';')

# Mostrando os dados
df.show()

# Parando a sessão Spark

