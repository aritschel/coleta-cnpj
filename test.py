from pyspark.sql import SparkSession

# Criando uma sessão Spark
spark = SparkSession.builder.appName("Exemplo").getOrCreate()

# Criando um DataFrame simples
data = [("Alice", 34), ("Bob", 45), ("Charlie", 25)]
df = spark.createDataFrame(data, ["Nome", "Idade"])

# Mostrando os dados
df.show()

# Parando a sessão Spark
spark.stop()
