from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

empresas_schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificacao_responsavel", IntegerType(), True),
    StructField("capital_social", StringType(), True),
    StructField("cod_porte", StringType(), True)
])

socios_schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("tipo_socio", IntegerType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("documento_socio", StringType(), True),
    StructField("cod_qualificacao_socio", StringType(), True)
])