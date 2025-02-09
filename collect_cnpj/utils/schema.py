from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

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

result_schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("qtde_socios", IntegerType(), True),
    StructField("flag_socio_estrangeiro", BooleanType(), True),
    StructField("doc_alvo", BooleanType(), True),
])

silver_schema = StructType([
    StructField("cnpj", StringType(), True),
    StructField("cod_porte", IntegerType(), True),
    StructField("flag_socio_estrangeiro", BooleanType(), False),
    StructField("socio_id", StringType(), True)
])


def get_schema(schema_name: str) -> StructType:
    """
    Returns the schema based on the schema name.

    Args:
    schema_name (str): The name of the schema.

    Returns:
    StructType: The corresponding schema.
    """
    schemas = {
        "empresas": empresas_schema,
        "socios": socios_schema,
        "result": result_schema,
        "silver": silver_schema
    }
    return schemas.get(schema_name, None)
