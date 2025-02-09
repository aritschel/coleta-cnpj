from utils.schema import empresas_schema, socios_schema, silver_schema
from utils.spark_session import init_spark
from utils.helpers import load_from_duckdb
from pyspark.sql.functions import when, col, lit, concat, regexp_replace


def main():

    spark = init_spark("load")
    spark
    socios_df = load_from_duckdb(spark, "socios")
    # # empresas_df = load_from_duckdb(spark, "empresas", empresas_schema)

    socios_df.show()

    # socios_df = transform_socios_data(socios_df)
    # empresas_df = transform_empresas_data(empresas_df)

    # df = join_data(empresas_df, socios_df)

    # df.write.option("schema", silver_schema).mode(
    #     "overwrite").parquet("silver/")


def transform_socios_data(socios_df):
    """Apply transformations to the socios DataFrame."""
    socios_df = socios_df.withColumn(
        "flag_socio_estrangeiro",
        when(col("documento_socio") == '***999999**', lit(
            True)).otherwise(lit(False))
    )
    socios_df = socios_df.withColumn("socio_id", concat(
        col("documento_socio"), col("nome_socio")))
    socios_df = socios_df.withColumn(
        "socio_id", regexp_replace(col("socio_id"), "\\*", ""))

    return socios_df.drop("cod_qualificacao_socio",
                          "tipo_socio", "documento_socio", "nome_socio")


def transform_empresas_data(empresas_df):
    """Apply transformations to the empresas DataFrame."""
    empresas_df = empresas_df.withColumn("cod_porte",
                                         col("cod_porte").cast("integer"))
    return empresas_df.drop("natureza_juridica", "qualificacao_responsavel",
                            "capital_social", "razao_social")


def join_data(empresas_df, socios_df):
    """Join the empresas and socios DataFrames on the 'cnpj' column."""
    return empresas_df.join(socios_df, "cnpj", "inner")


if __name__ == "__main__":
    main()
