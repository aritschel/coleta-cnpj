from utils.spark_session import write_to_jdbc
from utils.exec_db import load_from_postgres
from pyspark.sql.functions import when, col, lit, concat, regexp_replace


def main():
    """
    Main function to load, transform, and join data from PostgreSQL.
    """

    socios_df = load_from_postgres("socios")
    empresas_df = load_from_postgres("empresas")

    socios_df = transform_socios_data(socios_df)
    empresas_df = transform_empresas_data(empresas_df)

    df = join_data(empresas_df, socios_df)

    write_to_jdbc(df, "silver")


def transform_socios_data(socios_df):
    """
    Apply transformations to the socios DataFrame.

    Args:
    socios_df (DataFrame): The socios DataFrame.

    Returns:
    DataFrame: The transformed socios DataFrame.
    """
    socios_df = socios_df.withColumn(
        "flag_socio_estrangeiro",
        when(col("documento_socio") == '***999999**', lit(True)).otherwise(
            lit(False))
    )
    socios_df = socios_df.withColumn("socio_id", concat(
        col("documento_socio"), col("nome_socio")))
    socios_df = socios_df.withColumn(
        "socio_id", regexp_replace(col("socio_id"), "\\*", ""))

    return socios_df.drop("cod_qualificacao_socio", "tipo_socio",
                          "documento_socio", "nome_socio")


def transform_empresas_data(empresas_df):
    """
    Apply transformations to the empresas DataFrame.

    Args:
    empresas_df (DataFrame): The empresas DataFrame.

    Returns:
    DataFrame: The transformed empresas DataFrame.
    """
    empresas_df = empresas_df.withColumn("cod_porte",
                                         col("cod_porte").cast("integer"))
    return empresas_df.drop("natureza_juridica", "qualificacao_responsavel",
                            "capital_social", "razao_social")


def join_data(empresas_df, socios_df):
    """
    Join the empresas and socios DataFrames on the 'cnpj' column.

    Args:
    empresas_df (DataFrame): The empresas DataFrame.
    socios_df (DataFrame): The socios DataFrame.

    Returns:
    DataFrame: The joined DataFrame.
    """
    return empresas_df.join(socios_df, "cnpj", "inner")


if __name__ == "__main__":
    main()
