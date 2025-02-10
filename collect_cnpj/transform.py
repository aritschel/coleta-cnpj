from utils.spark_session import write_to_jdbc
from utils.exec_db import load_from_postgres
from pyspark.sql.functions import count, max, col, when, lit
from pyspark.sql.window import Window


def main():
    """
    Main function to load, transform, and write data.
    """
    df = load_from_postgres("silver")
    df = transform_data(df)
    write_to_jdbc(df, "result")


def transform_data(df):
    """
    Apply transformations to the DataFrame.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The transformed DataFrame.
    """
    window_spec = Window.partitionBy("cnpj")
    df = df.withColumn("qtde_socios", count("socio_id").over(window_spec))
    df = df.withColumn(
        "flag_socio_estrangeiro",
        max(col("flag_socio_estrangeiro")).over(window_spec)
    )
    df = df.drop("socio_id")
    df = df.dropDuplicates()
    df = df.withColumn(
        "doc_alvo",
        when((col("cod_porte") == 3) & (col("qtde_socios") > 1),
             lit(True)).otherwise(lit(False))
    )
    df = df.drop("cod_porte")
    return df


if __name__ == "__main__":
    main()
