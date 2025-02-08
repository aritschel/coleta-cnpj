from utils.schema import silver_schema, result_schema
from utils.spark_session import init_spark
from pyspark.sql.functions import count, max, col, when, lit
from pyspark.sql.window import Window

def main():
    spark = init_spark("transform")
    df = load_silver_data(spark)
    df = transform_data(df)
    write_gold_data(df)

def load_silver_data(spark):
    """Load data from the silver layer with the specified schema."""
    return spark.read.schema(silver_schema).parquet("silver/")

def transform_data(df):
    """Apply transformations to the DataFrame."""
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
        when((col("cod_porte") == 3) & (col("qtde_socios") > 1), lit(True)).otherwise(lit(False))
    )
    
    return df

def write_gold_data(df):
    """Write the transformed data to the gold layer."""
    df.write.option("schema", result_schema).mode("overwrite").parquet("gold/")

if __name__ == "__main__":
    main()