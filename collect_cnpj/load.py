from utils.schema import empresas_schema, socios_schema
from utils.spark_session import init_spark
from utils.helpers import set_file_name
import pyspark.sql.functions as f

data_base = "Socios9"

data_type = ".csv"

file_name = set_file_name(data_base, data_type)


spark = init_spark(data_base)

socios_df = spark.read.csv(
    path=f"{data_base}/{file_name}",
    schema=socios_schema,
    header=False,
    sep=";", 
)

socios_df.filter(f.col("documento_socio")=='***999999**').show()
