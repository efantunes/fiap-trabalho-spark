import logging
logging.basicConfig(level=logging.INFO)
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,year,lit,count,desc,col


# Criar sessão do Spark
spark = SparkSession.builder.appName("trabalho_final").getOrCreate()

df_clientes = (
    spark.read
    .format("csv")
    .option("sep",';')
    .option("header",'true')
    .option("compression",'gzip')
    .load(
        r"datasets-csv-clientes\clientes.csv.gz"
    )
)

# Filtrar clientes com mais de 50 anos
df_clientes.withColumn("IDADE",(lit(2025) - year("data_nasc")) )
df_agrupado = (
    df_clientes.filter((lit(2025) - year("data_nasc")) >50)
    .groupBy(year("data_nasc"))
    .agg(count('*').alias('quantidade'))
    .orderBy(desc(col('quantidade')))
)

df_agrupado.show(truncate=False)

spark.stop()