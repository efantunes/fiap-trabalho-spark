from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-rollup-cube") \
    .getOrCreate()

# Carregando o dataset pedidos
df = spark.read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/*.csv.gz", inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Desafio ROLLUP: Agrupando pelos campos UF e PRODUTO, calcule a soma das 
# QUANTIDADES dos produtos nos pedidos
df_rollup = df.rollup(col('uf'),col('produto')) \
                .agg(sum(col('quantidade')).alias('quantidade')) \
                .orderBy('uf','produto')

# Mostrando os resultados parciais do rollup
df_rollup.show(truncate=False)

# # Desafio CUBE: Agrupe pela HORA e UF do pedido e calcule a soma do valor total 
# dos pedidos
# # Atenção! 
# # 1. O dataset nao possui valor total do pedido, apenas quantidade e valor 
# unitario dos produtos. Sera necessario criar uma nova coluna de valor total 
# calculado.
# # 2. A coluna DATA_CRIACAO possui hora. Utilize a funcao "hour" do pacote
#  pyspark.sql.functions.
# # 3. Filtre apenas os estados SP, RJ e MG.

# # Incluindo a nova coluna de data
df = df.withColumn("VALOR_TOTAL_PEDIDO", col('valor_unitario')*col('quantidade'))
df = df.withColumn("HORA_PEDIDO", hour(col('data_criacao')))

df_cube = df.filter( col("uf").isin(['SP', 'RJ', 'MG']) ) \
            .cube("HORA_PEDIDO", "UF") \
            .agg(sum("VALOR_TOTAL_PEDIDO").alias("VALOR_TOTAL_PEDIDO")) \
            .orderBy("HORA_PEDIDO", "UF")

# # Mostrando os resultados parciais do cube
df_cube.show(truncate=False)

# # Parar a Spark Session
spark.stop()
