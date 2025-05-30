from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, date_trunc
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-aggregations") \
    .getOrCreate()

# Carregando o dataset de pedidos
df = spark.read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/*.csv.gz", inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Desafio AGREGAÇAO 1: 
#   Agrupando pelos campo UF, calcule a soma das QUANTIDADES dos produtos nos pedidos
df_agg1 = (
    df.groupBy(col('uf')) 
    .agg(
        sum(col('quantidade')).alias('quantidade')
    ) 
)

print("Resultado do desafio de agregacao 1")
df_agg1.show(truncate=False)

# Desafio AGREGAÇAO 2: 
#   Agrupe pelo atributo PRODUTO, calcule a soma do valor total dos pedidos
#   Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario 
# dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.

# Incluindo a nova coluna de valor total do pedido
# df = df.withColumn("VALOR_TOTAL_PEDIDO", _______)

df_agg2 = df.groupBy(col('produto')) \
            .agg(sum(col('valor_unitario')*col('quantidade')).alias("valor_total")) \
            .orderBy(col("valor_total").desc())

print("Resultado do desafio de agregacao 2")
df_agg2.show(truncate=False)

# Desafio AGREGAÇAO 3: Agrupe pela DATA DO PEDIDO e calcule a soma do valor total 
# dos pedidos
# Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario 
# dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.
#   O atributo DATA_CRIACAO possui hora, minuto e segundo. Utilize a funcao date_trunc 
# para truncar o valor.

# Incluindo a nova coluna de data truncada
df = df.withColumn("DATA_PEDIDO", date_trunc('day',col('data_criacao')))

# Incluindo a nova coluna de valor total do pedido
# df = df.withColumn("VALOR_TOTAL_PEDIDO", _______)

df_agg3 = df.groupBy('DATA_PEDIDO') \
            .agg(sum(col('valor_unitario')*col('quantidade')).alias("valor_total")) \
            .orderBy(col("DATA_PEDIDO").asc()) 

print("Resultado do desafio de agregacao 3")
df_agg3.show(truncate=False)

# Parar a Spark Session
spark.stop()
