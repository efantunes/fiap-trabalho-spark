from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

# Carregar o dataset JSON
caminho_arquivo = "desafio_arrays_structs_data.json"
dados_clientes = spark.read.option("multiLine","true").json(caminho_arquivo)

# Mostrar o schema do DataFrame
dados_clientes.printSchema()

# Mostrar os dados
dados_clientes.show(truncate=False)

# Escreva sua logica aqui
desaafio_01 = dados_clientes.select(
    col("nome"),
    col("idade"),
    col("notas.matematica").alias("matematica"),
    col("notas.portugues").alias("portugues"),
    col("notas.ciencias").alias("ciencias")
)

desaafio_02 = dados_clientes.withColumn(
    "exploded",explode("contatos")
    ).select(
    col("nome"),
    col("idade"),
    col("exploded.tipo").alias("tipo"),
    col("exploded.valor").alias("valor"),
)
desaafio_03 = dados_clientes.withColumn(
    "exploded",explode("interesses")
    ).select(
    col("nome"),
    col("idade"),
    col("exploded").alias("interesse")
)

# Apresente o resultado aqui
desaafio_01.show(100,truncate=False)
desaafio_02.show(100,truncate=False)
desaafio_03.show(100,truncate=False)