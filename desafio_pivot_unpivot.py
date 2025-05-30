from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col,expr
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable

# Inicializando a sessão do Spark
spark = SparkSession.builder.appName("dataeng-pivot").getOrCreate()

data = [
    ("Eletrônicos", 2022, 1000),
    ("Eletrônicos", 2023, 1500),
    ("Eletrônicos", 2024, 400),
    ("Móveis", 2022, 700),
    ("Móveis", 2023, 800),
    ("Móveis", 2024, 200),
    ("Vestuário", 2022, 500),
    ("Vestuário", 2023, 600),
    ("Vestuário", 2024, 300)
]

# Definir as colunas do DataFrame
columns = ["categoria_produto", "ano", "vendas"]

# Criando o DataFrame com os dados fornecidos
df = spark.createDataFrame(data, columns)

# Utilizando a função `pivot`, faça a transposição dos dados e 
# obtenha a `soma` das vendas por `ano` agrupando pelo atributo `categoria_produto`.
df_pivot = df.groupBy(col('categoria_produto')).pivot('ano').agg(sum(col('vendas')))

print("Questão 01 - Pivot")
print("DataFrame Original")
df.show()
# Mostrar o resultado
print("Após Pivot:")
df_pivot.show()

# Dados de exemplo em um DataFrame
data = [
    ("GELADEIRA" , 100, 200, 300, 200),
    ("TELEVISAO" , 400, 500, 600, 400),
    ("COMPUTADOR", 700, 800, 900, 950)
]

columns = ["Produto", "Q1", "Q2", "Q3", "Q4"]

# Criar DataFrame
df = spark.createDataFrame(data, columns)
print("Questão 02 - Unpivot")
# Exibir DataFrame original
print("DataFrame Original:")
df.show()

# Usar selectExpr para simular unpivot (melt)
unpivot_expr = """
stack(4,'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4)
"""

# Realizando unpivot
df_unpivot = df.select("Produto", expr(unpivot_expr))

# Exibir DataFrame após unpivot
print("DataFrame Após Unpivot:")
df_unpivot.show()

spark.stop()