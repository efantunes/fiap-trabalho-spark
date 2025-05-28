# from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,count,desc,col,udf
import os
import sys
from datetime import date,datetime

os.environ["PYSPARK_PYTHON"] = sys.executable

# Criar sessão do Spark
spark = SparkSession.builder.master("local[2]").appName("trabalho_final_udf").getOrCreate()

data = [
    ("Barbosa", "1990-05-14"),
    ("Roberto", "1985-07-23"),
    ("Charles", "1992-12-02"),
    ("Leandro", "1988-03-08"),
    ("Evanildo", "1995-10-30"),
    ("Francisco", "1991-08-19"),
    ("Graciane", "1987-01-11"),
    ("Heidson", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judite", "1994-09-17")
]
columns = ["nome", "data_nascimento"]
df = spark.createDataFrame(data, columns)
# Seu código aqui para definir e aplicar a UDF
@udf(T.StringType())
def calc_idade(nm_col,dt_nasc_col):
    today = date.today()
    dt_nasc_col_obj = datetime.strptime(dt_nasc_col, "%Y-%m-%d")
    age = today.year - dt_nasc_col_obj.year - ((today.month, today.day) < (dt_nasc_col_obj.month, dt_nasc_col_obj.day))
    return f"Olá, {nm_col}! Você tem {age} anos."

df_saudacao= df.withColumn("saudacao",calc_idade(col("nome"),col("data_nascimento")))
df_saudacao.show(truncate=False)

spark.stop()


