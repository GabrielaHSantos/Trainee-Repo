import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, trim, when

# Inicializa o contexto do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminho correto dos arquivos CSV na Raw Zone (ajustado para pegar todas as datas)
s3_input_filmes = "s3://data-lake-sprint6/Raw/Local/CSV/Filmes/"
s3_input_series = "s3://data-lake-sprint6/Raw/Local/CSV/Series/"

# Definir o esquema manualmente para os arquivos CSV
schema = StructType([
    StructField("id", StringType(), True),  # Alterado para StringType para evitar problemas com IDs
    StructField("tituloPincipal", StringType(), True),
    StructField("tituloOriginal", StringType(), True),
    StructField("anoLancamento", IntegerType(), True),
    StructField("tempoMinutos", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("notaMedia", FloatType(), True),
    StructField("numeroVotos", IntegerType(), True)
])

# Lendo os arquivos CSV dentro de todas as subpastas com delimitador '|'
filmes_df = spark.read.option("header", "true").option("delimiter", "|").schema(schema).csv(s3_input_filmes + "*/*/*/*.csv")
series_df = spark.read.option("header", "true").option("delimiter", "|").schema(schema).csv(s3_input_series + "*/*/*/*.csv")

# Verificando os dados lidos
print("Dados de Filmes (antes da limpeza):")
filmes_df.show(5, truncate=False)
print("Dados de Séries (antes da limpeza):")
series_df.show(5, truncate=False)

# Padronizando os nomes das colunas
columns_rename = {
    "id": "id",
    "tituloPincipal": "titulo_principal",
    "tituloOriginal": "titulo_original",
    "anoLancamento": "ano_lancamento",
    "tempoMinutos": "duracao_minutos",
    "genero": "genero",
    "notaMedia": "nota_media",
    "numeroVotos": "numero_votos"
}

filmes_df = filmes_df.selectExpr([f"`{col}` as `{new_col}`" for col, new_col in columns_rename.items()])
series_df = series_df.selectExpr([f"`{col}` as `{new_col}`" for col, new_col in columns_rename.items()])

# Removendo duplicatas
filmes_df = filmes_df.dropDuplicates()
series_df = series_df.dropDuplicates()

# Normalizando espaços vazios e tratando valores nulos
for column in filmes_df.columns:
    filmes_df = filmes_df.withColumn(column, trim(col(column)))  # Remove espaços em branco
    filmes_df = filmes_df.withColumn(column, when(col(column) == "", None).otherwise(col(column)))  # Substitui strings vazias por None

for column in series_df.columns:
    series_df = series_df.withColumn(column, trim(col(column)))  
    series_df = series_df.withColumn(column, when(col(column) == "", None).otherwise(col(column)))  

# Verificando os dados após a limpeza
print("Dados de Filmes (após a limpeza):")
filmes_df.show(5, truncate=False)
print("Dados de Séries (após a limpeza):")
series_df.show(5, truncate=False)

# Normaliza o campo 'genero' (remove espaços extras e converte para minúsculas)
filmes_df = filmes_df.withColumn("genero", trim(lower(col("genero"))))
series_df = series_df.withColumn("genero", trim(lower(col("genero"))))

# Filtrando apenas animações (com variações de "Animation" e "Animação")
filmes_df = filmes_df.filter(col("genero").rlike("(?i)animation|animação"))
series_df = series_df.filter(col("genero").rlike("(?i)animation|animação"))

# Verificando os dados filtrados
print("Dados de Filmes após filtro:")
filmes_df.show(5, truncate=False)
print("Dados de Séries após filtro:")
series_df.show(5, truncate=False)

# Caminhos de saída na Trusted Zone
s3_output_filmes = "s3://data-lake-sprint6/Trusted/Local/Parquet/Filmes/"
s3_output_series = "s3://data-lake-sprint6/Trusted/Local/Parquet/Series/"

# Salvando como PARQUET 
filmes_df.write.mode("overwrite").parquet(s3_output_filmes)
series_df.write.mode("overwrite").parquet(s3_output_series)

job.commit()