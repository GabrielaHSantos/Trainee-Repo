import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Inicializa o contexto do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminho dos arquivos CSV na Raw Zone
s3_input_filmes = "s3://data-lake-sprint6/Raw/Local/CSV/Filmes/"
s3_input_series = "s3://data-lake-sprint6/Raw/Local/CSV/Series/"

# Definindo o esquema manualmente para os arquivos CSV
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("tituloPincipal", StringType(), True),
    StructField("tituloOriginal", StringType(), True),
    StructField("anoLancamento", IntegerType(), True),
    StructField("tempoMinutos", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("notaMedia", FloatType(), True),
    StructField("numeroVotos", IntegerType(), True)
])

# Lendo os arquivos CSV com esquema definido
filmes_df = spark.read.option("header", "true").schema(schema).csv(s3_input_filmes)
series_df = spark.read.option("header", "true").schema(schema).csv(s3_input_series)

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

# Caminhos de saída na Trusted Zone
s3_output_filmes = "s3://data-lake-sprint6/Trusted/Local/Parquet/Filmes/"
s3_output_series = "s3://data-lake-sprint6/Trusted/Local/Parquet/Series/"

# Salvando como PARQUET 
filmes_df.write.mode("overwrite").parquet(s3_output_filmes)
series_df.write.mode("overwrite").parquet(s3_output_series)

job.commit()
