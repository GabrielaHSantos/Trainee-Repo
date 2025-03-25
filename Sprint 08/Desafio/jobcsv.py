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

# Caminho dos arquivos CSV na Raw Zone (somente filmes)
s3_input_filmes = "s3://data-lake-sprint6/Raw/Local/CSV/Filmes/"

# Definir o esquema manualmente para os arquivos de filmes
schema_filmes = StructType([
    StructField("id", StringType(), True),
    StructField("titulo", StringType(), True),  # Alterado para "titulo" para alinhar com TMDB
    StructField("titulo_original", StringType(), True),
    StructField("ano_lancamento", IntegerType(), True),
    StructField("duracao_minutos", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("nota_media", FloatType(), True),
    StructField("numero_votos", IntegerType(), True)
])

# Lendo os arquivos CSV com delimitador '|'
filmes_df = spark.read.option("header", "true").option("delimiter", "|").schema(schema_filmes).csv(s3_input_filmes + "*/*/*/*.csv")

# Removendo duplicatas
filmes_df = filmes_df.dropDuplicates()

# Normalizando espaços vazios e tratando valores nulos
for column in filmes_df.columns:
    filmes_df = filmes_df.withColumn(column, trim(col(column)))
    filmes_df = filmes_df.withColumn(column, when(col(column) == "", None).otherwise(col(column)))

# Normaliza o campo 'genero' (remove espaços extras e converte para minúsculas)
filmes_df = filmes_df.withColumn("genero", trim(lower(col("genero"))))

# Filtrando apenas animações
filmes_df = filmes_df.filter(col("genero").rlike("(?i)animation|animação"))

# Caminho de saída na Trusted Zone (somente filmes)
s3_output_filmes = "s3://data-lake-sprint6/Trusted/Local/Parquet/Filmes/"

# Salvando como PARQUET 
filmes_df.write.mode("overwrite").parquet(s3_output_filmes)

job.commit()
