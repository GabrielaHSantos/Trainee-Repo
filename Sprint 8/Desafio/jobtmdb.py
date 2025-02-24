import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, array_contains, lit, when
from datetime import datetime

# Inicializa o contexto do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminho de entrada na Raw Zone
s3_input_path = "s3://data-lake-sprint6/Raw/TMDB/JSON/2025/02/23/"

# Extrai ano, mês e dia do caminho de entrada 
ano, mes, dia = s3_input_path.strip("/").split("/")[-3:]

# Lendo os arquivos JSON
tmdb_df = spark.read.option("multiline", "true").json(s3_input_path)

# Verificando se o campo genre_ids existe e filtra apenas animações (gênero 16)
if "genre_ids" in tmdb_df.columns:
    tmdb_df = tmdb_df.filter(array_contains(col("genre_ids"), 16))
else:
    raise ValueError("O campo 'genre_ids' não foi encontrado no DataFrame.")

# Padonizando os nomes das colunas
tmdb_df = tmdb_df.select(
    col("id").alias("id"),
    when(col("tipo_conteudo") == "tv", col("name")).otherwise(col("title")).alias("titulo"),  
    when(col("tipo_conteudo") == "tv", col("original_name")).otherwise(col("original_title")).alias("titulo_original"),  
    when(col("tipo_conteudo") == "tv", col("first_air_date")).otherwise(col("release_date")).alias("data_lancamento"),  
    col("popularity").alias("popularidade"),
    col("vote_average").alias("nota_media"),
    col("vote_count").alias("numero_votos"),
    col("genre_ids").alias("generos"),
    col("budget").alias("orcamento"),
    col("revenue").alias("receita"),
    col("tipo_conteudo").alias("tipo_conteudo")  
)

# Tratando os valores nulos 
tmdb_df = tmdb_df.fillna({"orcamento": 0, "receita": 0})

# Adiciona colunas de partição (ano_ingestao, mes_ingestao, dia_ingestao) e usa a data de ingestão (ano, mes, dia extraídos do caminho de entrada) como valores padrão
tmdb_df = tmdb_df.withColumn("ano_ingestao", lit(ano).cast("int")) \
                 .withColumn("mes_ingestao", lit(mes).cast("int")) \
                 .withColumn("dia_ingestao", lit(dia).cast("int"))

# Caminho de saída na Trusted Zone para Filmes
s3_output_filmes_path = "s3://data-lake-sprint6/Trusted/TMDB/Parquet/Movies/"

# Caminho de saída na Trusted Zone para Séries
s3_output_series_path = "s3://data-lake-sprint6/Trusted/TMDB/Parquet/Series/"

# Filtra e salva os dados de filmes
filmes_df = tmdb_df.filter(col("tipo_conteudo") == "movie")
filmes_df.write.mode("overwrite") \
    .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao") \
    .parquet(s3_output_filmes_path)

# Filtra e salva os dados de séries
series_df = tmdb_df.filter(col("tipo_conteudo") == "tv")
series_df.write.mode("overwrite") \
    .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao") \
    .parquet(s3_output_series_path)

job.commit()