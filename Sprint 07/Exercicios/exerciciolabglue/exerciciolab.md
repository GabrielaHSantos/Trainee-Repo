# Processamento de Dados com AWS Glue

## Objetivo

O objetivo deste job é processar um arquivo CSV contendo registros de nomes e suas respectivas frequências, armazenado no Amazon S3, utilizando AWS Glue.

---

## 1.0 Criação do Job no AWS Glue

Primeiramente, acessei o console do AWS Glue e criei um novo job chamado **job_aws_glue_lab_4**. Configurei a origem dos dados para o bucket S3 contendo o arquivo **nomes.csv** e defini os seguintes parâmetros:

- **Nome do Job**: job_aws_glue_lab_4
- **Nome do Crawler**: FrequenciaRegistroNomesCrawler
- **S3 Input Path**: `s3://exerciciopblabglue/lab-glue/input/nomes.csv`
- **S3 Target Path**: `s3://exerciciopblabglue/lab-glue/frequencia_registro_nomes_eua/`

---

## 2.0 Script do Job

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, upper, count, desc

# Obtendo os parâmetros do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

# Criando contexto do Glue e Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definindo paths do S3
source_file = args['S3_INPUT_PATH']
target_path = f"{args['S3_TARGET_PATH']}/frequencia_registro_nomes_eua"

# Lendo o CSV do S3
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": [source_file]},
    format_options={"withHeader": True, "separator": ","}
).toDF()

# Convertendo a coluna 'nome' para maiúsculas
df = df.withColumn("nome", upper(col("nome")))

# Convertendo colunas 'sexo' e 'ano' para string para evitar problemas no particionamento
df = df.withColumn("sexo", col("sexo").cast("string"))
df = df.withColumn("ano", col("ano").cast("string"))

# Convertendo de volta para DynamicFrame para escrita no S3
dynamic_df = DynamicFrame.fromDF(df, glueContext)

# Escrevendo no S3 em formato JSON, particionado por sexo e ano
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": target_path,
        "partitionKeys": ["sexo", "ano"],
        "classification": "json"
    }
)

# Commit do Job
job.commit()
```

---

## 3.0 Leitura e Impressão do Schema do DataFrame

Utilizei o AWS Glue para ler o arquivo **nomes.csv** do S3 e criei um DataFrame. Em seguida, imprimi o schema dos dados para verificar a estrutura da tabela.

```python
df.printSchema()
```

---

## 4.0 Contagem de Linhas no DataFrame

Para verificar a quantidade total de registros no arquivo, utilizei o seguinte comando:

```python
print(f"Total de linhas no DataFrame: {df.count()}")
```


---

## 5.0 Contagem de Nomes Agrupados por Ano e Sexo

Agrupei os dados pelas colunas **ano** e **sexo**, contando a quantidade de ocorrências e ordenando os registros de forma decrescente.

```python
df_grouped = df.groupBy("ano", "sexo").agg(count("nome").alias("total_nomes"))
df_grouped.orderBy(df_grouped["ano"].desc()).show(10)
```


---

## 6.0 Nome Feminino Mais Frequente e Ano Correspondente

Utilizei o seguinte comando para encontrar o nome feminino mais registrado e em qual ano ocorreu:

```python
feminino_top = df.filter(col("sexo") == "F").orderBy(col("frequencia").desc()).limit(1)
feminino_top.show()
```


---

## 7.0 Nome Masculino Mais Frequente e Ano Correspondente

Para identificar o nome masculino mais registrado e seu respectivo ano, utilizei:

```python
masculino_top = df.filter(col("sexo") == "M").orderBy(col("frequencia").desc()).limit(1)
masculino_top.show()
```

---

## 8.0 Total de Registros por Ano (Top 10)

Agrupei os dados por **ano**, somei os registros e selecionei os 10 primeiros anos ordenados de forma crescente:

```python
df_ano = df.groupBy("ano").agg(count("nome").alias("total_registros"))
df_ano.orderBy("ano").show(10)
```


---
