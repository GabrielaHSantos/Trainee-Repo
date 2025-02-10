# Sobre

Nosso objetivo foi aplicar os conhecimentos adquiridos sobre AWS, Python e etc na ingestão de dados via API. Trabalhamos com a API TMDB para coletar informações sobre filmes e séries, realizando a ingestão dos dados diretamente para um bucket no AWS S3 através de uma função AWS Lambda escrita em Python.

# Instruções: Como foi realizado o desafio

#### Antes de começar a explicar o código, precisamos realizar algumas configurações:

- **Bibliotecas usadas**: `boto3`, `requests`, `json`, `os`, `datetime` e `botocore` (dependência do `boto3`).

Essa parte das bibliotecas tivemos de realizar as instalações delas localmente, eu as coloquei dentro de um dir `python` e as compactei como *camada_lamda_dependencias.zip* e após isso dentro do lambda na parte de camadas criei a camada *camada_lambda_dependencias* e realizei o upload do arquivo *.zip*.

#### A "camada_lambda_dependencias" dentro das camadas no lambda.
![camada](../Evidencias/evidenciadesafio/camadacriadanascamadas.png)

Código usado no terminal para criação dos diretórios, instalação das bibliotecas e compactação para zip:

``` powershell
mkdir camada_lambda_dependencias
cd camada_lambda_dependencias
mkdir python
pip install requests -t python/
pip install boto3 -t python/

Compress-Archive -Path python -DestinationPath C:\Users\Gabriel\camada_lambda_dependencias.zip
```

### Dando permissão no IAM pra função

Para conceder permissão total ao S3 para minha função Lambda *ingestaodadostmdb*, acessei o AWS IAM, fui até a seção Funções e encontrei a função ingestaodadostmdb. Em seguida, entrei na aba Permissões, cliquei em Anexar políticas, busquei por `AmazonS3FullAccess` e selecionei essa política. Por fim, cliquei em Anexar política para finalizar o processo


![permissaoiam](../Evidencias/evidenciadesafio/dandopermissoesprafunção.png)

### Criando variável de ambiente para a chave API

*OBS*: A API TMDB exige uma chave de acesso. Ela deve ser armazenada de forma segura e não deve ser exposta no código-fonte. Para realizar isso, eu fui nas configurações e coloquei a chave API nas variáveis de ambiente. Imagem abaixo do local onde colocamos a chave API.

![configvarambien](../Evidencias/evidenciadesafio/variaveldemabienet.png)

#### Configuração do cliente S3

```python
import os
import json
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
NOME_BUCKET = "data-lake-sprint6"
REGIAO = "us-east-1"
CAMINHO_RAW = "Raw/TMDB/JSON"
API_KEY = os.getenv("TMDB_API_KEY")  
```

*OBS*: Utilizei o bucket da Sprint passada

### Função para buscar dados da API TMDB

A função `obter_dados_tmdb` busca dados apenas dos gêneros "Comédia" e "Animação", tanto para filmes quanto para séries.

```python
def obter_dados_tmdb(genero, ano_limite="2022", eh_serie=False):
    dados = []
    ids_processados = set()
    tipo_conteudo = "tv" if eh_serie else "movie"

    for pagina in range(1, 51):
        url = f"https://api.themoviedb.org/3/discover/{tipo_conteudo}?api_key={API_KEY}&language=pt-BR&with_genres={genero}&page={pagina}&sort_by=popularity.desc&primary_release_year={ano_limite}"
        resposta = requests.get(url)
        
        if resposta.status_code == 200:
            dados_pagina = resposta.json().get("results", [])
            if not dados_pagina:
                break
            novos_dados = [item for item in dados_pagina if item['id'] not in ids_processados]
            dados.extend(novos_dados)
            ids_processados.update([item['id'] for item in novos_dados])
        else:
            print(f"Erro ao buscar dados: {resposta.status_code}")
            break
    
    return dados
```

### Salvando os Dados no raw do bucket da S3

A função AWS Lambda consome dados da API TMDB, divide os registros em grupos de até 100 entradas e salva os arquivos JSON na camada RAW do bucket S3.

```python
def salvar_no_s3(dados, tipo_dado):
    try:
        agora = datetime.now()
        ano = agora.strftime("%Y")
        mes = agora.strftime("%m")
        dia = agora.strftime("%d")
        chave_s3 = f"{CAMINHO_RAW}/{tipo_dado}/{ano}/{mes}/{dia}/dados.json"
        s3.put_object(Bucket=NOME_BUCKET, Key=chave_s3, Body=json.dumps(dados))
        print(f"Dados salvos em {chave_s3}")
    except ClientError as e:
        print(f"Erro ao salvar dados no S3: {e}")
```

### Implementando a Função AWS Lambda

 A função Lambda faz a ingestão de dados dos gêneros "Comédia" e "Animação" do TMDB e os armazena no S3.

```python
def lambda_handler(event, context):
    print("Iniciando a ingestão de dados da TMDB...")
    
    dados_animacao_filmes = obter_dados_tmdb(genero="16", ano_limite="2022", eh_serie=False)
    dados_comedia_filmes = obter_dados_tmdb(genero="35", ano_limite="2022", eh_serie=False)
    dados_animacao_series = obter_dados_tmdb(genero="16", ano_limite="2022", eh_serie=True)
    dados_comedia_series = obter_dados_tmdb(genero="35", ano_limite="2022", eh_serie=True)
    
    dados_combinados = dados_animacao_filmes + dados_comedia_filmes + dados_animacao_series + dados_comedia_series
    
    if dados_combinados:
        salvar_no_s3(dados_combinados, "FilmesSeries")
    
    return {
        "statusCode": 200,
        "mensagem": "Ingestão concluída! Dados de Comédia e Animação processados."
    }
```

### Resultado

#### Antes da execução:
![bucketantes](../Evidencias/evidenciadesafio/bucketantesdaexec.png)

#### Lambda mostrando que o código foi executado com sucesso no log
![sucesso](../Evidencias/evidenciadesafio/executadocmsucesso.png)

#### Depois da execução:
![bucketdepois](../Evidencias/evidenciadesafio/s3posexece1.png)

#### Arquivo JSON gerado:
![arquivojson](../Evidencias/evidenciadesafio/arquivojson.png)

