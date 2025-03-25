## Processamento de Arquivo com Spark em Docker

### Objetivo

Nesta atividade, configurei um ambiente Apache Spark dentro de um container Docker para processar um arquivo README.md armazenado em um repositório privado no GitHub. O objetivo foi contar a quantidade de palavras no arquivo utilizando Spark RDDs.

### 1.0 Configuração do Ambiente

#### 1.1 Baixar a imagem do Docker

Para iniciar, fiz o download da imagem do Docker que contém o Spark e o Jupyter Notebook:

```bash
docker pull jupyter/pyspark-notebook
```

#### 1.2 Criar e rodar um container

Após baixar a imagem, criei e iniciei um container a partir dela:

```bash
docker run -it -p 8888:8888 --name meu_spark jupyter/pyspark-notebook
```

Isso inicia o container e disponibiliza um ambiente interativo.

Para acessarmos o Jupyter Notebook, da pra utilizarmos o link gerado nos logs do container.
```
http://127.0.0.1:8888/lab?token=SEU_TOKEN_AQUI
```
Eu acabei decidindo utilizar outro terminal para acessar o container via bash:

``` bash
docker exec -it meu_spark bash
```

### 2.0 Download do Arquivo README.md

Como o nosso repositório GitHub é privado, utilizei um token de autenticação para baixar o arquivo com wget como podemos ver abaixo:

``` bash
wget --header="Authorization: token SEU_TOKEN_AQUI" -O Readme.md \
"https://raw.githubusercontent.com/USUARIO/REPOSITORIO/main/Readme.md"
```

Após o download, confirmei que o arquivo estava presente no diretório com:

``` bash
ls -l Readme.md
```

*Resultado*:

![resultadodownloadarquivo](../../Sprint%207/Evidencias/evidenciaexercicio/exerciciosparkcontador/resultadodownloaddoarquivo.png)


### 3.0 Execução do Spark Shell

Com o arquivo no ambiente, iniciei o Spark Shell executando:

``` bash
pyspark
``` 

Isso abriu um terminal interativo do Spark para processar o arquivo.Como podemos ver abaixo.

![terminalsaprk](../../Sprint%207/Evidencias/evidenciaexercicio/exerciciosparkcontador/resultadospark.png)

### 4.0 Contagem de Palavras com Spark e resultados

No shell do PySpark, utilizei o seguinte código para contar a quantidade de palavras no arquivo:

``` bash
import re

# Carregando o arquivo README.md como um RDD
rdd = sc.textFile("Readme.md")

# Processa as linhas, removendo pontuação, removendo palavras vazias e separando palavras

words = (
    rdd.flatMap(lambda line: re.sub(r"[^\wáéíóúãõçÁÉÍÓÚÃÕÇ-]", " ", line).split())
    .filter(lambda word: word.strip() != "")  
)

# Conta o número total de palavras
word_count = words.count()
print(word_count)

``` 

*Resultados*:

#### Arquivo readme que utilizamos

![readmeusadogithub](../../Sprint%207/Evidencias/evidenciaexercicio/exerciciosparkcontador/arquivoreadmedogithub.png)

#### Resultado do contador

![resultado](../../Sprint%207/Evidencias/evidenciaexercicio/exerciciosparkcontador/resultadodocontador.png)


Após processar o arquivo com Spark, obtive a contagem total de palavras que foi `15` o que demonstra que o código funcionou de maneira efetiva.

