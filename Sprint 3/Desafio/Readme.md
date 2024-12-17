# Sobre

Bom, neste desafio temos como objetivo principal é ler o arquivo de estatísticas da Loja do Google (googleplaystore.csv), processar e gerar gráficos de análise com o auxilio das bibliotecas `Pandas` e `Matplotlib`.

# Instruções, como foi realizado de fato o desafio.

#### Antes de eu começar a explicar o código em si precisamos realizar algumas coisas como:

- A instalação do Python em nossa máquina
- A instalação das Extensões Jupiter(ipynb) e Python
- A instalação das Bibliotecas que vamos usar

#### Essa ultima, para instalarmos as bibliotecas, iremos abrir o terminal no Vscode após as instalações necessárias e usar o seguinte comando:

``` bash
pip install pandas matplotlib notebook

```

### 1. Preparando o Ambiente e lendo o arquivo.csv (Pós instalações)

Bom partindo do ponto que ja temos as bibliotecas intaladas e o arquivo googleplaystore.csv também foi baixado. Iremos criar um arquivo .ipynb (notebook), nele nos vamos realizar o seguinte comando para importar as biblioteca.

``` bash
import pandas as pd
import matplotlib.pyplot as plt
```

e também o seguinte comando para exibir os gráficos no nosso notebook.

``` bash
%matplotlib inline
```

<br>

Após isso realizamos a leitura do arquivo .csv e no caso do meu desafio após essa leitura eu coloquei um comando para exibir as primeiras linhas só para ter certeza ques estava lidando com o arquivo correto (escolha pessoal minha, não havia necessidade).

Comando utilizado:

``` bash
file_path = "googleplaystore.csv" 
data = pd.read_csv(file_path)

data.head()
```

Resultado:

![retornolinhas](/Sprint%203/Evidencias/evidencias_desafio/linhasconfirmandoleituraarquivo.png)

<br>

#### Remoção de duplicatas

Como é pedido no desafio devemos remover as duplicatas deste arquivo. Então, a principio eu decidi realizar a releitura do arquivo original .csv e armazenei as informações em um DataFrame chamado `data_original`. Registrei a quantidade total de linhas antes da remoção das duplicatas, usando o comando:

``` bash
# Releitura do arquivo original
data_original = pd.read_csv(file_path)

# Quantidade total de linhas antes de remover duplicatas
total_linhas_antes = data_original.shape[0]
```

Em seguida, utilizei o método .drop_duplicates() para remover duplicatas com base nas colunas 'App' e 'Category', ou seja, as linhas foram consideradas duplicadas e removidas apenas se o nome do aplicativo e a categoria fossem exatamente iguais e logo após isso criando uma cópia do DataFrame limpo.

 Comando abaixo:

``` bash
data = data_original.drop_duplicates(subset=['App', 'Category']).copy()
```

Após a remoção, calculei novamente a quantidade total de linhas no DataFrame resultante, determinei o número de duplicatas removidas subtraindo as linhas finais das iniciais e por fim exibi as mensagens apresentadas no código para verificar o processo.

``` bash
# Quantidade total de linhas depois de remover duplicatas
total_linhas_depois = data.shape[0]

# Quantidade de duplicatas removidas
duplicatas = total_linhas_antes - total_linhas_depois

# Exibindo mensagens
print(f"Quantidade total de linhas antes da remoção de duplicatas: {total_linhas_antes}")
print(f"Quantidade total de linhas depois da remoção de duplicatas: {total_linhas_depois}")
print(f"Quantidade de duplicatas removidas: {duplicatas}")
```

Resultado:

![respostaremocaoduplicatas](/Sprint%203/Evidencias/evidencias_desafio/resultadoremocaodupli.png)

### 2. Grafico de barras: Top 5 apps por numero de instalações

Bom pra gerar o gráfico dos Top 5 aplicativos por número de instalações, foi necessário realizar alguns ajustes na coluna `Installs`. Inicialmente, a coluna continha caracteres não numéricos, como `+` e `,`, que impediam a conversão para valores numéricos.

Utilizei o método `.str.replace()` para substituir os caracteres `+ e ,` por uma string vazia `''`. O parâmetro `regex=True` permite a aplicação de expressões regulares. Após a remoção dos caracteres, foi necessário converter a coluna `'Installs'` para o tipo numérico. A função `pd.to_numeric()` foi utilizada com o parâmetro `errors='coerce'` para tratar valores que não puderam ser convertidos. Após a conversão, valores inválidos foram transformados em `NaN`. Portanto, usei o método `.dropna()` para remover essas linhas do DataFrame. Com essas etapas, a coluna `'Installs'` ficou limpa e pronta para análise. Como podemos ver no código abaixo:

``` bash
# Precisei fazer as mudanças abaixo pra conseguir gerar o grafico 

# Removendo caracteres não numéricos na coluna 'Installs'
data['Installs'] = data['Installs'].str.replace('[+,]', '', regex=True)  

# Convertendo a coluna 'Installs' para numérico
data['Installs'] = pd.to_numeric(data['Installs'], errors='coerce')  

# Removendo valores nulos, caso existam, após a conversão
data = data.dropna(subset=['Installs'])
```
<br>


Após o tratamento dos dados, selecionei os 5 aplicativos com o maior número de instalações usando o método `.nlargest()` e gerei um gráfico de barras para visualização.

``` bash

top_5_installs = data.nlargest(5, 'Installs')[['App', 'Installs']]

```

E utilizei a biblioteca `matplotlib` para criar o gráfico de barras. Adicionei títulos, rótulos nos eixos e configurei a rotação dos nomes dos aplicativos no eixo X para melhor visualização.

``` bash
plt.figure(figsize=(10, 5))
plt.bar(top_5_installs['App'], top_5_installs['Installs'], color='pink')
plt.title('Top 5 Aplicativos por Número de Instalações')
plt.xlabel('Aplicativos')
plt.ylabel('Número de Instalações')
plt.xticks(rotation=45)
plt.show()

```

Resultado:
![barras](/Sprint%203/Evidencias/evidencias_desafio/grafico_barras_top5appsinstalacoes.png)


### 3. Grafico de pizza: Categorias por Frequência de Aparição


Para analisar a distribuição das categorias de aplicativos, contei a frequência de cada categoria utilizando o método `.value_counts()` aplicado na coluna `'Category'`. Em seguida, agrupei as categorias com menos de *4%* de participação em uma nova categoria chamada `"Outros"`. Para isso, calculei a proporção de cada categoria em relação ao total e filtrei apenas as que possuem participação maior ou igual ao *threshold* definido. As categorias com participação abaixo desse limite foram somadas e agrupadas em `"Outros"`.

Além disso, defini uma paleta de cores pastel chamada pastel_arco_iris para aplicar no gráfico de pizza, gerado com o método `.plot.pie()`. O gráfico exibe as porcentagens `(autopct='%1.1f%%')` e possui configurações para melhorar a visualização, como o ângulo inicial `(startangle=140)` e a remoção do rótulo do *eixo Y*.

Como apresentado no codigo abaixo:


``` bash
# Contar a frequência de cada categoria
categories_count = data['Category'].value_counts()

# Criando a categoria "Outros" para categorias com menos de 4% de participação
threshold = 0.04
categories_count = categories_count[categories_count / categories_count.sum() >= threshold]

# Categorias com menos de 3% vão para "Outros"
others_count = data['Category'].value_counts()[data['Category'].value_counts() / data['Category'].value_counts().sum() < threshold]
categories_count['Others'] = others_count.sum()

# Definindo uma paleta de cores 
pastel_arco_iris = ['#FFB3BA','#FFDFBA','#FFFFBA','#B3FFBA','#BAE1FF','#D1BAFF','#FFBAF3','#FF9B9B','#D1F7FF']  

# Criar gráfico de pizza
plt.figure(figsize=(8, 8))  
categories_count.plot.pie(
    autopct='%1.1f%%',  
    startangle=140,  
    colors=pastel_arco_iris 
)
plt.title('Distribuição das Categorias de Apps') 
plt.ylabel('')  
plt.show() 

```

Resultado:
![pizza](/Sprint%203/Evidencias/evidencias_desafio/grafico_pizza_categoriasporfrequenciaparicao.png)


### 4. Exibindo App Mais Caro 

Para identificar e exibir o aplicativo mais caro, usei o método `.idxmax()`, que retorna o índice do valor máximo presente na coluna `*'Price'*`. Em seguida, utilizei o `.loc` para acessar os dados do aplicativo nesse índice específico e selecionei apenas as colunas *'App'* e *'Price'* para exibição.

Como podemos ver no código abaixo:

``` bash
app_mais_caro = data.loc[data['Price'].idxmax()]
app_mais_caro[['App', 'Price']]

```

Resultado:

![app+caro](/Sprint%203/Evidencias/evidencias_desafio/appmaiscaro.png)


### 5. Contagem de apps classificados como "Mature 17+"

Para contar a quantidade de aplicativos classificados como *"Mature 17+"*, filtrei o DataFrame utilizando uma condição na coluna `'Content Rating'`. Em seguida, utilizei `.shape[0]` para obter o total de linhas que atendem à condição, que corresponde ao número de aplicativos nessa classificação. Por fim, mostrei o resultado usando um `print()`.

Como podemos ver no codigo abaixo:

``` bash
mature_17_count = data[data['Content Rating'] == 'Mature 17+'].shape[0]
print(f"Quantidade de aplicativos classificados como 'Mature 17+': {mature_17_count}")

```

Resultado:

![mature+17](/Sprint%203/Evidencias/evidencias_desafio/contagemappclassmature+17.png)

<br>

### 6. Top 10 apps por número de reviews

Primeiramente foi necessário garantir que a coluna `'Reviews'` estivesse no formato numérico. Para isso, utilizei a função `pd.to_numeric()` com o parâmetro `errors='coerce'`, que transforma valores inválidos em *NaN*. Em seguida, removi as linhas com valores nulos utilizando o método `.dropna()`.

Como um mesmo aplicativo pode aparecer mais de uma vez no DataFrame, mantive apenas a entrada com o maior número de reviews para cada aplicativo. Para isso, utilizei o método .`groupby()` na coluna *'App'*, seguido de `.idxmax()` para encontrar o índice com o maior valor na coluna *'Reviews'*.

Por fim, utilizei o método `.nlargest()` para selecionar os 10 aplicativos com o maior número de reviews, exibindo apenas as colunas *'App'* e *'Reviews'* em ordem decrescente.

Como podemos ver no código abaixo:

``` bash
# Convertendo a coluna 'Reviews' para numérico
data['Reviews'] = pd.to_numeric(data['Reviews'], errors='coerce')

# Removendo valores nulos, caso existam
data = data.dropna(subset=['Reviews'])

# Removendo duplicatas por app, mantendo o maior número de reviews
data = data.loc[data.groupby('App')['Reviews'].idxmax()]

# Top 10 apps por número de reviews
top_10_reviews = data.nlargest(10, 'Reviews')[['App', 'Reviews']]

# Exibindo os 10 apps com maior número de reviews de forma decrescente
print("Top 10 Aplicativos por Número de Reviews (ordem decrescente):")
print(top_10_reviews)

```

Resultado:

![top10appporreview](/Sprint%203/Evidencias/evidencias_desafio/top10appspornumerodereviews.png)


### 7. Criar pelo menos 2 calculos sobre o dataset e apresentar um em formato de lista e outro em formato de valor

#### Lista: Top 10 Categorias com Mais Apps

Aqui está o texto formatado de forma unificada no mesmo estilo:

Para listar as Top 10 categorias com mais aplicativos, utilizei o método `.value_counts()` na coluna 'Category', que retorna a contagem de ocorrências de cada categoria em ordem decrescente. Em seguida, apliquei o método `.head(10)` para selecionar apenas as 10 categorias com o maior número de aplicativos.

Para exibir os resultados de forma numerada e organizada, utilizei um loop for com a função `.items()`, que retorna os pares de chave e valor (categoria e contagem). O loop enumera as categorias a partir de 1 e imprime cada uma no formato desejado.

Como podemos ver no código abaixo:

``` bash

top_10_categories = data['Category'].value_counts().head(10)


print("Top 10 Categorias com mais Apps:")
for i, (category, count) in enumerate(top_10_categories.items(), 1):
    print(f"{i}. {category}: {count}")

```

Resultado:

![top10categoriacmmaisapp](/Sprint%203/Evidencias/evidencias_desafio/top10categoriascmmaisapps.png)


<br>

#### Categoria Mais Popular com Base no Número de Instalações

Para determinar a categoria mais popular com base no número de instalações, primeiro utilizei o método `.groupby()` na coluna 'Category' para agrupar os dados por categoria e, em seguida, apliquei `.sum()` na coluna 'Installs' para calcular o total de instalações em cada categoria.

Com os totais calculados, utilizei `.idxmax()` para identificar a categoria com o maior número de instalações e `.max()` para obter o valor correspondente.

Por fim, exibi o resultado com a função *print()*, mostrando a categoria mais popular e o total de instalações.

Como podemos ver no código abaixo:

``` bash
# Somar o número de instalações por categoria
category_installs = data.groupby('Category')['Installs'].sum()

# Encontrar a categoria mais popular
most_popular_category = category_installs.idxmax()
most_popular_installs = category_installs.max()

# Exibindo o resultado
print(f"A categoria mais popular é: {most_popular_category}, com {most_popular_installs} instalações no total.")

```

resultadp:

![categmaispopularporinstal](/Sprint%203/Evidencias/evidencias_desafio/categoriamaispopularcmbaseinstalls.png)

<br>

### 8. Graficos dos indicadores acima


#### Lista: Top 10 Categorias com Mais Apps Gráfico de linha

Para a criação desse grafico, extraí os dados da variável top_10_categories, que contém as categorias e suas respectivas contagens de aplicativos. Utilizei categories para armazenar as categorias e counts para as contagens de aplicativos.

Em seguida, utilizei *matplotlib.pyplot* para gerar o gráfico. A função *plt.plot()* foi usada para criar a linha, com marcadores *(marker='o')* e linha contínua *(linestyle='-')*, além da cor azul (color='b'). Adicionei também título e rótulos nos eixos, rotacionando as categorias no eixo X para melhorar a visualização. A legenda foi configurada com *plt.legend()* e adicionei uma grade no gráfico com *plt.grid()*.

Segue o código e o gráfico abaixo:

``` bash
import matplotlib.pyplot as plt

# Extraindo os dados da variável top_10_categories
categories = top_10_categories.index  
counts = top_10_categories.values     

# Gráfico de Linhas
plt.figure(figsize=(10, 5))
plt.plot(categories, counts, marker='o', linestyle='-', color='b', label='Número de Apps')
plt.title('Top 10 Categorias com mais Apps', fontsize=14)
plt.xlabel('Categorias', fontsize=12)
plt.ylabel('Número de Apps', fontsize=12)
plt.xticks(rotation=45)
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
```

resultado:

![linhagrafico](/Sprint%203/Evidencias/evidencias_desafio/graficolinhastop10categoriaapp.png)

<br>

#### Grafico dispersão: Categoria Mais Popular com Base no Número de Instalações

Para criar o gráfico de dispersão, utilizei os dados da variável *category_installs*, que contém as categorias e o total de instalações em cada uma delas. Extraí as categorias para a variável *categories* e os totais de instalações para a variável *installs*.

Em seguida, usei *matplotlib.pyplot* para gerar o gráfico de dispersão com a função *plt.scatter()*, onde defini a cor dos pontos como vermelha (color='r'), o tamanho dos pontos como 100 (s=100), e adicionei a legenda label='Instalações por Categoria'. A exibição dos eixos foi ajustada com rótulos e título, além de uma rotação de 45 graus para as categorias no eixo X para melhorar a legibilidade.

Segue o código e o gráfico abaixo:

``` bash
categories = category_installs.index  
installs = category_installs.values   

# Gráfico de Dispersão
plt.figure(figsize=(10, 5))
plt.scatter(categories, installs, color='r', s=100, label='Instalações por Categoria')
plt.title('Instalações por Categoria (Gráfico de Dispersão)', fontsize=14)
plt.xlabel('Categorias', fontsize=12)
plt.ylabel('Número de Instalações', fontsize=12)
plt.xticks(rotation=45, fontsize=8, ha='right')  
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()
```

Resultado:

![](/Sprint%203/Evidencias/evidencias_desafio/graficodispersaocategoriamaispopularcmbaseinstall.png)

