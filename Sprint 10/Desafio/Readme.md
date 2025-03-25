# Sobre

O objetivo desta Sprint foi transformar os dados já processados na tabela *fato_filmes* e nas tabelas *dim* em uma visualização intuitiva e informativa, criando um dashboard no Amazon QuickSight para analisar os filmes da franquia Pokémon.

# Instruções: Como foi realizado o desafio

# Tema da análise e perguntas as quais ela responde

O foco danossa análise foram os filmes da franquia *Pokémon*, com o objetivo de entender sua evolução ao longo do tempo, considerando:

- Popularidade ao longo do tempo: Evolução da relevância dos filmes de 1998 a 2022.

- Análise financeira: Lucratividade e receita média dos filmes Pokémon.

- Influência dos jogos: Correlação entre lançamentos de jogos e o sucesso dos filmes.

- Comparação com outras franquias: Como Pokémon se posiciona no mercado cinematográfico.

<p>

# Criação dos Datasets no AWS QuickSight

O primeiro passo foi acessar o QuickSight e navegar até a aba "Datasets". A partir daí:

- Criamos um novo dataset e selecionamos a tabela fato_filmes.

- Como a junção de tabelas já havia sido realizada no Athena, utilizamos uma view consolidada, garantindo que todos os dados necessários estivessem disponíveis.

- Nomeamos essa view como fato_filmes e a utilizamos na construção do dashboard.

Dataset criado na Quicksight:

![dataset](../../Sprint%2010/Evidencias/conjutodadosquicksight.png)

<p>

# Construção do Dashboard

Após termos tudo preparado fui criar o dashboard, primeiramente cliquei em analises e criei uma nova utilizando o dataset que criamos anteriormente abaixo irei citar os graficos que usei e dissertar sobre o que eles nos informam

## Quantidade de titulos de Pokémon usados

Para nossa análise usamos um total de 41 titulos de pokemon, eu acabei utilizando um filtro no quicksight usando a coluna *franquia* para separar esses titulos dos demais. 

![quantidadetitulos](../../Sprint%2010/Evidencias/quantidade.png)


## Gráfico 1: Popularidade dos Filmes Pokémon (1998-2022)

Tipo: Gráfico de linha

Configuração:

EIXO X: ano
VALOR: popularidade (Média)

#### O que este gráfico responde?

Ele mostra a evolução da popularidade dos filmes Pokémon ao longo dos anos. Podemos identificar tendências, picos de popularidade e períodos de declínio, analisando como os lançamentos impactaram o interesse do público.

Grafico abaixo:

![grafico1](../../Sprint%2010/Evidencias/grafico1.png)


## Gráfico 2: Média de Lucro dos Filmes Pokémon

Tipo: Gráfico de rosca 

Configuração:

GRUPO/COR: titulo

VALOR: lucro (Média)

#### O que este gráfico responde?

Este gráfico apresenta uma visão geral da lucratividade dos filmes Pokémon. Ele ajuda a entender se a franquia é financeiramente sustentável e qual foi a média de lucro que os filmes obtiveram no total.

Grafico abaixo:

![grafico2](../../Sprint%2010/Evidencias/grafico2.png)


## Gráfico 3: Receita Média por Franquia

Tipo: Gráfico de barras empilhadas horizontais 

Configuração:

EIXO Y: franquia

VALOR: receita (Média)

#### O que este gráfico responde?

Ele compara a receita média dos filmes Pokémon com outras franquias, permitindo analisar como a franquia se posiciona financeiramente no mercado cinematográfico.

Grafico abaixo:

![grafico3](../../Sprint%2010/Evidencias/grafico3.png)


## Gráfico 4: Jogos com Maior Influência na Popularidade dos Filmes Pokémon

Tipo: Nuvem de palavras

 Configuração:

AGRUPAR POR: jogos_relacionados

TAMANHO: popularidade (Média)

#### O que este gráfico responde?

Ele destaca quais jogos da franquia tiveram maior impacto na popularidade dos filmes. Isso nos permite entender se certos lançamentos impulsionaram a audiência dos filmes.

Grafico abaixo:

![grafico4](../../Sprint%2010/Evidencias/grafico4.png)


## Gráfico 5: Influência dos Lançamentos de Jogos na Receita dos Filmes Pokémon (1998-2022)

📊 Tipo: Gráfico de barras verticais📌 Configuração:

EIXO X:

ano

VALOR2:

receita (Média)

id_jogo (Contagem de distintos)


#### O que este gráfico responde? 

Ele examina a relação entre o lançamento de jogos e a receita dos filmes, ajudando a identificar se os lançamentos de novos jogos impactaram o faturamento dos filmes Pokémon.

Grafico abaixo:

![grafico5](../../Sprint%2010/Evidencias/grafico5.png)


# Dashboard Completo

![dashboard](../../Sprint%2010/Evidencias/dashboard.png)

Acesse o *pdf* do dashboard clicando [aqui](../../Sprint%2010/Desafio)