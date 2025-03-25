import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, concat_ws, lower, trim, when, coalesce, lit, to_date, year, month, dayofmonth, row_number
)
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf


# Criando a SparkSession
spark = SparkSession.builder.appName("GlueJob_Refined").getOrCreate()

# Caminho dos dados da Trusted Zone
trusted_filmes_path = "s3://data-lake-sprint6/Trusted/Local/Parquet/Filmes/"
trusted_movies_path = "s3://data-lake-sprint6/Trusted/TMDB/Parquet/Movies/ano_ingestao=2025/mes_ingestao=3/dia_ingestao=18/"
trusted_games_path = "s3://data-lake-sprint6/Trusted/IGDB/Parquet/Games/"

# Caminho da Refined Zone
refined_filmes_path = "s3://data-lake-sprint6/Refined/fatos_filmes/"
refined_dim_franquia_path = "s3://data-lake-sprint6/Refined/dim_franquia/"
refined_dim_jogo_path = "s3://data-lake-sprint6/Refined/dim_jogo/"
refined_dim_tempo_path = "s3://data-lake-sprint6/Refined/dim_tempo/"

# Carregando dados da Trusted Zone
filmes_df = spark.read.parquet(trusted_filmes_path)
movies_df = spark.read.parquet(trusted_movies_path)
games_df = spark.read.parquet(trusted_games_path)

# Garantindo compatibilidade dos tipos de colunas
filmes_df = filmes_df.withColumn("id", col("id").cast(IntegerType()))
movies_df = movies_df.withColumn("id", col("id").cast(IntegerType()))
movies_df = movies_df.withColumn("orcamento", col("orcamento").cast(IntegerType()))
movies_df = movies_df.withColumn("receita", col("receita").cast(IntegerType()))
movies_df = movies_df.withColumn("popularidade", col("popularidade").cast(DoubleType()))
movies_df = movies_df.withColumn("nota_media", col("nota_media").cast(DoubleType()))
movies_df = movies_df.withColumn("numero_votos", col("numero_votos").cast(LongType()))

# Mapeamento de gêneros
generos_map = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime", 99: "Documentary",
    18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Science Fiction", 10770: "TV Movie", 53: "Thriller",
    10752: "War", 37: "Western"
}

def mapear_generos(generos_array):
    return [generos_map.get(genero, "Desconhecido") for genero in generos_array]

mapear_generos_udf = udf(mapear_generos, ArrayType(StringType()))
movies_df = movies_df.withColumn("generos", mapear_generos_udf(col("generos")))
movies_df = movies_df.withColumn("generos", concat_ws(", ", col("generos")))

# Normalizando os títulos
filmes_df = filmes_df.withColumn("titulo", lower(trim(col("titulo"))))
movies_df = movies_df.withColumn("titulo", lower(trim(col("titulo"))))
movies_df = movies_df.withColumn("titulo_original", lower(trim(col("titulo_original"))))

# Convertendo ano_lancamento para data_lancamento
filmes_df = filmes_df.withColumn(
    "data_lancamento", 
    to_date(concat_ws("-", col("ano_lancamento"), lit("01"), lit("01")), "yyyy-MM-dd")
)
movies_df = movies_df.withColumn(
    "data_lancamento", 
    to_date(col("data_lancamento"), "yyyy-MM-dd")
)

# Priorizando os dados do TMDB (movies) com join mais robusto
df_final = movies_df.alias("m").join(
    filmes_df.alias("f"),
    (lower(trim(col("m.titulo"))) == lower(trim(col("f.titulo")))) |
    (lower(trim(col("m.titulo_original"))) == lower(trim(col("f.titulo_original")))),
    "left"
).select(
    coalesce(col("m.id"), col("f.id")).alias("id_original"),
    coalesce(col("m.titulo"), col("f.titulo")).alias("titulo"),
    coalesce(col("m.titulo_original"), col("f.titulo_original")).alias("titulo_original"),
    col("m.orcamento").alias("orcamento"),
    col("m.receita").alias("receita"),
    col("m.generos"),
    coalesce(col("m.data_lancamento"), col("f.data_lancamento")).alias("data_lancamento"),
    col("m.popularidade"),  # Adicionando popularidade
    col("m.nota_media"),    # Adicionando nota_media
    col("m.numero_votos")   # Adicionando numero_votos
).dropDuplicates(["titulo_original", "titulo"])

# Criando ID único para filmes
window_spec_filmes = Window.orderBy(col("titulo"), col("data_lancamento"))
df_final = df_final.withColumn("id_filme", row_number().over(window_spec_filmes))

# Adicionando dados financeiros diretamente na tabela fato
df_final = df_final.withColumn(
    "lucro", 
    when((col("orcamento") == 0) | (col("receita") == 0), None)
    .otherwise(expr("receita - orcamento"))
)

# Criando Dimensão Franquia com id_franquia fixo para Pokémon
dim_franquia_df = df_final.filter(
    col("titulo").rlike("pokémon|digimon|one piece|naruto|dragon ball|yugioh|beyblade|sailor moon|mario|sonic")
).select(
    col("id_filme"),
    col("titulo"),
    when(col("titulo").contains("pokémon"), 1)  # id_franquia fixo para Pokémon
    .when(col("titulo").contains("digimon"), 2)
    .when(col("titulo").contains("one piece"), 3)
    .when(col("titulo").contains("naruto"), 4)
    .when(col("titulo").contains("dragon ball"), 5)
    .when(col("titulo").contains("yugioh"), 6)
    .when(col("titulo").contains("beyblade"), 7)
    .when(col("titulo").contains("sailor moon"), 8)
    .when(col("titulo").contains("mario"), 9)
    .when(col("titulo").contains("sonic"), 10)
    .otherwise(0).alias("id_franquia"),  # Outras franquias
    when(col("titulo").contains("pokémon"), "Pokémon")
    .when(col("titulo").contains("digimon"), "Digimon")
    .when(col("titulo").contains("one piece"), "One Piece")
    .when(col("titulo").contains("naruto"), "Naruto")
    .when(col("titulo").contains("dragon ball"), "Dragon Ball")
    .when(col("titulo").contains("yugioh"), "Yu-Gi-Oh!")
    .when(col("titulo").contains("beyblade"), "Beyblade")
    .when(col("titulo").contains("sailor moon"), "Sailor Moon")
    .when(col("titulo").contains("mario"), "Super Mario")
    .when(col("titulo").contains("sonic"), "Sonic the Hedgehog")
    .otherwise("Outros").alias("franquia")
).distinct()

# Adicionando id_franquia à tabela fato
df_final = df_final.join(
    dim_franquia_df.select("id_filme", "id_franquia"),
    "id_filme",
    "left"
)

# Criando Dimensão Tempo com id_filme
dim_tempo_df = df_final.select(
    col("data_lancamento").alias("data"),
    year(col("data_lancamento")).alias("ano"),
    month(col("data_lancamento")).alias("mes"),
    dayofmonth(col("data_lancamento")).alias("dia"),
    col("id_filme")  # Adicionando id_filme na dim_tempo
).distinct()

# Lista de títulos de jogos
jogos_pokemon = [
    {"titulo": "detective pikachu", "data_lancamento": "2018-03-22"},
    {"titulo": "new pokémon snap", "data_lancamento": "2021-04-29"},
    {"titulo": "pokémon alpha sapphire", "data_lancamento": "2014-11-21"},
    {"titulo": "pokémon art academy", "data_lancamento": "2014-06-19"},
    {"titulo": "pokémon battle revolution", "data_lancamento": "2006-12-14"},
    {"titulo": "pokémon battle trozei", "data_lancamento": "2014-03-12"},
    {"titulo": "pokémon black", "data_lancamento": "2010-09-18"},
    {"titulo": "pokémon blue", "data_lancamento": "1996-10-15"},
    {"titulo": "pokémon brilliant diamond", "data_lancamento": "2021-11-19"},
    {"titulo": "pokémon channel", "data_lancamento": "2003-07-18"},
    {"titulo": "pokémon colosseum", "data_lancamento": "2003-11-21"},
    {"titulo": "pokémon conquest", "data_lancamento": "2012-03-17"},
    {"titulo": "pokémon crystal", "data_lancamento": "2022-07-03"},
    {"titulo": "pokémon dash", "data_lancamento": "2004-12-02"},
    {"titulo": "pokémon diamond", "data_lancamento": "2000-12-31"},
    {"titulo": "pokémon emerald", "data_lancamento": "2022-07-15"},
    {"titulo": "pokémon firered", "data_lancamento": "2004-01-29"},
    {"titulo": "pokémon gold", "data_lancamento": "1999-11-21"},
    {"titulo": "pokémon green", "data_lancamento": "1996-02-27"},
    {"titulo": "pokémon heartgold", "data_lancamento": "2009-09-12"},
    {"titulo": "pokémon leafgreen", "data_lancamento": "2004-01-29"},
    {"titulo": "pokémon legends: arceus", "data_lancamento": "2022-01-28"},
    {"titulo": "pokémon moon", "data_lancamento": "2016-11-18"},
    {"titulo": "pokémon mystery dungeon: blue rescue team", "data_lancamento": "2005-11-17"},
    {"titulo": "pokémon mystery dungeon: explorers of darkness", "data_lancamento": "2007-09-13"},
    {"titulo": "pokémon mystery dungeon: explorers of sky", "data_lancamento": "2009-04-18"},
    {"titulo": "pokémon mystery dungeon: explorers of time", "data_lancamento": "2007-09-13"},
    {"titulo": "pokémon mystery dungeon: gates to infinity", "data_lancamento": "2012-11-23"},
    {"titulo": "pokémon mystery dungeon: red rescue team", "data_lancamento": "2005-11-17"},
    {"titulo": "pokémon mystery dungeon: rescue team dx", "data_lancamento": "2020-03-06"},
    {"titulo": "pokémon omega ruby", "data_lancamento": "2014-11-21"},
    {"titulo": "pokémon pearl", "data_lancamento": "2006-09-28"},
    {"titulo": "pokémon pinball", "data_lancamento": "1999-04-14"},
    {"titulo": "pokémon pinball: ruby & sapphire", "data_lancamento": "2003-08-01"},
    {"titulo": "pokémon platinum", "data_lancamento": "2008-09-13"},
    {"titulo": "pokémon quest", "data_lancamento": "2018-05-29"},
    {"titulo": "pokémon ranger", "data_lancamento": "2006-03-23"},
    {"titulo": "pokémon ranger: guardian signs", "data_lancamento": "2010-03-06"},
    {"titulo": "pokémon ranger: shadows of almia", "data_lancamento": "2008-03-20"},
    {"titulo": "pokémon red", "data_lancamento": "1996-02-27"},
    {"titulo": "pokémon ruby", "data_lancamento": "2002-11-21"},
    {"titulo": "pokémon rumble", "data_lancamento": "2013-04-24"},
    {"titulo": "pokémon rumble blast", "data_lancamento": "2011-08-11"},
    {"titulo": "pokémon rumble rush", "data_lancamento": "2019-05-15"},
    {"titulo": "pokémon rumble u", "data_lancamento": "2013-04-24"},
    {"titulo": "pokémon sapphire", "data_lancamento": "2002-11-21"},
    {"titulo": "pokémon scarlet", "data_lancamento": "2022-11-18"},
    {"titulo": "pokémon shield", "data_lancamento": "2019-11-15"},
    {"titulo": "pokémon shining pearl", "data_lancamento": "2021-11-19"},
    {"titulo": "pokémon shuffle", "data_lancamento": "2015-02-18"},
    {"titulo": "pokémon silver", "data_lancamento": "1999-11-21"},
    {"titulo": "pokémon snap", "data_lancamento": "1999-03-21"},
    {"titulo": "pokémon soulsilver", "data_lancamento": "2009-09-12"},
    {"titulo": "pokémon stadium", "data_lancamento": "2000-12-14"},
    {"titulo": "pokémon stadium 2", "data_lancamento": "2000-12-14"},
    {"titulo": "pokémon sun", "data_lancamento": "2016-11-18"},
    {"titulo": "pokémon sword", "data_lancamento": "2019-11-15"},
    {"titulo": "pokémon trading card game", "data_lancamento": "2022-02-17"},
    {"titulo": "pokémon trozei!", "data_lancamento": "2005-10-20"},
    {"titulo": "pokémon unite", "data_lancamento": "2021-07-20"},
    {"titulo": "pokémon ultra moon", "data_lancamento": "2017-11-17"},
    {"titulo": "pokémon ultra sun", "data_lancamento": "2017-11-17"},
    {"titulo": "pokémon violet", "data_lancamento": "2022-11-18"},
    {"titulo": "pokémon white", "data_lancamento": "2012-06-23"},
    {"titulo": "pokémon x", "data_lancamento": "2013-10-12"},
    {"titulo": "pokémon xd: gale of darkness", "data_lancamento": "2005-08-04"},
    {"titulo": "pokémon y", "data_lancamento": "2013-10-12"},
    {"titulo": "pokémon yellow", "data_lancamento": "1998-09-12"}
]

# Lista de títulos de filmes
filmes_pokemon = [
    {"titulo": "pokémon: zoroark - mestre das ilusões", "data_lancamento": "2010-07-10"},
    {"titulo": "pokémon: pikachu's winter vacation 2", "data_lancamento": "1999-12-22"},
    {"titulo": "pokémon: pikachu's winter vacation", "data_lancamento": "1998-12-22"},
    {"titulo": "pokémon: pikachu's rescue adventure", "data_lancamento": "1999-07-17"},
    {"titulo": "pokémon: partner up with pikachu!", "data_lancamento": "2019-04-15"},
    {"titulo": "pokémon: o retorno de mewtwo", "data_lancamento": "2001-08-17"},
    {"titulo": "pokémon: o pesadelo de darkrai", "data_lancamento": "2007-07-14"},
    {"titulo": "pokémon: o mentor do pokémon miragem", "data_lancamento": "2006-04-29"},
    {"titulo": "pokémon: o filme 2000 - o poder de um", "data_lancamento": "1999-07-17"},
    {"titulo": "pokémon: o filme - mewtwo contra-ataca!", "data_lancamento": "1998-07-18"},
    {"titulo": "pokémon: mewtwo - prologue to awakening", "data_lancamento": "2013-07-11"},
    {"titulo": "pokémon: lucario e o mistério de mew", "data_lancamento": "2005-07-16"},
    {"titulo": "pokémon: happy birthday to you!", "data_lancamento": "2017-09-14"},
    {"titulo": "pokémon: gotta dance!", "data_lancamento": "2003-07-19"},
    {"titulo": "pokémon: giratina e o cavaleiro do céu", "data_lancamento": "2008-07-19"},
    {"titulo": "pokémon: eevee & friends", "data_lancamento": "2013-07-13"},
    {"titulo": "pokémon: diancie — princess of the diamond domain", "data_lancamento": "2014-07-17"},
    {"titulo": "pokémon: arceus e a jóia da vida", "data_lancamento": "2009-07-18"},
    {"titulo": "pokémon, o filme: segredos da selva", "data_lancamento": "2020-12-25"},
    {"titulo": "pokémon, o filme: mewtwo contra-ataca - evolução", "data_lancamento": "2019-07-12"},
    {"titulo": "pokémon ranger e o lendário templo do mar", "data_lancamento": "2006-07-15"},
    {"titulo": "pokémon o filme: volcanion e a maravilha mecânica", "data_lancamento": "2016-07-16"},
    {"titulo": "pokémon o filme: preto - victini e reshiram", "data_lancamento": "2011-07-16"},
    {"titulo": "pokémon o filme: o poder de todos", "data_lancamento": "2018-07-13"},
    {"titulo": "pokémon o filme: kyurem contra a espada da justiça", "data_lancamento": "2012-07-13"},
    {"titulo": "pokémon o filme: hoopa e o duelo lendário", "data_lancamento": "2015-07-18"},
    {"titulo": "pokémon o filme: genesect e a lenda revelada", "data_lancamento": "2013-07-13"},
    {"titulo": "pokémon o filme: eu escolho você!", "data_lancamento": "2017-07-15"},
    {"titulo": "pokémon o filme: diancie e o casulo da destruição", "data_lancamento": "2014-07-19"},
    {"titulo": "pokémon o filme: branco - victini e zekrom", "data_lancamento": "2011-07-16"},
    {"titulo": "pokémon mystery dungeon: team go-getters out of the gate!", "data_lancamento": "2006-09-08"},
    {"titulo": "pokémon mystery dungeon: explorers of time & darkness", "data_lancamento": "2007-09-09"},
    {"titulo": "pokémon crônicas: raikou - a lenda do trovão!", "data_lancamento": "2001-12-30"},
    {"titulo": "pokémon 7: alma gêmea", "data_lancamento": "2004-07-22"},
    {"titulo": "pokémon 6: jirachi - realizador de desejos", "data_lancamento": "2003-07-19"},
    {"titulo": "pokémon 4: viajantes do tempo", "data_lancamento": "2001-07-06"},
    {"titulo": "pokémon 3d adventure: find mew!", "data_lancamento": "2005-03-18"},
    {"titulo": "pokémon 3: o feitiço dos unown", "data_lancamento": "2000-07-08"},
    {"titulo": "pikachu and the pokémon music squad", "data_lancamento": "2015-07-18"},
    {"titulo": "outsider: a pokémon horror", "data_lancamento": "2022-01-12"},
    {"titulo": "heróis pokémon: latios & latias", "data_lancamento": "2002-07-13"}
]

# Mapeamento corrigido entre jogos e filmes Pokémon (máximo 1 ano de diferença entre lançamentos)
mapeamento_pokemon_corrigido = [
    {"jogo": "pokémon red", "data_jogo": "1996-02-27", "filme": "pokémon: o filme - mewtwo contra-ataca!", "data_filme": "1998-07-18"},
    {"jogo": "pokémon blue", "data_jogo": "1996-10-15", "filme": "pokémon: o filme - mewtwo contra-ataca!", "data_filme": "1998-07-18"},
    {"jogo": "pokémon green", "data_jogo": "1996-02-27", "filme": "pokémon: o filme - mewtwo contra-ataca!", "data_filme": "1998-07-18"},
    {"jogo": "pokémon yellow", "data_jogo": "1998-09-12", "filme": "pokémon: o filme - mewtwo contra-ataca!", "data_filme": "1998-07-18"},
    
    {"jogo": "pokémon gold", "data_jogo": "1999-11-21", "filme": "pokémon: o filme 2000 - o poder de um", "data_filme": "1999-07-17"},
    {"jogo": "pokémon silver", "data_jogo": "1999-11-21", "filme": "pokémon: o filme 2000 - o poder de um", "data_filme": "1999-07-17"},
    {"jogo": "pokémon pinball", "data_jogo": "1999-04-14", "filme": "pokémon: pikachu's winter vacation", "data_filme": "1998-12-22"},
    {"jogo": "pokémon snap", "data_jogo": "1999-03-21", "filme": "pokémon: pikachu's rescue adventure", "data_filme": "1999-07-17"},
    
    {"jogo": "pokémon stadium", "data_jogo": "2000-12-14", "filme": "pokémon 3: o feitiço dos unown", "data_filme": "2000-07-08"},
    {"jogo": "pokémon stadium 2", "data_jogo": "2000-12-14", "filme": "pokémon 3: o feitiço dos unown", "data_filme": "2000-07-08"},
    {"jogo": "pokémon diamond", "data_jogo": "2000-12-31", "filme": "pokémon 3: o feitiço dos unown", "data_filme": "2000-07-08"},
    
    {"jogo": "pokémon crystal", "data_jogo": "2022-07-03", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"},
    
    {"jogo": "pokémon ruby", "data_jogo": "2002-11-21", "filme": "heróis pokémon: latios & latias", "data_filme": "2002-07-13"},
    {"jogo": "pokémon sapphire", "data_jogo": "2002-11-21", "filme": "heróis pokémon: latios & latias", "data_filme": "2002-07-13"},
    
    {"jogo": "pokémon channel", "data_jogo": "2003-07-18", "filme": "pokémon 6: jirachi - realizador de desejos", "data_filme": "2003-07-19"},
    {"jogo": "pokémon colosseum", "data_jogo": "2003-11-21", "filme": "pokémon 6: jirachi - realizador de desejos", "data_filme": "2003-07-19"},
    {"jogo": "pokémon pinball: ruby & sapphire", "data_jogo": "2003-08-01", "filme": "pokémon: gotta dance!", "data_filme": "2003-07-19"},
    
    {"jogo": "pokémon firered", "data_jogo": "2004-01-29", "filme": "pokémon 7: alma gêmea", "data_filme": "2004-07-22"},
    {"jogo": "pokémon leafgreen", "data_jogo": "2004-01-29", "filme": "pokémon 7: alma gêmea", "data_filme": "2004-07-22"},
    {"jogo": "pokémon dash", "data_jogo": "2004-12-02", "filme": "pokémon 7: alma gêmea", "data_filme": "2004-07-22"},
    
    {"jogo": "pokémon trozei!", "data_jogo": "2005-10-20", "filme": "pokémon: lucario e o mistério de mew", "data_filme": "2005-07-16"},
    {"jogo": "pokémon mystery dungeon: blue rescue team", "data_jogo": "2005-11-17", "filme": "pokémon mystery dungeon: team go-getters out of the gate!", "data_filme": "2006-09-08"},
    {"jogo": "pokémon mystery dungeon: red rescue team", "data_jogo": "2005-11-17", "filme": "pokémon mystery dungeon: team go-getters out of the gate!", "data_filme": "2006-09-08"},
    {"jogo": "pokémon xd: gale of darkness", "data_jogo": "2005-08-04", "filme": "pokémon 3d adventure: find mew!", "data_filme": "2005-03-18"},
    
    {"jogo": "pokémon ranger", "data_jogo": "2006-03-23", "filme": "pokémon ranger e o lendário templo do mar", "data_filme": "2006-07-15"},
    {"jogo": "pokémon pearl", "data_jogo": "2006-09-28", "filme": "pokémon: o mentor do pokémon miragem", "data_filme": "2006-04-29"},
    {"jogo": "pokémon battle revolution", "data_jogo": "2006-12-14", "filme": "pokémon ranger e o lendário templo do mar", "data_filme": "2006-07-15"},
    
    {"jogo": "pokémon mystery dungeon: explorers of darkness", "data_jogo": "2007-09-13", "filme": "pokémon mystery dungeon: explorers of time & darkness", "data_filme": "2007-09-09"},
    {"jogo": "pokémon mystery dungeon: explorers of time", "data_jogo": "2007-09-13", "filme": "pokémon mystery dungeon: explorers of time & darkness", "data_filme": "2007-09-09"},
    
    {"jogo": "pokémon platinum", "data_jogo": "2008-09-13", "filme": "pokémon: giratina e o cavaleiro do céu", "data_filme": "2008-07-19"},
    {"jogo": "pokémon ranger: shadows of almia", "data_jogo": "2008-03-20", "filme": "pokémon: o pesadelo de darkrai", "data_filme": "2007-07-14"},
    
    {"jogo": "pokémon heartgold", "data_jogo": "2009-09-12", "filme": "pokémon: arceus e a jóia da vida", "data_filme": "2009-07-18"},
    {"jogo": "pokémon soulsilver", "data_jogo": "2009-09-12", "filme": "pokémon: arceus e a jóia da vida", "data_filme": "2009-07-18"},
    {"jogo": "pokémon mystery dungeon: explorers of sky", "data_jogo": "2009-04-18", "filme": "pokémon: giratina e o cavaleiro do céu", "data_filme": "2008-07-19"},
    
    {"jogo": "pokémon black", "data_jogo": "2010-09-18", "filme": "pokémon: zoroark - mestre das ilusões", "data_filme": "2010-07-10"},
    {"jogo": "pokémon ranger: guardian signs", "data_jogo": "2010-03-06", "filme": "pokémon: zoroark - mestre das ilusões", "data_filme": "2010-07-10"},
    
    {"jogo": "pokémon rumble blast", "data_jogo": "2011-08-11", "filme": "pokémon o filme: branco - victini e zekrom", "data_filme": "2011-07-16"},
    
    {"jogo": "pokémon conquest", "data_jogo": "2012-03-17", "filme": "pokémon o filme: kyurem contra a espada da justiça", "data_filme": "2012-07-13"},
    {"jogo": "pokémon white", "data_jogo": "2012-06-23", "filme": "pokémon o filme: kyurem contra a espada da justiça", "data_filme": "2012-07-13"},
    {"jogo": "pokémon mystery dungeon: gates to infinity", "data_jogo": "2012-11-23", "filme": "pokémon o filme: kyurem contra a espada da justiça", "data_filme": "2012-07-13"},
    
    {"jogo": "pokémon x", "data_jogo": "2013-10-12", "filme": "pokémon o filme: genesect e a lenda revelada", "data_filme": "2013-07-13"},
    {"jogo": "pokémon y", "data_jogo": "2013-10-12", "filme": "pokémon o filme: genesect e a lenda revelada", "data_filme": "2013-07-13"},
    {"jogo": "pokémon rumble", "data_jogo": "2013-04-24", "filme": "pokémon: eevee & friends", "data_filme": "2013-07-13"},
    {"jogo": "pokémon rumble u", "data_jogo": "2013-04-24", "filme": "pokémon: mewtwo - prologue to awakening", "data_filme": "2013-07-11"},
    
    {"jogo": "pokémon battle trozei", "data_jogo": "2014-03-12", "filme": "pokémon o filme: diancie e o casulo da destruição", "data_filme": "2014-07-19"},
    {"jogo": "pokémon art academy", "data_jogo": "2014-06-19", "filme": "pokémon: diancie — princess of the diamond domain", "data_filme": "2014-07-17"},
    {"jogo": "pokémon alpha sapphire", "data_jogo": "2014-11-21", "filme": "pokémon o filme: diancie e o casulo da destruição", "data_filme": "2014-07-19"},
    {"jogo": "pokémon omega ruby", "data_jogo": "2014-11-21", "filme": "pokémon o filme: diancie e o casulo da destruição", "data_filme": "2014-07-19"},
    
    {"jogo": "pokémon shuffle", "data_jogo": "2015-02-18", "filme": "pokémon o filme: hoopa e o duelo lendário", "data_filme": "2015-07-18"},
    
    {"jogo": "pokémon sun", "data_jogo": "2016-11-18", "filme": "pokémon o filme: volcanion e a maravilha mecânica", "data_filme": "2016-07-16"},
    {"jogo": "pokémon moon", "data_jogo": "2016-11-18", "filme": "pokémon o filme: volcanion e a maravilha mecânica", "data_filme": "2016-07-16"},
    
    {"jogo": "pokémon ultra sun", "data_jogo": "2017-11-17", "filme": "pokémon o filme: eu escolho você!", "data_filme": "2017-07-15"},
    {"jogo": "pokémon ultra moon", "data_jogo": "2017-11-17", "filme": "pokémon o filme: eu escolho você!", "data_filme": "2017-07-15"},
    
    {"jogo": "pokémon quest", "data_jogo": "2018-05-29", "filme": "pokémon o filme: o poder de todos", "data_filme": "2018-07-13"},
    {"jogo": "detective pikachu", "data_jogo": "2018-03-22", "filme": "pokémon o filme: o poder de todos", "data_filme": "2018-07-13"},
    
    {"jogo": "pokémon rumble rush", "data_jogo": "2019-05-15", "filme": "pokémon, o filme: mewtwo contra-ataca - evolução", "data_filme": "2019-07-12"},
    {"jogo": "pokémon sword", "data_jogo": "2019-11-15", "filme": "pokémon: partner up with pikachu!", "data_filme": "2019-04-15"},
    {"jogo": "pokémon shield", "data_jogo": "2019-11-15", "filme": "pokémon: partner up with pikachu!", "data_filme": "2019-04-15"},
    
    {"jogo": "pokémon mystery dungeon: rescue team dx", "data_jogo": "2020-03-06", "filme": "pokémon, o filme: segredos da selva", "data_filme": "2020-12-25"},
    
    {"jogo": "pokémon unite", "data_jogo": "2021-07-20", "filme": "pokémon, o filme: segredos da selva", "data_filme": "2020-12-25"},
    {"jogo": "pokémon brilliant diamond", "data_jogo": "2021-11-19", "filme": "pokémon, o filme: segredos da selva", "data_filme": "2020-12-25"},
    {"jogo": "pokémon shining pearl", "data_jogo": "2021-11-19", "filme": "pokémon, o filme: segredos da selva", "data_filme": "2020-12-25"},
    {"jogo": "new pokémon snap", "data_jogo": "2021-04-29", "filme": "pokémon, o filme: segredos da selva", "data_filme": "2020-12-25"},
    
    {"jogo": "pokémon legends: arceus", "data_jogo": "2022-01-28", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"},
    {"jogo": "pokémon scarlet", "data_jogo": "2022-11-18", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"},
    {"jogo": "pokémon violet", "data_jogo": "2022-11-18", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"},
    {"jogo": "pokémon emerald", "data_jogo": "2022-07-15", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"},
    {"jogo": "pokémon trading card game", "data_jogo": "2022-02-17", "filme": "outsider: a pokémon horror", "data_filme": "2022-01-12"}
]

# Criando Dimensão Jogos com id_filme associado
window_spec_games = Window.orderBy(col("titulo"))
games_df = games_df.withColumn("id_jogo", row_number().over(window_spec_games))

# Convertendo as datas de lançamento para o formato correto
games_df = games_df.withColumn("data_lancamento", to_date(col("data_lancamento"), "yyyy-MM-dd"))
df_final = df_final.withColumn("data_lancamento", to_date(col("data_lancamento"), "yyyy-MM-dd"))

# Inicialize id_filme em games_df se ela não existir
if "id_filme" not in games_df.columns:
    games_df = games_df.withColumn("id_filme", lit(None).cast(IntegerType()))

# Associando os jogos aos filmes usando a lista de mapeamento
for item in mapeamento_pokemon:
    # Normalizar o título do filme
    filme_titulo_normalizado = item["filme"].lower().strip()
    
    # Encontrar o id_filme correspondente ao filme
    filme_info = next((f for f in filmes_pokemon if f["titulo"].lower().strip() == filme_titulo_normalizado), None)
    
    if filme_info:
        # Encontrar o id_filme no DataFrame df_final
        filme_df = df_final.filter(
            (col("titulo") == filme_titulo_normalizado) & 
            (col("data_lancamento") == to_date(lit(filme_info["data_lancamento"]), "yyyy-MM-dd"))
        )
        
        if filme_df.count() > 0:
            id_filme = filme_df.select("id_filme").first()[0]
            
            # Normalizar o título do jogo
            jogo_titulo_normalizado = item["jogo"].lower().strip()
            
            # Encontrar a data de lançamento do jogo
            jogo_info = next((j for j in jogos_pokemon if j["titulo"].lower().strip() == jogo_titulo_normalizado), None)
            
            if jogo_info:
                # Associar o id_filme ao jogo correspondente
                games_df = games_df.withColumn(  # <- AGORA ESTÁ INDENTADO CORRETAMENTE
                    "id_filme",
                    when(
                        (lower(trim(col("titulo"))) == jogo_titulo_normalizado) & 
                        (col("data_lancamento") == to_date(lit(jogo_info["data_lancamento"]), "yyyy-MM-dd")),
                        lit(id_filme)
                    ).otherwise(col("id_filme"))
                )

# Selecionando as colunas para a dim_jogo
dim_jogo_df = games_df.select(
    col("id_jogo"),
    col("titulo").alias("titulo_jogo"),  # Renomeando para evitar conflito
    col("data_lancamento").alias("data_lancamento_jogo"),  # Renomeando para evitar conflito
    col("id_filme")  # Adicionando a relação com o filme
).distinct()

# Salvando os dados na Refined Zone
dim_tempo_df.write.mode("overwrite").parquet(refined_dim_tempo_path)
dim_franquia_df.write.mode("overwrite").parquet(refined_dim_franquia_path)
dim_jogo_df.write.mode("overwrite").parquet(refined_dim_jogo_path)
df_final.write.mode("overwrite").partitionBy("data_lancamento").parquet(refined_filmes_path)