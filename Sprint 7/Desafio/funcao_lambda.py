import json
import boto3
import requests
import os
from datetime import datetime

# Configurações
CHAVE_API = os.getenv('TMDB_API_KEY')
URL_BASE = "https://api.themoviedb.org/3"
BUCKET_S3 = "data-lake-sprint6"
CLIENTE_S3 = boto3.client('s3')
MAX_REGISTROS = 100
MAX_TAMANHO_ARQUIVO = 10 * 1024 * 1024
MAX_PAGINAS = 50
ANO_LIMITE = "2022-12-31"

def obter_dados_tmdb(eh_serie=False):
    dados = []
    ids_processados = set()
    tipo_conteudo = "tv" if eh_serie else "movie"
    genero_animacao = "16"
    data_filtro = "first_air_date.lte" if eh_serie else "release_date.lte"
    
    for pagina in range(1, MAX_PAGINAS + 1):
        url = (f"{URL_BASE}/discover/{tipo_conteudo}?api_key={CHAVE_API}&language=pt-BR&with_genres={genero_animacao}" 
               f"&{data_filtro}={ANO_LIMITE}&page={pagina}&sort_by=popularity.desc")
        resposta = requests.get(url)
        
        if resposta.status_code == 200:
            dados_pagina = resposta.json().get("results", [])
            if not dados_pagina:
                break
            
            for item in dados_pagina:
                if item['id'] not in ids_processados:
                    dados.append(item)
                    ids_processados.add(item['id'])
        else:
            print(f"Erro ao buscar dados: {resposta.status_code}")
            break

    return dados

def buscar_titulos_pokemon():
    dados_pokemon = []
    ids_processados = set()

    for pagina in range(1, MAX_PAGINAS + 1):
        url = f"{URL_BASE}/search/multi?api_key={CHAVE_API}&language=pt-BR&query=pokemon&page={pagina}"
        resposta = requests.get(url)
        
        if resposta.status_code == 200:
            resultados = resposta.json().get("results", [])
            for item in resultados:
                if "Pokémon" in item.get('title', '') or "Pokémon" in item.get('name', ''):
                    data_lancamento = item.get('release_date') or item.get('first_air_date')
                    if data_lancamento and data_lancamento[:4] <= "2022" and item['id'] not in ids_processados:
                        dados_pokemon.append(item)
                        ids_processados.add(item['id'])
        else:
            print(f"Erro ao buscar dados da página {pagina}: {resposta.status_code}")
            break

    print(f"Total de títulos Pokémon encontrados: {len(dados_pokemon)}")
    return dados_pokemon

def salvar_no_s3(dados):
    agora = datetime.utcnow()
    caminho_base = f"Raw/TMDB/JSON/{agora.strftime('%Y/%m/%d')}/"
    partes_dados = [dados[i:i + MAX_REGISTROS] for i in range(0, len(dados), MAX_REGISTROS)]
    caminhos_arquivos = []

    for idx, parte in enumerate(partes_dados):
        nome_arquivo = f"filmes_e_series_{idx + 1}.json"
        json_dados = json.dumps(parte, indent=4)

        if len(json_dados.encode('utf-8')) > MAX_TAMANHO_ARQUIVO:
            print(f"Arquivo {nome_arquivo} excedeu 10MB, ajustando tamanho...")
            parte = parte[:MAX_REGISTROS - 1]

        CLIENTE_S3.put_object(
            Bucket=BUCKET_S3,
            Key=caminho_base + nome_arquivo,
            Body=json.dumps(parte, indent=4),
            ContentType="application/json"
        )

        caminhos_arquivos.append(caminho_base + nome_arquivo)

    return caminhos_arquivos

def lambda_handler(event, context):
    print("Iniciando a ingestão de dados da TMDB para animações...")

    dados_filmes = obter_dados_tmdb(eh_serie=False)
    dados_series = obter_dados_tmdb(eh_serie=True)
    dados_pokemon = buscar_titulos_pokemon()

    total_animacao = len(dados_filmes) + len(dados_series)
    total_pokemon = len(dados_pokemon)

    print(f"Total de dados de animação processados: {total_animacao}")
    print(f"Total de títulos Pokémon processados: {total_pokemon}")

    dados_combinados = dados_filmes + dados_series + dados_pokemon

    if dados_combinados:
        caminhos = salvar_no_s3(dados_combinados)
        print(f"Dados salvos em: {', '.join(caminhos)}")

    return {
        "statusCode": 200,
        "mensagem": f"Ingestão concluída! Processados {total_animacao} dados de animação e {total_pokemon} títulos Pokémon."
    }
