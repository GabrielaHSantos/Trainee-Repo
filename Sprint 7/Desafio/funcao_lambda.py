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

def obter_dados_tmdb(genero, ano_limite="2022", eh_serie=False):
    # consulta a API TMDB para buscar filmes ou séries de um determinado gênero até um ano limite
    dados = []
    ids_processados = set()
    tipo_conteudo = "tv" if eh_serie else "movie"

    for pagina in range(1, MAX_PAGINAS + 1):
        url = f"{URL_BASE}/discover/{tipo_conteudo}?api_key={CHAVE_API}&language=pt-BR&with_genres={genero}&page={pagina}&sort_by=popularity.desc&primary_release_year={ano_limite}"
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

def salvar_no_s3(dados):
    # salva os dados no Amazon S3, dividindo-os em arquivos se for necessário
    agora = datetime.utcnow()
    caminho_base = f"Raw/TMDB/JSON/{agora.strftime('%Y/%m/%d')}/"
    partes_dados = [dados[i:i + MAX_REGISTROS] for i in range(0, len(dados), MAX_REGISTROS)]
    caminhos_arquivos = []
    
    for idx, parte in enumerate(partes_dados):
        nome_arquivo = f"filmes_series_{idx+1}.json"
        json_dados = json.dumps(parte, indent=4)
        
        if len(json_dados.encode('utf-8')) > MAX_TAMANHO_ARQUIVO:
            print(f"Arquivo {nome_arquivo} excedeu 10MB, ajustando tamanho...")
            parte = parte[:MAX_REGISTROS-1]
        
        CLIENTE_S3.put_object(
            Bucket=BUCKET_S3,
            Key=caminho_base + nome_arquivo,
            Body=json.dumps(parte, indent=4),
            ContentType="application/json"
        )
        
        caminhos_arquivos.append(caminho_base + nome_arquivo)
    
    return caminhos_arquivos

def lambda_handler(event, context):
    print("Iniciando a ingestão de dados da TMDB...")
    
    dados_animacao_filmes = obter_dados_tmdb(genero="16", ano_limite="2022", eh_serie=False)
    dados_comedia_filmes = obter_dados_tmdb(genero="35", ano_limite="2022", eh_serie=False)
    dados_animacao_series = obter_dados_tmdb(genero="16", ano_limite="2022", eh_serie=True)
    dados_comedia_series = obter_dados_tmdb(genero="35", ano_limite="2022", eh_serie=True)
    
    total_dados = len(dados_animacao_filmes) + len(dados_comedia_filmes) + len(dados_animacao_series) + len(dados_comedia_series)
    total_animacao = len(dados_animacao_filmes) + len(dados_animacao_series)
    total_comedia = len(dados_comedia_filmes) + len(dados_comedia_series)
    
    print(f"Total de dados processados: {total_dados}")
    print(f"Dados de animação: {total_animacao}")
    print(f"Dados de comédia: {total_comedia}")
    
    dados_combinados = dados_animacao_filmes + dados_comedia_filmes + dados_animacao_series + dados_comedia_series
    
    if dados_combinados:
        caminhos = salvar_no_s3(dados_combinados)
        print(f"Dados salvos em: {', '.join(caminhos)}")
    
    return {
        "statusCode": 200,
        "mensagem": f"Ingestão concluída! Processados {total_animacao} dados de animação e {total_comedia} dados de comédia."
    }
