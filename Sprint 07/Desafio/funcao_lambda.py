import json
import boto3
import requests
import os
import asyncio
from datetime import datetime
import re  

# Configurações
CHAVE_API = os.getenv('TMDB_API_KEY')
URL_BASE = "https://api.themoviedb.org/3"
BUCKET_S3 = "data-lake-sprint6"
CLIENTE_S3 = boto3.client('s3')
MAX_REGISTROS = 100
MAX_TAMANHO_ARQUIVO = 10 * 1024 * 1024  # 10 MB
MAX_PAGINAS = 50
ANO_LIMITE = "2022-12-31"

async def buscar_detalhes_assincrono(ids):
    """Busca detalhes de filmes em lote de forma assíncrona."""
    urls = [f"{URL_BASE}/movie/{id}?api_key={CHAVE_API}&language=pt-BR" for id in ids]
    
    async def fetch_url(url):
        resposta = requests.get(url)
        if resposta.status_code == 200:
            dados = resposta.json()
            return {
                'id': dados.get('id'),
                'budget': dados.get('budget', 0) or 0,  # Garantindo valor numérico
                'revenue': dados.get('revenue', 0) or 0  # Garantindo valor numérico
            }
        return None

    tarefas = [fetch_url(url) for url in urls]
    respostas = await asyncio.gather(*tarefas)
    return [res for res in respostas if res]

def obter_dados_tmdb():
    """Obtém dados de filmes de animação do TMDB até 2022."""
    dados = []
    ids_processados = set()
    genero_animacao = "16"
    ids_para_buscar = []
    regex_nome_valido = re.compile(r'[A-Za-z\s]')
    
    for pagina in range(1, MAX_PAGINAS + 1):
        url = (f"{URL_BASE}/discover/movie?api_key={CHAVE_API}&language=pt-BR&with_genres={genero_animacao}" 
               f"&primary_release_date.lte={ANO_LIMITE}&page={pagina}&sort_by=popularity.desc")
        resposta = requests.get(url)
        
        if resposta.status_code == 200:
            dados_pagina = resposta.json().get("results", [])
            if not dados_pagina:
                break
            
            for item in dados_pagina:
                nome = item.get('title')
                if not nome or not regex_nome_valido.search(nome):
                    continue  # Ignora itens sem nome válido
                
                if item['id'] not in ids_processados:
                    ids_para_buscar.append(item['id'])
                    item['tipo_conteudo'] = "movie"
                    dados.append(item)
                    ids_processados.add(item['id'])
        else:
            print(f"Erro ao buscar dados: {resposta.status_code}")
            break
    
    if ids_para_buscar:
        loop = asyncio.get_event_loop()
        dados_completos = loop.run_until_complete(buscar_detalhes_assincrono(ids_para_buscar))
        
        for item in dados:
            for detalhado in dados_completos:
                if item['id'] == detalhado['id']:
                    item['budget'] = detalhado['budget']
                    item['revenue'] = detalhado['revenue']
    
    return dados

def buscar_titulos_pokemon():
    """Busca filmes de Pokémon no TMDB até 2022."""
    dados_pokemon = []
    ids_processados = set()
    ids_para_buscar = []

    for pagina in range(1, MAX_PAGINAS + 1):
        url = f"{URL_BASE}/search/movie?api_key={CHAVE_API}&language=pt-BR&query=pokemon&page={pagina}"
        resposta = requests.get(url)
        
        if resposta.status_code == 200:
            resultados = resposta.json().get("results", [])
            for item in resultados:
                if "Pokémon" in item.get('title', ''):
                    data_lancamento = item.get('release_date')
                    if data_lancamento and data_lancamento[:4] <= "2022" and item['id'] not in ids_processados:
                        item['tipo_conteudo'] = "movie"
                        dados_pokemon.append(item)
                        ids_para_buscar.append(item['id'])
                        ids_processados.add(item['id'])
        else:
            print(f"Erro ao buscar dados da página {pagina}: {resposta.status_code}")
            break

    if ids_para_buscar:
        loop = asyncio.get_event_loop()
        dados_completos = loop.run_until_complete(buscar_detalhes_assincrono(ids_para_buscar))
        
        for item in dados_pokemon:
            for detalhado in dados_completos:
                if item['id'] == detalhado['id']:
                    item['budget'] = detalhado['budget']
                    item['revenue'] = detalhado['revenue']
    
    return dados_pokemon

def salvar_no_s3(dados):
    """Salva os dados no S3."""
    agora = datetime.utcnow()
    caminho_base = f"Raw/TMDB/JSON/{agora.strftime('%Y/%m/%d')}/"
    partes_dados = [dados[i:i + MAX_REGISTROS] for i in range(0, len(dados), MAX_REGISTROS)]
    caminhos_arquivos = []

    for idx, parte in enumerate(partes_dados):
        nome_arquivo = f"filmes_{idx + 1}.json"
        json_dados = json.dumps(parte, indent=4)

        if len(json_dados.encode('utf-8')) > MAX_TAMANHO_ARQUIVO:
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
    """Função principal para ingestão de filmes."""
    print("Iniciando a ingestão de dados da TMDB para filmes de animação...")

    dados_filmes = obter_dados_tmdb()
    dados_pokemon = buscar_titulos_pokemon()

    total_filmes = len(dados_filmes)
    total_pokemon = len(dados_pokemon)

    print(f"Total de filmes processados: {total_filmes}")
    print(f"Total de títulos Pokémon processados: {total_pokemon}")

    dados_combinados = dados_filmes + dados_pokemon

    if dados_combinados:
        caminhos = salvar_no_s3(dados_combinados)
        print(f"Dados salvos em: {', '.join(caminhos)}")

    return {
        "statusCode": 200,
        "mensagem": f"Ingestão concluída! Processados {total_filmes} filmes e {total_pokemon} títulos Pokémon."
    }
