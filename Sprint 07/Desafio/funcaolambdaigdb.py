import json
import boto3
import requests
import os
import time
from datetime import datetime

# Configurações
CLIENT_ID = os.getenv('IGDB_CLIENT_ID')  # Client ID da IGDB
ACCESS_TOKEN = os.getenv('IGDB_ACCESS_TOKEN')  # Access Token da IGDB
BUCKET_S3 = "data-lake-sprint6"  # Nome do bucket S3
CLIENTE_S3 = boto3.client('s3')

# Lista completa de jogos Pokémon
pokemon_games = [
    "Pokémon Red", "Pokémon Blue", "Pokémon Green", "Pokémon Yellow", "Pokémon Gold", 
    "Pokémon Silver", "Pokémon Crystal", "Pokémon Ruby", "Pokémon Sapphire", "Pokémon Emerald", 
    "Pokémon FireRed", "Pokémon LeafGreen", "Pokémon Diamond", "Pokémon Pearl", "Pokémon Platinum", 
    "Pokémon HeartGold", "Pokémon SoulSilver", "Pokémon Black", "Pokémon White", "Pokémon Black 2", 
    "Pokémon White 2", "Pokémon X", "Pokémon Y", "Pokémon Omega Ruby", "Pokémon Alpha Sapphire", 
    "Pokémon Sun", "Pokémon Moon", "Pokémon Ultra Sun", "Pokémon Ultra Moon", 
    "Pokémon Let's Go Pikachu", "Pokémon Let's Go Eevee", "Pokémon Sword", "Pokémon Shield", 
    "Pokémon Legends: Arceus", "Pokémon Brilliant Diamond", "Pokémon Shining Pearl", 
    "Pokémon Scarlet", "Pokémon Violet", "Pokémon GO",
    "Pokémon Mystery Dungeon: Red Rescue Team", "Pokémon Mystery Dungeon: Blue Rescue Team", 
    "Pokémon Mystery Dungeon: Explorers of Time", "Pokémon Mystery Dungeon: Explorers of Darkness", 
    "Pokémon Mystery Dungeon: Explorers of Sky", "Pokémon Mystery Dungeon: Gates to Infinity", 
    "Pokémon Mystery Dungeon: Rescue Team DX", "Pokémon Super Mystery Dungeon",
    "Pokémon Colosseum", "Pokémon XD: Gale of Darkness", "Pokémon Battle Revolution", 
    "Pokémon Stadium", "Pokémon Stadium 2", "Pokémon Rumble", "Pokémon Rumble Blast", 
    "Pokémon Rumble U", "Pokémon Rumble World", "Pokémon Rumble Rush", 
    "Pokémon Trozei!", "Pokémon Battle Trozei", "Pokémon Shuffle", 
    "Pokémon Quest", "Pokémon Café ReMix", "Pokémon Snap", "New Pokémon Snap", 
    "Pokémon Pinball", "Pokémon Pinball: Ruby & Sapphire", "Pokémon Trading Card Game", 
    "Pokémon Trading Card Game 2", "Pokémon Art Academy", 
    "Detective Pikachu", "Detective Pikachu Returns", "Pokémon Ranger", "Pokémon Ranger: Shadows of Almia", 
    "Pokémon Ranger: Guardian Signs", "Pokémon Dash", "Pokémon Channel", "Pokémon Conquest", 
    "Pokémon Masters EX", "Pokémon UNITE"
]

# Headers para autenticação na API IGDB
headers = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

def buscar_datas_lancamento():
    """Busca as datas de lançamento dos jogos Pokémon na API IGDB."""
    games_data = []

    for game in pokemon_games:
        print(f"🔎 Buscando {game}...")
        query = f'search "{game}"; fields name, first_release_date; limit 50;'
        response = requests.post("https://api.igdb.com/v4/games", headers=headers, data=query)

        if response.status_code == 200:
            game_info = response.json()
            if game_info:
                for entry in game_info:
                    name = entry.get("name", "")
                    release_date = entry.get("first_release_date", None)
                    
                    if game.lower() in name.lower() and release_date:
                        release_date = datetime.utcfromtimestamp(release_date).strftime('%Y-%m-%d')
                        year = int(release_date[:4])
                        if 1996 <= year <= 2022:
                            games_data.append({"game": game, "release_date": release_date})
                            print(f"✅ {game} - {release_date}")
                            break
            else:
                print(f" {game} não encontrado.")
        else:
            print(f" Erro na API: {response.status_code}")
            print(f"Resposta da API: {response.text}")

        time.sleep(1)

    return games_data

def salvar_no_s3(dados):
    """Salva os dados no S3 em formato JSON."""
    agora = datetime.utcnow()
    caminho_base = f"Raw/IGDB/JSON/{agora.strftime('%Y/%m/%d')}/"
    nome_arquivo = "pokemon_games_release_dates.json"

    CLIENTE_S3.put_object(
        Bucket=BUCKET_S3,
        Key=caminho_base + nome_arquivo,
        Body=json.dumps(dados, indent=4, ensure_ascii=False),
        ContentType="application/json"
    )

    return caminho_base + nome_arquivo

def lambda_handler(event, context):
    """Função principal do Lambda."""
    print("Iniciando a busca de datas de lançamento dos jogos Pokémon...")

    dados_jogos = buscar_datas_lancamento()

    if dados_jogos:
        caminho_arquivo = salvar_no_s3(dados_jogos)
        print(f" Dados salvos em: s3://{BUCKET_S3}/{caminho_arquivo}")

    return {
        "statusCode": 200,
        "mensagem": f"Ingestão concluída! Processados {len(dados_jogos)} jogos Pokémon."
    }
