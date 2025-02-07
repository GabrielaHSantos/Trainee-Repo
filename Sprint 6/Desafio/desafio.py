import os
import zipfile
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Configuração do cliente S3
s3 = boto3.client('s3')

# Nome do bucket e região
NOME_BUCKET = "data-lake-sprint6"
REGIAO = "us-east-1"

def criar_bucket(nome_bucket, regiao="us-east-1"):
    """
    Cria o bucket no S3 se ele não existir.
    """
    try:
        resposta = s3.list_buckets()
        buckets_existentes = [bucket['Name'] for bucket in resposta.get("Buckets", [])]

        if nome_bucket in buckets_existentes:
            print(f"O bucket '{nome_bucket}' já existe.")
        else:
            if regiao == "us-east-1":
                s3.create_bucket(Bucket=nome_bucket)
            else:
                s3.create_bucket(
                    Bucket=nome_bucket,
                    CreateBucketConfiguration={"LocationConstraint": regiao},
                )
            print(f"Bucket '{nome_bucket}' criado com sucesso na região {regiao}.")
    except ClientError as e:
        print(f"Erro ao criar/verificar o bucket: {e}")

def enviar_para_s3(arquivo_zip):
    """
    Extrai arquivos CSV do ZIP e envia para o S3,
    armazenando-os em diretórios separados (Filmes ou Series) conforme o nome do arquivo.
    """
    try:
        agora = datetime.now()
        ano, mes, dia = agora.strftime("%Y"), agora.strftime("%m"), agora.strftime("%d")

        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            arquivos_csv = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not arquivos_csv:
                print("Nenhum arquivo CSV encontrado dentro do ZIP!")
                return

            for arquivo in arquivos_csv:
                nome_arquivo = os.path.basename(arquivo).lower()

                if 'movie' in nome_arquivo or 'filme' in nome_arquivo:
                    pasta = "Filmes"
                elif 'series' in nome_arquivo or 'serie' in nome_arquivo:
                    pasta = "Series"
                else:
                    print(f"Tipo de arquivo não identificado para '{nome_arquivo}'. Pulando.")
                    continue

                chave_s3 = f"Raw/Local/CSV/{pasta}/{ano}/{mes}/{dia}/{os.path.basename(arquivo)}"

                with zip_ref.open(arquivo) as file:
                    s3.upload_fileobj(file, NOME_BUCKET, chave_s3)
                    print(f"Arquivo '{nome_arquivo}' enviado para '{chave_s3}' com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar arquivos para o S3: {e}")

def main():
    caminho_zip = "Filmes+e+Series.zip"
    print(f"Arquivo a ser processado: {caminho_zip}")

    if not os.path.exists(caminho_zip):
        print(f"Arquivo '{caminho_zip}' não encontrado!")
        return

    criar_bucket(NOME_BUCKET, REGIAO)
    enviar_para_s3(caminho_zip)

if __name__ == "__main__":
    main()
