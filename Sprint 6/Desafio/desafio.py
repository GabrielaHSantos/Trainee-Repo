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
CAMINHO_RAW = "Raw/Local/CSV"

def criar_bucket(nome_bucket, regiao="us-east-1"):
    
    #Cria o bucket no S3 se ele não existir.

    try:
        # Listar buckets existentes
        resposta = s3.list_buckets()
        buckets_existentes = [bucket['Name'] for bucket in resposta.get("Buckets", [])]

        # Verificar se o bucket já existe
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

def enviar_para_s3(arquivo_zip, tipo_arquivo):
    # Carrega um arquivo CSV diretamente do ZIP para o S3.
    try:
        # Extrair informações de data para o caminho no S3
        agora = datetime.now()
        ano = agora.strftime("%Y")
        mes = agora.strftime("%m")
        dia = agora.strftime("%d")

        # Abrir o arquivo ZIP
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            # Filtrar arquivos CSV dentro do ZIP
            arquivos_csv = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not arquivos_csv:
                print("Nenhum arquivo CSV encontrado dentro do ZIP!")
                return

            # Enviar cada arquivo CSV para o S3
            for arquivo in arquivos_csv:
                with zip_ref.open(arquivo) as file:
                    # Definir o caminho no S3
                    chave_s3 = f"{CAMINHO_RAW}/{tipo_arquivo}/{ano}/{mes}/{dia}/{os.path.basename(arquivo)}"
                    # Fazer o upload do arquivo para o S3
                    s3.upload_fileobj(file, NOME_BUCKET, chave_s3)
                    print(f"Arquivo '{arquivo}' carregado para '{chave_s3}' com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar arquivos do ZIP para o S3: {e}")

def main():
    # Configurar o arquivo ZIP
    caminho_zip = "Filmes+e+Series.zip"
    print(f"Arquivo a ser processado: {caminho_zip}")

    # Verificar se o arquivo realmente existe no diretório atual
    if os.path.exists(caminho_zip):
        print(f"Arquivo '{caminho_zip}' encontrado.")
    else:
        print(f"Arquivo '{caminho_zip}' não encontrado!")
        return

    # Criar o bucket no S3
    criar_bucket(NOME_BUCKET, REGIAO)

    # Enviar os arquivos CSV do ZIP para o S3
    enviar_para_s3(caminho_zip, "Filmes_e_Series")

if __name__ == "__main__":
    main()
