# bibliotecas usadas

import pandas as pd
import boto3
from botocore.exceptions import ClientError

# Caminho do arquivo CSV
file_path = "DADOS_ABERTOS_MEDICAMENTOS.csv"

# Releitura do arquivo original com separador e codificação apropriados
def remover_duplicatas(file_path):
    try:
        data_original = pd.read_csv(file_path, sep=";", encoding="ISO-8859-1", on_bad_lines="skip")

        # Nome da coluna usada para remover duplicatas
        coluna_chave = "NUMERO_PROCESSO"

        # Quantidade total de linhas antes de remover duplicatas
        total_linhas_antes = data_original.shape[0]

        # Removendo duplicatas com base na coluna especificada
        data_sem_duplicatas = data_original.drop_duplicates(subset=[coluna_chave])

        # Quantidade total de linhas depois de remover duplicatas
        total_linhas_depois = data_sem_duplicatas.shape[0]

        # Quantidade de duplicatas removidas
        duplicatas = total_linhas_antes - total_linhas_depois

        # Exibindo mensagens
        print(f"Quantidade total de linhas antes da remoção de duplicatas: {total_linhas_antes}")
        print(f"Quantidade total de linhas depois da remoção de duplicatas: {total_linhas_depois}")
        print(f"Quantidade de duplicatas removidas: {duplicatas}")

        return data_sem_duplicatas
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")

# Configuração do cliente S3
s3 = boto3.client('s3')

# Nome do bucket e região
nome_bucket = 'desafio-sprint5pb'
regiao = 'us-east-1'

def criando_bucket(nome_bucket, regiao='us-east-1'):
    try:
        # Listar buckets existentes
        response = s3.list_buckets()
        buckets_existentes = [bucket['Name'] for bucket in response.get('Buckets', [])]

        # Verificar se o bucket já existe
        if nome_bucket in buckets_existentes:
            print(f"O bucket '{nome_bucket}' já existe.")
        else:
            if regiao == 'us-east-1':
                # Criar bucket sem a configuração de região (us-east-1 é a região padrão)
                s3.create_bucket(Bucket=nome_bucket)
            else:
                # Criar bucket com a configuração de região
                s3.create_bucket(
                    Bucket=nome_bucket,
                    CreateBucketConfiguration={'LocationConstraint': regiao}
                )
            print(f"Bucket '{nome_bucket}' criado com sucesso na região {regiao}.")
    except ClientError as e:
        print(f"Erro ao criar ou verificar o bucket: {e}")


def upload_arquivo(nome_bucket, file_path, s3_key):
    try:
        s3.upload_file(file_path, nome_bucket, s3_key)
        print(f"Arquivo '{file_path}' enviado com sucesso para o bucket '{nome_bucket}' como '{s3_key}'.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo: {e}")

if __name__ == "__main__":
    # Remover duplicatas
    data_sem_duplicatas = remover_duplicatas(file_path)

    # Criar bucket no S3 se não existir
    criando_bucket(nome_bucket, regiao)

    # Fazer upload do arquivo
    s3_key = 'DADOS_ABERTOS_MEDICAMENTOS.csv'
    upload_arquivo(nome_bucket, file_path, s3_key)
