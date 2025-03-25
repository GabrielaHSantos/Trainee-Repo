# Bibliotecas usadas
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import unicodedata

# Nome do bucket e chave do arquivo
nome_bucket = 'desafio-sprint5pb'
s3_key = 'DADOS_ABERTOS_MEDICAMENTOS.csv'

def normalizar_string(texto):
    # Remove acentos e normaliza o texto.
    if isinstance(texto, str):
        return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8').strip()
    return texto

def manipular_dados_s3(nome_bucket, s3_key):

    # Carrega os dados do S3, aplica as manipulações exigidas e retorna o DataFrame processado.

    s3 = boto3.client('s3')
    
    # Baixar o arquivo do S3 com codificação ISO-8859-1

    response = s3.get_object(Bucket=nome_bucket, Key=s3_key)
    csv_data = response['Body'].read().decode('ISO-8859-1')
    
    # Criar o DataFrame usando ponto e vírgula como separador

    df = pd.read_csv(StringIO(csv_data), sep=';', on_bad_lines="skip")

    # Limpar espaços nos nomes das colunas

    df.columns = df.columns.str.strip()  
    
    # Normalizar todas as strings no DataFrame

    for coluna in df.select_dtypes(include=['object']).columns:
        df[coluna] = df[coluna].apply(normalizar_string)
    
    # Validar se as colunas necessárias existem

    colunas_necessarias = ['TIPO_PRODUTO', 'SITUACAO_REGISTRO']
    for coluna in colunas_necessarias:
        if coluna not in df.columns:
            raise ValueError(f"Coluna necessária '{coluna}' não encontrada no DataFrame.")
    
    # Filtrar dados com pelo menos dois operadores lógicos

    filtro = (df['TIPO_PRODUTO'] == 'MEDICAMENTO') & (df['SITUACAO_REGISTRO'] == 'VALIDO')
    df_filtrado = df[filtro].copy()
    
    # Função de agregação 

    agregados = df_filtrado.groupby('TIPO_PRODUTO').agg(
        total_registros=('NUMERO_REGISTRO_PRODUTO', 'count'),
        total_produtos=('NOME_PRODUTO', 'nunique')
    )
    
    # Função condicional

    df_filtrado['REGISTRO_VALIDO'] = df_filtrado['SITUACAO_REGISTRO'].apply(lambda x: 'Sim' if x == 'VALIDO' else 'Não')
    
    # Função de conversão

    df_filtrado['DATA_VENCIMENTO_REGISTRO'] = pd.to_datetime(df_filtrado['DATA_VENCIMENTO_REGISTRO'], errors='coerce')
    
    # Função de data - Extrair o ano de 'DATA_VENCIMENTO_REGISTRO'

    df_filtrado['ANO_REGISTRO'] = df_filtrado['DATA_VENCIMENTO_REGISTRO'].dt.year.astype('Int64')
    
    # Função de string

    df_filtrado['CLASSE_TERAPEUTICA_NORMALIZADA'] = df_filtrado['CLASSE_TERAPEUTICA'].str.lower().str.replace(' ', '_')

    if df_filtrado.empty:
        print("Nenhum registro encontrado após o filtro. Verifique os dados.")
    else:
        print(f"Total de registros após filtro: {df_filtrado.shape[0]}")
    
    return df_filtrado

def upload_novo_arquivo(nome_bucket, df_resultante, nome_arquivo_original):

    # Faz o upload do DataFrame processado de volta para o S3.

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    novo_s3_key = nome_arquivo_original.replace('.csv', f'_NOVO_{timestamp}.csv')
    
    # Salvar o DataFrame em um buffer de string com separador ';'

    csv_buffer = StringIO()
    df_resultante.to_csv(csv_buffer, index=False, sep=';', encoding='utf-8')  
    
    # Fazer upload do arquivo para o S3

    s3 = boto3.client('s3')
    s3.put_object(Bucket=nome_bucket, Key=novo_s3_key, Body=csv_buffer.getvalue())
    print(f"Arquivo processado enviado para o S3 como '{novo_s3_key}'.")

if __name__ == "__main__":

    # Executar manipulação

    try:
        df_resultante = manipular_dados_s3(nome_bucket, s3_key)
        
        if not df_resultante.empty:

            # Faz upload do arquivo processado

            upload_novo_arquivo(nome_bucket, df_resultante, s3_key)
        else:
            print("O DataFrame filtrado está vazio. Não será enviado ao S3.")
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
