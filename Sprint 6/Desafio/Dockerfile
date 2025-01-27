# Usar uma imagem oficial do Python como base
FROM python:3.9-slim

# Instalar boto3
RUN pip install boto3

# Configurar o diretório de trabalho dentro do container
WORKDIR /app

# Copiar todos os arquivos do diretório local para o container
COPY . /app

# Comando padrão para executar o script
CMD ["python", "desafio.py"]
