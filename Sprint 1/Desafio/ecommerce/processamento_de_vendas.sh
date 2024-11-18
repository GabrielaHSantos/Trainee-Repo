#!/bin/bash

# Diretórios e arquivos
VENDAS_DIR="vendas"
BACKUP_DIR="$VENDAS_DIR/backup"
DATA_ATUAL=$(date '+%Y%m%d')
HORA_ATUAL=$(date '+%H%M')
ARQUIVO_ORIGINAL="dados_de_vendas.csv"
ARQUIVO_BACKUP="$BACKUP_DIR/dados-$DATA_ATUAL-$HORA_ATUAL.csv"
ARQUIVO_BACKUP_ZIP="$BACKUP_DIR/backup-dados-$DATA_ATUAL-$HORA_ATUAL.zip"
ARQUIVO_VENDAS="$VENDAS_DIR/dados-$DATA_ATUAL-$HORA_ATUAL.csv"
RELATORIO="$BACKUP_DIR/relatorio-$DATA_ATUAL-$HORA_ATUAL.txt"

# Criar o diretório de backup
mkdir -p "$BACKUP_DIR"

# Criar backup com data e hora no nome
cp "$ARQUIVO_ORIGINAL" "$ARQUIVO_BACKUP"

# Compactar o backup em formato zip
zip -j "$ARQUIVO_BACKUP_ZIP" "$ARQUIVO_BACKUP"

# Remover o arquivo de backup não compactado
rm "$ARQUIVO_BACKUP"

# Criar uma cópia do arquivo original dentro do diretório vendas com data e hora
cp "$ARQUIVO_ORIGINAL" "$ARQUIVO_VENDAS"

# Criar o relatório com as 11 primeiras linhas do arquivo de vendas
{
    echo "Relatório Gerado em: $(date '+%Y/%m/%d %H:%M')"
    echo "====================================="

    # Dados do arquivo original (informações que não devem ser repetidas)
    DATA_PRIMEIRO_REGISTRO=$(head -n 1 "$ARQUIVO_ORIGINAL" | cut -d ',' -f 4)
    DATA_ULTIMO_REGISTRO=$(tail -n 1 "$ARQUIVO_ORIGINAL" | cut -d ',' -f 4)
    QUANTIDADE_ITENS_VENDIDOS=$(cut -d ',' -f 2 "$ARQUIVO_ORIGINAL" | tail -n +2 | sort -u | wc -l)

    # Imprimindo informações de resumo
    echo "Data do Primeiro Registro: $DATA_PRIMEIRO_REGISTRO"
    echo "Data do Último Registro: $DATA_ULTIMO_REGISTRO"
    echo "Quantidade de Itens Diferentes Vendidos: $QUANTIDADE_ITENS_VENDIDOS"

    echo ""
    head -n 11 "$ARQUIVO_ORIGINAL"
} > "$RELATORIO"

echo "Relatório gerado em: $RELATORIO"

# Remover a cópia do arquivo em vendas com data e hora
rm "$ARQUIVO_VENDAS"
