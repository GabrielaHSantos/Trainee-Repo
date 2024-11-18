#!/bin/bash

# Diretório onde os relatórios estão armazenados
BACKUP_DIR="vendas/backup"
RELATORIO_FINAL="$BACKUP_DIR/relatorio_final.txt"  # Relatório final dentro do backup

# Criar ou limpar o arquivo relatorio_final.txt
> "$RELATORIO_FINAL"

# Adicionar a data e a hora ao início do relatório final
echo "Relatório Consolidado - Gerado em $(date '+%Y/%m/%d %H:%M')" > "$RELATORIO_FINAL"
echo "======================================" >> "$RELATORIO_FINAL"
echo "" >> "$RELATORIO_FINAL"  # Adiciona uma linha em branco para separação

# Adicionar conteúdo de todos os relatórios .txt ao relatório final
for relatorio in "$BACKUP_DIR"/relatorio-*.txt; do
    if [[ -f "$relatorio" ]]; then
        echo "Conteúdo do relatório extraído de: $relatorio" >> "$RELATORIO_FINAL"
        cat "$relatorio" >> "$RELATORIO_FINAL"
        echo "" >> "$RELATORIO_FINAL"  # Adiciona uma linha em branco para separação
    fi
done

echo "Relatório consolidado gerado em: $RELATORIO_FINAL"
