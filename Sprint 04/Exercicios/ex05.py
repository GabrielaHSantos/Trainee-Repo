#Um determinado sistema escolar exporta a grade de notas dos estudantes em formato CSV. Cada linha do arquivo corresponde ao nome do estudante, acompanhado de 5 notas de avaliação, no intervalo [0-10]. É o arquivo estudantes.csv de seu exercício.

#Precisamos processar seu conteúdo, de modo a gerar como saída um relatório em formato textual contendo as seguintes informações:

    #Nome do estudante

    #Três maiores notas, em ordem decrescente

    #Média das três maiores notas, com duas casas decimais de precisão

#O resultado do processamento deve ser escrito na saída padrão (print), ordenado pelo nome do estudante e obedecendo ao formato descrito a seguir:

#Nome: <nome estudante> Notas: [n1, n2, n3] Média: <média>

#Exemplo:

#Nome: Maria Luiza Correia Notas: [7, 5, 5] Média: 5.67

#Nome: Maria Mendes Notas: [7, 3, 3] Média: 4.33


#Em seu desenvolvimento você deverá utilizar lambdas e as seguintes funções: round, map, sorted

import csv

def processar_notas(arquivo_csv):
    estudantes = []

    with open(arquivo_csv, 'r', encoding='utf-8') as arquivo:
        leitor = csv.reader(arquivo)
        for linha in leitor:
            nome = linha[0]
            notas = list(map(float, linha[1:]))
            maiores_notas = sorted(notas, reverse=True)[:3]
            maiores_notas = [int(n) if n.is_integer() else n for n in maiores_notas]
            media = round(sum(maiores_notas) / 3, 2)
            estudantes.append((nome, maiores_notas, media))
    
    estudantes = sorted(estudantes, key=lambda x: x[0])
    
    for nome, maiores_notas, media in estudantes:
        print(f"Nome: {nome} Notas: {maiores_notas} Média: {media}")

processar_notas('estudantes.csv')
