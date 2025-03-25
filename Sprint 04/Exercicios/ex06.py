#Você foi encarregado de desenvolver uma nova feature  para um sistema de gestão de supermercados. O analista responsável descreveu o requisito funcional da seguinte forma:


#- Para realizar um cálculo de custo, o sistema deverá permitir filtrar um determinado conjunto de produtos, de modo que apenas aqueles cujo valor unitário for superior à média deverão estar presentes no resultado. Vejamos o exemplo:


# Conjunto de produtos (entrada):


    #Arroz: 4.99

    #Feijão: 3.49

    #Macarrão: 2.99

    #Leite: 3.29

    #Pão: 1.99


def maiores_que_media(conteudo):
    media = sum(conteudo.values()) / len(conteudo)
    produtos_acima_da_media = [(nome, preco) for nome, preco in conteudo.items() if preco > media]
    return sorted(produtos_acima_da_media, key=lambda x: x[1])
