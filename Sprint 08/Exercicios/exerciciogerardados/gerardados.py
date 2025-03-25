#Etapa 1: lista de 250 números aleatórios após aplicar o metodo reverse

# Importando a biblioteca para gerar números aleatórios
import random  

# Cria uma lista com 250 números aleatórios entre 1 e 1000
numeros = [random.randint(1, 1000) for _ in range(250)]

# Inverte a lista
numeros.reverse()

# Imprime o resultado
print(numeros)



#Etapa 2: criação de uma lista de 20 animais e salvando ela em um arquivo .csv 

# Criar lista com 20 nomes de animais
animais = ["Cachorro", "Gato", "Elefante", "Tigre", "Leão", "Cobra", "Águia", "Lobo", "Urso", "Zebra",
           "Cavalo", "Papagaio", "Jacaré", "Arraia", "Ornitorrinco", "Golfinho", "Panda", "Coruja", "Galo", "Tatu"]

# Ordenar em ordem crescente
animais.sort()

# Imprimir os animais um a um
[print(animal) for animal in animais]

# Salvar em um arquivo CSV
with open("animais.csv", "w", encoding="utf-8") as file:
    for animal in animais:
        file.write(animal + "\n")



#Etapa 3: Gerando um dataset de nomes aleatórios

import names
import time

# Definindo a seed de aleatoriedade
random.seed(40)

# Definindo quantidade
qtd_nomes_unicos = 3000  
qtd_nomes_aleatorios = 1000000  

# Criando a lista de nomes únicos
nomes_unicos = [names.get_full_name() for _ in range(qtd_nomes_unicos)]

print("Gerando {} nomes aleatórios...".format(qtd_nomes_aleatorios))

# Criando uma lista de nomes aleatórios se baseando na lista de únicos
nomes_aleatorios = [random.choice(nomes_unicos) for _ in range(qtd_nomes_aleatorios)]

# Salvadno os nomes num arquivo .txt
with open("nomes_aleatorios.txt", "w", encoding="utf-8") as file:
    for nome in nomes_aleatorios:
        file.write(nome + "\n")

print("Arquivo nomes_aleatorios.txt gerado com sucesso!")
