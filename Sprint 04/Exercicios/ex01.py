#Você está recebendo um arquivo contendo 10.000 números inteiros, um em cada linha. Utilizando lambdas e high order functions, apresente os 5 maiores valores pares e a soma destes. 

# Você deverá aplicar as seguintes funções no exercício: map, filter, sorted, sum.

#Seu código deverá exibir na saída (simplesmente utilizando 2 comandos `print()`): a lista dos 5 maiores números pares em ordem decrescente e a soma destes valores.

with open('number.txt', 'r') as file:
    numeros = list(map(int, file.readlines()))
    
numeros_pares = filter(lambda x: x is not None, map(lambda x: x if x % 2 == 0 else None, numeros))
cinco_maiores_pares = sorted(numeros_pares, reverse=True)[:5]
soma_cinco_maiores = sum(cinco_maiores_pares)

print(cinco_maiores_pares)
print(soma_cinco_maiores)