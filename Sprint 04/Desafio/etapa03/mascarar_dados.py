import hashlib

while True:
    entrada = input("Digite uma string para mascarar: ")
    hash_obj = hashlib.sha1(entrada.encode())
    print(f"Hash gerado: {hash_obj.hexdigest()}")
