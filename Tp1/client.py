import socket

def lerArquivo(nomeDoArquivo):
    """Funcao que leh os dados e relacionamentos dos usuarios
     da rede social e retorna cada linha lida"""

    #ler dados relevantes do arquivo
    dadosDoArquivo = []
    with open(nomeDoArquivo) as arquivo:
        #para cada linha do arquivo
        for linha in arquivo:
            #remover linhas vazias
            if linha != "\n":
                # adicionar dados validos
                #remove o caractere '\n' que toda linha possui no final
                dadosDoArquivo.append(linha.replace("\n", ""))
    return dadosDoArquivo

#ler dados do arquivo

#ler usuarios
usuarios = lerArquivo("users.txt")

#ler relacionamentos
relacionamentos = lerArquivo("relationship.txt")

#imprimir leitura do arquivo
print(usuarios)
print(relacionamentos)

#definir o host ("IP") e a porta
host = 'localhost'
porta = 8080

#tamanho do buffer para receber dados
buffer = 1024

#criar um socket TCP/ IPv4
cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#conectar a um servidor
try:
    cliente.connect((host, porta))
except ConnectionRefusedError:
    print("\nO servidor nao estah aceitando conexoes !\nPor favor tente novamente em alguns segundos.")
    exit()

cliente.sendall("Eu sou um cliente, estou me conectando ao servidor".encode())

#receber ACK de conexao
resposta = cliente.recv(buffer).decode()
print(resposta)

#envia a quantidade de usuarios que serao enviados
cliente.sendall(usuarios[0].encode())

#receber confirmacao
cliente.recv(buffer).decode()

#para cada usuario
for usuario in usuarios[1:]:
    #envia usuarios
    cliente.sendall(usuario.encode())

    # receber confirmacao
    cliente.recv(buffer).decode()

#envia a quantidade de relacionamentos que serao enviados
cliente.sendall(relacionamentos[0].encode())
#receber confirmacao
cliente.recv(buffer).decode()

#para cada relacionamento
for relacionamento in relacionamentos[1:]:
    #envia relacionamentos
    cliente.sendall(relacionamento.encode())
    # receber confirmacao
    cliente.recv(buffer).decode()

print("\nEsperando Resposta do Servidor !")

#receber os dados do servidor
respostas = []
for i in range(int(usuarios[0])):
    resposta = cliente.recv(buffer).decode()
    respostas.append(resposta)
    # enviar confirmacao
    cliente.sendall("Confirmacao".encode())


#mensagem de terminar a execucao
resposta = cliente.recv(buffer).decode()
print(resposta)

#imprimir resposta do servidor
print(respostas)

#escrever os resultados no arquivo "saida.txt"
with open("saida.txt", 'w') as arquivo:
    for resposta in respostas:
        arquivo.write(resposta + "\n")
