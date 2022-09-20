import socket
import graphframes
from time import sleep
import threading

def sugestao(grafo, vertice, usuarios):
    """sugere amigos para um usuarios"""
    adjacencias = grafo[vertice]

    sugestivo = []

    for adjacencia in adjacencias:
        for adjacencia2 in grafo[adjacencia]:
            sugestivo += adjacencia2
    sugestivo = [sug for sug in sugestivo if sug not in adjacencias]
    sugestivo = list(set(sugestivo))

    retorno = usuarios[int(vertice)].split(" ")[1] + ": "
    for sug in sugestivo:
        #para nao inserir como sugestao voce proprio
        if sug not in retorno and usuarios[int(sug)].split(" ")[1] != usuarios[int(vertice)].split(" ")[1]:
            retorno += usuarios[int(sug)].split(" ")[1] + ","

    return retorno



def calculaSugestaoDeAmigos(usuarios, relacionamentos, intervalo):
    grafo = graphframes.GraphFrame(usuarios, relacionamentos)
    resultado = []
    #calcula amigos proximo para cada usuario no intervalo
    for inter in intervalo:
        #para cada usuario deve_se gerar a sugestao de amigos
        resultado.append(sugestao(grafo, inter, usuarios))
        """criamos a funcao mas poderia ter feito
            graph.find (“(a)-[e]->(b); (b)-[e2]->(c); !(a)-[]->(c)”)
            queremos um usuário “a” que tenha conexão com “b” (a)-[e]->(b), 
            sendo que “b” tem conexão com “c” (b)-[e2]->(c), 
            mas “a” e “c” não tenha conexão !(a)-[]->(c)."""

    return resultado

def lidarComServer(work):
    print("Conectado ao servidor")

    # receber intervalo do servidor
    print("Recebendo Intervalo")
    intervalo = work.recv(buffer).decode().split(" ")
    # envia confirmacao
    work.sendall("Confirmacao".encode())

    print(intervalo)

    # receber quantidade de usuarios
    print("Recebendo quantidade de usuarios")
    quantidadeDeUsuarios = int(work.recv(buffer).decode())
    # envia confirmacao
    work.sendall("Confirmacao".encode())

    # receber cada um dos usuarios
    print("Recebendo Usuarios")
    usuarios = []
    for i in range(quantidadeDeUsuarios):
        usuarios.append(work.recv(buffer).decode())
        # envia confirmacao
        work.sendall("Confirmacao".encode())

    # ler a quantidade de relacionamentos que serao enviados
    print("Recebendo quantidade de relacionamentos")
    quantidadeDeRelacionamentos = int(work.recv(buffer).decode())
    # envia confirmacao
    work.sendall("Confirmacao".encode())


    # receber cada um dos relacionamentos
    print("Recebendo relacionamentos")
    relacionamentos = []
    for i in range(quantidadeDeRelacionamentos):
        relacionamentos.append(work.recv(buffer).decode())
        # envia confirmacao
        work.sendall("Confirmacao".encode())

    respostas = calculaSugestaoDeAmigos(usuarios, relacionamentos, intervalo)
    # retorna uma lista onde ["Usuarioreferido: Sugestao de Amigos",...]

    print(respostas)
    # enviar resultados para o servidor
    for resposta in respostas:
        work.sendall(resposta.encode())

        # receber confirmacao
        work.recv(buffer).decode()

    work.close()


#definir o host ("IP") e a porta
host = 'localhost'
porta = 8081

#tamanho do buffer para receber dados
buffer = 1024


if __name__ == "__main__":
    #conectar a um servidor
    while True:
        try:
            # criar um socket
            work = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            work.connect((host, porta)) #conectar ao servidor

            #criar uma thread de atendimento e espera 15 segundos para a mesma maquina atender denovo
            #caso espere menos de 10 segundos varias threads de uma mesma maquina podem entrar em um
            # so problema, mas cada thread vai ser considerada uma maquina diferente
            socketWork = threading.Thread(target=lidarComServer, args=(work,))
            socketWork.start()
            sleep(15)

        except ConnectionRefusedError:
            print("\nO servidor nao estah aceitando conexoes !\nPor favor tente novamente em alguns segundos.")
            sleep(5) # espera 5 segundos para tentar conectar ao servidor novamente

        except ConnectionResetError:
            print("\nO servidor destruiu a conexao")

