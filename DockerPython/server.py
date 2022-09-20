import socket
import threading
from multiprocessing.pool import ThreadPool
import multiprocessing
from time import sleep

WORKERS = 4
buffer = 1024

def lidarComWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios, relacionamentos, socketWork, intervalo):
     try:
        #enviar grafo para a maquina
        #enviar intervalo
        #conveter em str primeiro
        intervalo = [str(i) for i in intervalo]
        intervalo = " ".join(intervalo)

        socketWork.sendall(intervalo.encode())
        # receber confirmacao
        socketWork.recv(buffer).decode()

        # envia a quantidade de usuarios que serao enviados
        socketWork.sendall(str(quantidadeDeUsuarios).encode())
        # receber confirmacao
        socketWork.recv(buffer).decode()

        # para cada usuario
        for usuario in usuarios:
            # envia usuarios
            socketWork.sendall(usuario.encode())
            # receber confirmacao
            socketWork.recv(buffer).decode()

        # envia a quantidade de relacionamentos que serao enviados
        socketWork.sendall(str(quantidadeDeRelacionamentos).encode())
        # receber confirmacao
        socketWork.recv(buffer).decode()

        # para cada relacionamento
        for relacionamento in relacionamentos:
            # envia relacionamentos
            socketWork.sendall(relacionamento.encode())
            # receber confirmacao
            socketWork.recv(buffer).decode()
        print("\nEsperando Resposta do Work!")

        resposta = []
        for i in intervalo.split(" "):
            resposta.append(socketWork.recv(buffer).decode())
            # envia confirmacao
            socketWork.sendall("Confirmacao".encode())
        print("Resposta", resposta)
        return resposta
     except Exception:
        while True:
            pass



def direcionarWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios, relacionamentos):
    resultados = []
    ip2 = 'localhost'
    porta2 = 8081

    buffer = 1024

    # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
    servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # cria o servidor com o endereco de ip e porta esepecificados acima
    while True:
        try:
            servidor2.bind((ip2, porta2))
            servidor2.listen(WORKERS)
            break
        except OSError:
            continue



    # determinar o numero maximo de conexoes simultaneas


    print("Escutando", ip2, porta2)

    #conectar nos workes
    socketWorkes = []
    NaoDeuTimeOut = True
    # timeout de 20 segundos para que chegue uma conexao
    servidor2.settimeout(20)
    # enquanto nao der um timeout ou seja, enquanto chegar conexoes repete o laco abaixo
    while NaoDeuTimeOut:
        try:
            worker, addr = servidor2.accept()  # retorna o cliente e as informacoes da conexao
            print("Conexao aceita de:", addr[0], addr[1])
            # adicionar conexões
            socketWorkes.append(worker)
            print(socketWorkes)

        except ConnectionResetError:
            print("Deu o time out, nenhuma nova conexao")
            NaoDeuTimeOut = False
            servidor2.settimeout(None)
            servidor2.close()
            # espera 30 segundos antes de terminar a execucao do servidor
            # para dar tempo de alguns cliente terminarem de executar
            # neste momento o sevidor nao aceita mais conexoes
            break
        except socket.timeout:
            print("Deu o time out, nenhuma nova conexao")
            NaoDeuTimeOut = False
            servidor2.settimeout(None)
            servidor2.close()
            # espera 30 segundos antes de terminar a execucao do servidor
            # para dar tempo de alguns cliente terminarem de executar
            # neste momento o sevidor nao aceita mais conexoes
            break

    #enviar dados para cada worker
    inicioDoIntervalo = 0
    intervaloInicial = list(range(quantidadeDeUsuarios))

    while True:
        intervalos = []
        print(socketWorkes)
        workes = len(socketWorkes)
        print(workes)

        if workes == 0:
            print("Sem work, exiting...")
            return resultados

        tamanhoDoIntervalo = len(intervaloInicial)//workes

        for i in range(workes):
            if i == workes-1: #ultimo work pega o resto
                intervalos.append(intervaloInicial[inicioDoIntervalo:])

            else:
                intervalos.append(intervaloInicial[inicioDoIntervalo:inicioDoIntervalo+tamanhoDoIntervalo])
                inicioDoIntervalo += tamanhoDoIntervalo

        #criar uma thread para cada worker e pegar resultado
        threads = []
        for i, work in enumerate(socketWorkes):
            pool = ThreadPool(processes=1)
            threads.append((pool, pool.apply_async(lidarComWorkes,
                                                               (quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios,
                                                                relacionamentos, work, intervalos[i]))))

        #coletar os resutados
        falhas = []
        for i, thread in enumerate(threads):
            try:
                resultado = thread[1].get(30)
            except multiprocessing.context.TimeoutError:
                falhas.append(i)
                thread[0].close()
                continue

            for r in resultado:
                resultados.append(r)

        #tratar falhas e redistruibuir
        if falhas != []:
            #eliminar intervalo ja calculados
            for i, j in enumerate(intervalos):
                if i not in falhas:
                    del intervalos[i]

            #redifinir intervalo
            inicioDoIntervalo = 0

            #concatenar intervalos perdidos
            intervaloInicialAux = []

            for inter in intervalos:
                intervaloInicialAux += inter

            intervaloInicial = intervaloInicialAux

            #reconectar nos workes
            # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
            servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            while True:
                try:
                    servidor2.bind((ip2, porta2))
                    servidor2.listen(WORKERS)
                    break
                except OSError:
                    continue

            print("Escutando", ip2, porta2)

            # conectar nos workes
            socketWorkes = []
            NaoDeuTimeOut = True
            # timeout de 20 segundos para que chegue uma conexao
            servidor2.settimeout(20)
            # enquanto nao der um timeout ou seja, enquanto chegar conexoes repete o laco abaixo
            while NaoDeuTimeOut:
                try:
                    worker, addr = servidor2.accept()  # retorna o cliente e as informacoes da conexao
                    print("Conexao aceita de:", addr[0], addr[1])
                    # adicionar conexões
                    socketWorkes.append(worker)
                    print(socketWorkes)

                except ConnectionResetError:
                    print("Deu o time out, nenhuma nova conexao")
                    NaoDeuTimeOut = False
                    servidor2.settimeout(None)
                    servidor2.close()
                    # espera 30 segundos antes de terminar a execucao do servidor
                    # para dar tempo de alguns cliente terminarem de executar
                    # neste momento o sevidor nao aceita mais conexoes
                    break
                except socket.timeout:
                    print("Deu o time out, nenhuma nova conexao")
                    NaoDeuTimeOut = False
                    servidor2.settimeout(None)
                    servidor2.close()
                    # espera 30 segundos antes de terminar a execucao do servidor
                    # para dar tempo de alguns cliente terminarem de executar
                    # neste momento o sevidor nao aceita mais conexoes
                    break
            continue

        break # sai do loop se nao tiver falhas
    #fechar conexoes
    for sock in socketWorkes:
        sock.close()

    return resultados



def lidarComCliente(socketCliente):
    """funcao que relizarah a execucao de contagem de caracteres de cada solicitacao de cada cliente"""

    #ler a quantidade de usuarios que serao enviados
    quantidadeDeUsuarios = int(socketCliente.recv(buffer).decode())

    #enviar confirmacao
    socketCliente.sendall("Confirmacao".encode())

    #receber cada um dos usuarios
    usuarios = []
    for i in range(quantidadeDeUsuarios):
        usuarios.append(socketCliente.recv(buffer).decode())
        # enviar confirmacao
        socketCliente.sendall("Confirmacao".encode())

    # ler a quantidade de relacionamentos que serao enviados
    quantidadeDeRelacionamentos = int(socketCliente.recv(buffer).decode())
    # enviar confirmacao
    socketCliente.sendall("Confirmacao".encode())

    # receber cada um dos relacionamentos
    relacionamentos = []
    for i in range(quantidadeDeRelacionamentos):
        relacionamentos.append(socketCliente.recv(buffer).decode())
        # enviar confirmacao
        socketCliente.sendall("Confirmacao".encode())

    print(relacionamentos)

    #enviar para as maquinas
    resultados = direcionarWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios,relacionamentos)


    #enviar resultados para cliente

    for resultado in resultados:
        socketCliente.sendall(resultado.encode())
        #receber confirmacao
        socketCliente.recv(buffer).decode()


    #solicitar o termino da execucao
    socketCliente.sendall("Termine a execucao !".encode())

    print("Cliente atendido !")
    #fechar conexao
    socketCliente.close()

if __name__ == '__main__':
    ip = 'localhost'
    porta = 8080

    #AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    #cria o servidor com o endereco de ip e porta esepecificados acima
    servidor.bind((ip, porta))

    MAXIMOCONEXOES = 5

    #determinar o numero maximo de conexoes simultaneas
    servidor.listen(MAXIMOCONEXOES)

    print("Escutando", ip, porta)

    while True:
        cliente, addr = servidor.accept() #retorna o cliente e as informacoes da conexao

        print("Conexao aceita de:", addr[0], addr[1])

        #recebe mensagem de entrada do cliente
        solicitacao = cliente.recv(buffer).decode()
        print("Recebido: ", solicitacao)
        print("\n--------------------------\n")

        #envia mensagem ACK para cliente
        cliente.sendall(f'\nMensagem destinada ao cliente {addr[0]} {addr[1]} \nACK ! Recebido pelo servidor !\n'.encode())

        #cria uma thread para cada cliente aceito
        socketCliente = threading.Thread(target=lidarComCliente, args=(cliente,))

        socketCliente.start()















