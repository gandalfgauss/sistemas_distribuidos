import socket
import threading
from multiprocessing.pool import ThreadPool
import multiprocessing
from time import sleep

WORKERS = 4
buffer = 1024

def lidarComWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios, relacionamentos, socketWork, intervalo):
     try:
        #enviar grafo para um worker

        #enviar intervalo primeiro
        #conveter em str primeiro
        intervalo = [str(i) for i in intervalo]
        intervalo = " ".join(intervalo)

        #codificar e enviar
        socketWork.sendall(intervalo.encode())
        # receber confirmacao
        if socketWork.recv(buffer).decode() != "Confirmacao":
            return None

        # envia a quantidade de usuarios que serao enviados
        socketWork.sendall(str(quantidadeDeUsuarios).encode())
        # receber confirmacao
        if socketWork.recv(buffer).decode() != "Confirmacao":
            return None

        # para cada usuario
        for usuario in usuarios:
            # envia usuarios
            socketWork.sendall(usuario.encode())
            # receber confirmacao
            if socketWork.recv(buffer).decode() != "Confirmacao":
                return None

        # envia a quantidade de relacionamentos que serao enviados
        socketWork.sendall(str(quantidadeDeRelacionamentos).encode())
        # receber confirmacao
        if socketWork.recv(buffer).decode() != "Confirmacao":
            return None

        # para cada relacionamento
        for relacionamento in relacionamentos:
            # envia relacionamentos
            socketWork.sendall(relacionamento.encode())
            # receber confirmacao
            if socketWork.recv(buffer).decode() != "Confirmacao":
                return None

        print("\nEsperando Resposta do Work!")

        resposta = []
        for i in intervalo.split(" "):
            resposta.append(socketWork.recv(buffer).decode())
            # envia confirmacao
            socketWork.sendall("Confirmacao".encode())

        #retorna respostas
        return resposta

     except Exception:
        #se capturar qualquer excecao retorna o valor None
        return None



def direcionarWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios, relacionamentos):
    #lista que armazerarah os resultados dos workes
    resultados = []

    #definir ip e porta de distribuicao de tarefas aos workes
    ip2 = 'localhost'
    porta2 = 8081

    # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
    servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT

    # cria o servidor com o endereco de ip e porta esepecificados acima
    #se o servidor2 estiver usando a porta com outro cliente espera ele fechar
    while True:
        try:
            servidor2.bind((ip2, porta2))
            servidor2.listen(WORKERS)

            break
        except OSError:
            continue

    print("Escutando", ip2, porta2)

    #conectar nos workes

    socketWorkes = [] #essa lista armazerah a conexao com os workes

    NaoDeuTimeOut = True
    # timeout de 10 segundos para que chegue uma conexao com os workes
    servidor2.settimeout(10)
    # enquanto nao der um timeout aceita conexoes de workes
    while NaoDeuTimeOut:
        try:
            worker, addr = servidor2.accept()  # fica esperando a conexao de algum worker
            print("Conexao aceita de work:", addr[0], addr[1])
            # adicionar conexões de workes na lista de conexoes
            socketWorkes.append(worker)

        except ConnectionResetError:
            print("Deu o time out, nenhuma nova conexao")
            NaoDeuTimeOut = False
            servidor2.settimeout(None)
            servidor2.close()

            break
        except socket.timeout:
            #excecao caso nao chegue nenhuma nova conexao
            print("Deu o time out, nenhuma nova conexao")
            #se nao tem workers tenta novamente
            if len(socketWorkes) == 0:
                servidor2.settimeout(10)
                continue
            NaoDeuTimeOut = False
            servidor2.settimeout(None)
            servidor2.close()
            break

    #agora deve_se enviar dados para cada worker
    inicioDoIntervalo = 0

    #cria uma lista de 0 a tamanho de usuarios que serah o intervalo inicial
    intervaloInicial = list(range(quantidadeDeUsuarios))

    while True:
        intervalos = []  # essa armazenara os intervalos a serem trabalhados por cada worker
        workes = len(socketWorkes) #obter quantidade de workes disponiveis

        #se todos os workes pararem de funcionar retorna o resultado que tem ate entao
        if workes == 0:
            print("Sem work, exiting...")
            return resultados

        #definir o tamanho do intervalo que sera dividido para cada worker
        tamanhoDoIntervalo = len(intervaloInicial)//workes

        print("Numero de workes:", workes)

        #distribuir intervalos
        for i in range(workes):
            if i == workes-1: #ultimo work pega o resto do intervalo
                intervalos.append(intervaloInicial[inicioDoIntervalo:])

            else:
                intervalos.append(intervaloInicial[inicioDoIntervalo:inicioDoIntervalo+tamanhoDoIntervalo])
                inicioDoIntervalo += tamanhoDoIntervalo

        #criar uma thread para cada worker e pegar resultado de suas tarefas
        threads = []
        for i, work in enumerate(socketWorkes):
            pool = ThreadPool(processes=1)
            threads.append((pool, pool.apply_async(lidarComWorkes,
                                                               (quantidadeDeUsuarios, quantidadeDeRelacionamentos,
                                                                usuarios, relacionamentos, work, intervalos[i]))))

        #coletar os resutados
        print("Obtendo os resultados !")
        falhas = [] # essa lista armazenarah a posicao das threads dos workes que deram problema

        #coletar resultados das threads
        for i, thread in enumerate(threads):
            resultado = thread[1].get()

            #se o resultado for None entao deu erro na thread
            if resultado is None:
                #entao a posicao da thread eh adicionada na lista de falhas
                falhas.append(i)
                # e o loop se repete
                continue
            #caso contrario os resultados sao adicionados na lista de resultados
            for r in resultado:
                resultados.append(r)

        # se a lista de falhas nao estiver vazia
        # entao algo deve ser tratado
        if falhas != []:
            print("Teve falha !")

            #deve se entao eliminar intervalos ja calculados
            for i, j in enumerate(intervalos):
                if i not in falhas:
                    del intervalos[i]

            #e redifinir intervalo e concatenar intervalos perdidos

            inicioDoIntervalo = 0
            intervaloInicialAux = []

            for inter in intervalos:
                intervaloInicialAux += inter

            intervaloInicial = intervaloInicialAux

            #Deve se também reconectar nos workes
            # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
            servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            servidor2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT

            while True:
                try:
                    servidor2.bind((ip2, porta2))
                    servidor2.listen(WORKERS)

                    break
                except OSError:
                    continue

            print("Escutando", ip2, porta2)

            # reconectar nos workes
            socketWorkes = [] #essa lista armazerah a conexao com os workes, ela foi zerada nesse instante

            NaoDeuTimeOut = True
            # timeout de 10 segundos para que chegue uma conexao
            servidor2.settimeout(10)
            # enquanto nao der um timeout aceita conexoes de workes
            while NaoDeuTimeOut:
                try:
                    worker, addr = servidor2.accept()  # fica esperando a conexao de algum worker
                    print("Conexao aceita de:", addr[0], addr[1])
                    # adicionar conexões de workes na lista de conexoes
                    socketWorkes.append(worker)

                except ConnectionResetError:
                    print("Deu o time out, nenhuma nova conexao")
                    NaoDeuTimeOut = False
                    servidor2.settimeout(None)
                    servidor2.close()

                    break
                except socket.timeout:
                    print("Deu o time out, nenhuma nova conexao")
                    NaoDeuTimeOut = False
                    servidor2.settimeout(None)
                    servidor2.close()

                    break
                #repetir loop para resolver falhas
            continue
        #caso nao haja falhas termina o loop
        break # sai do loop se nao tiver falhas
    #fechar conexoes com os workes
    for sock in socketWorkes:
        sock.close()
    #retorna a lista de resultados
    return resultados



def lidarComCliente(socketCliente):
    """funcao que lidarah com o cliente"""

    #ler a quantidade de usuarios que serao enviados
    quantidadeDeUsuarios = int(socketCliente.recv(buffer).decode())

    #enviar confirmacao
    socketCliente.sendall("Confirmacao".encode())

    #receber cada um dos usuarios
    usuarios = []
    for i in range(quantidadeDeUsuarios):
        #recebe um usuario
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
        #recebe relacionamento
        relacionamentos.append(socketCliente.recv(buffer).decode())
        # enviar confirmacao
        socketCliente.sendall("Confirmacao".encode())

    #apos o recebimento dos dados do cliente deve_se trabalhar com os mesmos e os distruibuir
    resultados = direcionarWorkes(quantidadeDeUsuarios, quantidadeDeRelacionamentos, usuarios,relacionamentos)

    print("Terminou")
    print("Imprimindo resultados:", resultados)

    print("Enviando dados para cliente")
    #enviar resultados para cliente
    for resultado in resultados:
        #envia um resultado
        socketCliente.sendall(resultado.encode())
        #receber confirmacao de que o resultado chegou
        socketCliente.recv(buffer).decode()

    print("Os dados foram enviados")

    #solicitar o termino da execucao do cliente
    socketCliente.sendall("Termine a execucao !".encode())

    print("Cliente atendido !")
    #fechar conexao com o cliente
    socketCliente.close()

if __name__ == '__main__':
    #inicialmente é definido o ip e a porta de atendimento ao cliente
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
        #fica no aguardo de uma conexao com o cliente
        cliente, addr = servidor.accept()

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















