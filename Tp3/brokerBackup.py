import socket
import threading

from time import sleep

buffer = 1024

ip = "localhost"
porta = 8086
enderecosBrokers = [("localhost", 8080), ("localhost", 8081)]  # enderecos dos brokers
enderecosPublicadores = [("localhost", 8084), ("localhost", 8085)]  # enderecos dos publicadores
enderecosSubEscritores = [("localhost", 8082), ("localhost", 8083)]  # enderecos dos subescritores

topicos = [[0, "Topico 0"], [1, "Topico 1"], [2, "Topico 2"]]  # topicos
atualizacao = {0: [], 1: [], 2: []}
acquires = {0: threading.Lock(), 1: threading.Lock(), 2: threading.Lock()}
subEscritoresOnline = []  # essa lista armazerah a conexao com os subEscritores e os topicos que devem ser avisados


def publicarNoTopico(topico, mensagem):
    for top in topicos:
        if int(top[0]) == int(topico):
            top.append(mensagem)


def acquireRelease(topicoX, mensagemX):
    # criar thread para trocar informacoes com brokers
    # criar thread para subescritores
    # tentar conectar a todos os brokers
    for enderecoBroker in enderecosBrokers:
        try:
            # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
            servidor2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            servidor2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT
            servidor2.connect(enderecoBroker)

            # enviar topico os brokens
            servidor2.sendall(str(topicoX).encode())
            servidor2.recv(buffer)  # receber confirmacao

            # enviar mensagens para os brokens
            servidor2.sendall(str(mensagemX).encode())
            servidor2.recv(buffer)  # receber confirmacao

            servidor2.close()
        except ConnectionRefusedError:
            continue
        except Exception:
            continue

def notify(topico, mensagem):
    for top, sub in subEscritoresOnline:
        if int(top) == int(topico):
            sub.sendall(mensagem.encode())

    publicarNoTopico(topico, mensagem)


def lidarComPublicador(publicador):
    # receber topico do publicador
    topicoASerEscrito = int(publicador.recv(buffer).decode())

    # olhar se o broker(eu mesmo) tem o topico
    confirmacao = conferirSeTemOTopico(topicoASerEscrito)

    # enviar a confirmacao para subEscritor avisando se tem ou nao o topico
    publicador.sendall(confirmacao.encode())

    if confirmacao == "0":
        print(f'Nao tenho o Topico {topicoASerEscrito} para Publicar')
        publicador.close()
        return

    print(f'Publicando no topico {topicoASerEscrito}')
    acquires[topicoASerEscrito].acquire()
    print("Depois do Acquire")
    # recebe mensagem do publicador a ser publicado no Topico A ser Escrito
    mensagem = publicador.recv(buffer).decode()
    print("Mensagem Recebida do publicador: ", mensagem)

    # agora eh necessario avisar os brokers que vai ter modificaocao
    # acquire e release
    acquireRelease(topicoASerEscrito, mensagem)

    # publicar mensagem
    print(f'Publicar no Topico {topicoASerEscrito}')

    # ordenar atualizacoes e chamar notify
    mensagens = atualizacao[topicoASerEscrito]
    mensagens.append(mensagem)
    mensagens.sort()

    # notificar subescritores
    for mensagem in mensagens:
        notify(topicoASerEscrito, mensagem)

    atualizacao[topicoASerEscrito].clear()
    acquires[topicoASerEscrito].release()  # devolver recurso

    # enviar mensagem para publicador falando que foi publicado com sucesso
    publicador.sendall(f'\nMensagem destinada ao Publicador ! Publicado com sucesso'.encode())
    publicador.recv(buffer)  # confirmacao
    publicador.close()
    print(f'Publicado no topico {topicoASerEscrito} a mensagem {mensagem}. Fim !')
    print("Topicos apos publicacao", topicos)


def lidarComPublicadores():
    while True:
        # broken tentar conectar aos publicadores
        for enderecoPublicador in enderecosPublicadores:
            try:
                # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
                broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                broker.connect(enderecoPublicador)
                print("Conectado a um Publicador")

                # criar uma thread de atendimento ao publicador
                threadPub = threading.Thread(target=lidarComPublicador, args=(broker,))
                threadPub.start()
            except ConnectionRefusedError:
                continue
            except OSError:
                continue
            except Exception:
                continue


# essa funcao confere se o topicoX existe no broker
def conferirSeTemOTopico(topicoX):
    for topico in topicos:
        if int(topico[0]) == int(topicoX):
            return "1"

    return "0"


def lidarComSubEscritor(subEscritor):
    try:
        # receber topico de subEscritor
        topicoRecebido = int(subEscritor.recv(buffer).decode())

        # olhar se o broker(eu mesmo) tem o topico
        confirmacao = conferirSeTemOTopico(topicoRecebido)

        # enviar a confirmacao para subEscritor avisando se tem ou nao o topico
        subEscritor.sendall(confirmacao.encode())
        subEscritor.recv(buffer)  # receber confirmacao

        if confirmacao == "0":
            subEscritor.close()
            return
        else:
            subEscritoresOnline.append((topicoRecebido, subEscritor))
            print(f'SubEscritor inscrito no topico {topicoRecebido}')
    except ConnectionResetError:
        return

    # essa funcao eh responsavel por subsescrever um subescritor a um topico


def lidarComSubEscritores():
    while True:
        # broken tentar conectar aos subEscritores
        for enderecoSubEscritor in enderecosSubEscritores:
            try:
                # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
                broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                broker.connect(enderecoSubEscritor)
                print("Conectado a um SubEscritor")

                # criar uma thread de atendimento ao publicador
                threadPub = threading.Thread(target=lidarComSubEscritor, args=(broker,))
                threadPub.start()

            except ConnectionRefusedError:
                continue


def lidarComBrokers2(broker):
    # receber topico e mensagem dos brokers
    topico = broker.recv(buffer).decode()
    broker.sendall("Confirmacao".encode())  # enviar confirmacao
    print(f'Topico recebido do Broker {topico}')

    mensagem = broker.recv(buffer).decode()
    broker.sendall("Confirmacao".encode())  # enviar confirmacao
    print(f'Mensagem recebida do Broker {mensagem}')

    print("Conferencia", conferirSeTemOTopico(topico))
    if conferirSeTemOTopico(topico) == "1":
        if acquires[int(topico)].acquire(False):
            notify(topico, mensagem)
            print(f'Publicando Atualizacao no Topico {topico}')
            acquires[int(topico)].release()

        else:
            print(f'Publicando Atualizacao no Topico {topico} no vetor de Atualizacao')
            atualizacao[int(topico)] = mensagem
    print("Topicos apos atualizacao", topicos)
    broker.close()


# recebe atualizacoes de outros brokers
def lidarComBrokers():
    # AF_INET -> servico IPv4, SOCK STREAM -> Protocolo Tcp
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT

    # cria o servidor com o endereco de ip e porta esepecificados acima
    # se o servidor2 estiver usando a porta com outro cliente espera ele fechar
    while True:
        try:
            servidor.bind((ip, porta))
            servidor.listen(1)

            break
        except OSError:
            continue

    print("Escutando", ip, porta)

    # enquanto nao der um timeout aceita conexoes de Brokers
    while True:
        try:
            broker, addr = servidor.accept()  # fica esperando a conexao de algum broker
            print("Conexao aceita de Broken:", addr[0], addr[1])

            threadParaBrokers2 = threading.Thread(target=lidarComBrokers2, args=(broker,))
            threadParaBrokers2.start()


        except ConnectionResetError:
            print("Deu o time out, nenhuma nova conexao")
            NaoDeuTimeOut = False
            servidor.settimeout(None)
            continue

        except socket.timeout:
            # excecao caso nao chegue nenhuma nova conexao
            print("Deu o time out, nenhuma nova conexao")
            NaoDeuTimeOut = False
            servidor.settimeout(None)
            servidor.close()
            break

        except Exception:
            continue


if __name__ == '__main__':
    # criar thread para publicadores
    threadParaPublicadores = threading.Thread(target=lidarComPublicadores)
    threadParaPublicadores.start()

    # criar thread para subescritores
    threadParaSubEscritores = threading.Thread(target=lidarComSubEscritores)
    threadParaSubEscritores.start()

    threadParaBrokers = threading.Thread(target=lidarComBrokers)
    threadParaBrokers.start()

    threadParaPublicadores.join()
    threadParaSubEscritores.join()
    threadParaBrokers.join()
