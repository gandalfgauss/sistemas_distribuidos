import socket
import threading


def receberNotify(conexaoBroker):
    brokerDoTopico = conexaoBroker[1]
    topicoParaSerNotificado = conexaoBroker[0]

    # receber atualizacoes
    while True:
        # receber a atualizacao/notify
        mensagem = brokerDoTopico.recv(buffer).decode()
        brokerDoTopico.sendall("Confirmacao".encode())  # enviar confirmacao de recebimento de notify
        print("Nova Atualizacao do Topico", topicoParaSerNotificado)

        # atualizar topico
        for topico in topicos:
            if topico[0] == int(topicoParaSerNotificado):
                topico.append(mensagem)
                break
        print("Topicos atualizados", topicos)


# definir o host ("IP") e a porta
ip = 'localhost'
porta = 8083

# tamanho do buffer para receber dados
buffer = 1024

# Topico(s) a serem escritos
topicosASeremIncritos = [1, 2]

topicos = [[topico, f'Topico {topico}'] for topico in topicosASeremIncritos]

conexoes = []


# para cada topico a ser inscrito deve se subscrever fazendo a conexao com broker
for topico in topicosASeremIncritos:
    # loop de conexoes
    while True:
        # criar um socket TCP/ IPv4
        subEscritor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        subEscritor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT
        # cria um publicador com o endereco de ip e porta especificados acima
        subEscritor.bind((ip, porta))

        # determinar o numero maximo de conexoes simultaneas
        subEscritor.listen(5)

        # aceitar conexoes de um broker
        print("Procurando Conexoes !")
        broker, addr = subEscritor.accept()
        subEscritor.close()
        print(f'Conectado ao Broker {addr[0]} {addr[1]}')

        # enviar topico para broker avaliar se ele tem
        broker.sendall(str(topico).encode())

        # receber confirmacao se o broker tem ou nao o topico
        confirmacao = int(broker.recv(buffer).decode())
        broker.sendall("Confirmacao".encode())

        # se a confirmacao for 0
        if confirmacao == 0:
            # broker nao tem o topico
            # fecha a conexao
            broker.close()
            continue
        else:  # caso contrario
            # o broker tem o topico
            print(f'O Broker {addr[0]} {addr[1]} increveu o SubEscritor 2 no topico {topico}')
            # adiciona o topico e a conexao com o broker numa lista
            conexoes.append((topico, broker))
            break


# criar thread para cada conexao com o topico para receber as atualizacoes
threads = []
for conexao in conexoes:
    threads.append(threading.Thread(target=receberNotify, args=(conexao,)))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()





