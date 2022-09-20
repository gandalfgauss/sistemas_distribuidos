import socket
from random import randint
from time import sleep

# definir o host ("IP") e a porta
ip = 'localhost'
porta = 8084

# tamanho do buffer para receber dados
buffer = 1024

# aceitar conexoes de um broker


# loop de mensagens
while True:
    # gerar Mensagem e Topico(s) a ser publicados
    topico = randint(0, 2)
    mensagem = "Publicador1"

    # loop de conexao
    while True:
        # criar um socket TCP/ IPv4
        publicador = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        publicador.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # por causa do TIME_WAIT
        # cria um publicador com o endereco de ip e porta especificados acima
        publicador.bind((ip, porta))
        # determinar o numero maximo de conexoes simultaneas
        publicador.listen(5)


        print("Procurando Conexoes !")
        broker, addr = publicador.accept()
        publicador.close()
        print(f'Conectado ao Broker {addr[0]} {addr[1]}')
        print(f'Favor Publicar no topico {topico} a mensagem {mensagem}')
        # enviar o topico para broker
        broker.sendall(str(topico).encode())


        # receber confirmacao se o broker tem ou nao o topico
        confirmacao = int(broker.recv(buffer).decode())


        # se a confirmacao for 0
        if confirmacao == 0:
            print("Esse Broker nao tem o Topico", topico)
            # broker nao tem o topico
            # fecha a conexao
            broker.close()
            continue
        else:  # caso contrario
            # o broker tem o topico
            # envia a mensagem para o broker
            broker.sendall(f'{mensagem}'.encode())


            print(broker.recv(buffer).decode())  # recebe uma mensagem se deu certo
            broker.sendall("Confirmacao".encode())
            sleep(10)
            break

