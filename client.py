from pika import BlockingConnection
import time
import os
from sys import argv


class Error(Exception):
    def __init__(self, m):
        self.message = m


def as_severity(sev):
    if sev == 1:
        return "DEBUG"
    elif sev == 2:
        return "INFO"
    elif sev == 3:
        return "WARNING"
    elif sev == 4:
        return "ERROR"
    elif sev == 5:
        return "CRITICAL"
    raise Error('Numero de severidade nao existente')


conn = BlockingConnection()
ch = conn.channel()
assert (len(argv) > 1)
exchange = argv[1]
ch.exchange_declare(exchange=exchange, exchange_type='fanout')
running = True
while running:
    try:
        msg = input("Mensagem (verbose:on/off)(end)")
        if msg.startswith('verbose:'):
            toggle = msg.split(':')
            if len(toggle) != 2 and toggle[1] != 'on' and toggle[1] != 'off':
                raise Error('Parametro incorreto')
            ch.basic_publish(exchange=exchange, routing_key='', body=msg)
        elif msg == 'end': \
                running = False
        else:
            s = input("Digite a severidade")
            if not s.isdigit():
                raise Error("Digite um numero valido")
            s = as_severity(int(s))
            b = f"{time.time()}:{os.getpid()}:{s}:{msg}"
            ch.basic_publish(exchange=exchange, routing_key="", body=b)
    except Error as e:
        print(e.message)

ch.close()
conn.close()
