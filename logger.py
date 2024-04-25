from pika import BlockingConnection
from sys import argv

verbose = True


class InvalidSyntax(Exception):
    def __init__(self):
        self.message = "Sintaxe invalida"


def validate(sev):
    msg = ""
    if sev == "DEBUG" or \
            sev == "WARNING" or \
            sev == "INFO" or \
            sev == "ERROR" or \
            sev == "CRITICAL":
        return msg
    else:
        raise InvalidSyntax()


def callback(channel, method, properties, body):
    msg = body.decode('utf-8')
    s = msg.split(":")
    try:
        if not log(s[0], s[1], s[2], s[3]):
            verbos(s[0], s[1])
    except:
        print("Sintaxe invalida")


def log(t, s, i, m):
    global verbose
    if len(s) == 4:
        if verbose:
            validate(s)
            print(f"[{s}] {t}|pid:{i}: {m}")
        return True
    return False


def verbos(attr, d):
    global verbose
    if attr == 'verbose':
        if d == 'on':
            verbose = True
        elif d == 'off':
            verbose = False
        else:
            raise InvalidSyntax()
        return True
    return False


conn = BlockingConnection()
ch = conn.channel()
assert (len(argv) > 1)
exchange = argv[1]
ch.exchange_declare(exchange=exchange, exchange_type='fanout')
queue = ch.queue_declare(queue='', exclusive=True)
queue_name = queue.method.queue
ch.queue_bind(queue=queue_name, exchange=exchange)

ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

try:
    ch.start_consuming()
finally:
    ch.stop_consuming()
    conn.close()
