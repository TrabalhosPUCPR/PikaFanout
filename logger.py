from pika import BlockingConnection
from sys import argv

verbose = True


class Error(Exception):
    def __init__(self, m="Sintaxe invalida"):
        self.message = m


def validate(sev):
    msg = ""
    print(sev)
    if sev == "DEBUG" or \
            sev == "WARNING" or \
            sev == "INFO" or \
            sev == "ERROR" or \
            sev == "CRITICAL":
        return msg
    else:
        raise Error("Severidade incorreta")


def callback(channel, method, properties, body):
    msg = body.decode('utf-8')
    s = msg.split(":")
    try:
        if not log(s):
            verbos(s)
    except Error as e:
        print(e.message)
    except:
        print("Sintaxe invalida")


def log(s):
    global verbose
    if len(s) == 4:
        if verbose:
            validate(s[1])
            print(f"[{s[1]}] {s[0]}|pid:{s[2]}: {s[3]}")
        return True
    return False


def verbos(s):
    global verbose
    if len(s) == 2 and s[0] == 'verbose':
        if s[1] == 'on':
            verbose = True
        elif s[1] == 'off':
            verbose = False
        else:
            raise Error()
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
    print("Start consuming...")
    ch.start_consuming()
finally:
    print("Stop consuming.")
    ch.stop_consuming()
    conn.close()
