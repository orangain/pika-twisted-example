# coding: utf-8

import sys
import pika

def main(message):
    parameters = pika.ConnectionParameters()
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='prev_queue')

    channel.basic_publish(exchange='',
            routing_key='prev_queue',
            body=message)

if __name__ == '__main__':
    try:
        message = sys.argv[1]
    except IndexError:
        sys.stderr.write('Usage: python send.py MESSAGE\n')
        exit(1)

    main(message)
