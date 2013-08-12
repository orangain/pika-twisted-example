# coding: utf-8

import time
import logging
import pika

QUEUE_NAME = 'next_queue'
RETRY_COUNT = 5
RETRY_SLEEP = 4

logger = logging.getLogger(__name__)

def main():
    logging.basicConfig(level=logging.INFO)

    parameters = pika.ConnectionParameters()
    for i in range(RETRY_COUNT):
        try:
            connection = pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            if i == RETRY_COUNT - 1:
                raise

            logger.info('Failed to connect; will retry in %s seconds.' % RETRY_SLEEP)
            time.sleep(RETRY_SLEEP)
        else:
            break

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(_callback, queue=QUEUE_NAME)

    logger.info('Start consuming')
    channel.start_consuming()

def _callback(ch, method, properties, body):
    logger.info('Received: %s' % body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
