# coding: utf-8

import logging
import pika
from pika.adapters import TwistedProtocolConnection
from twisted.internet import reactor, task, defer, protocol

PREV_QUEUE_NAME = 'prev_queue'
NEXT_QUEUE_NAME = 'next_queue'
AMQP_DEFAULT_PORT = 5672

logger = logging.getLogger(__name__)

class TwistedMessageQueue(object):

    def __init__(self, host, port, queue_name):
        port = port or AMQP_DEFAULT_PORT

        self.queue_name = queue_name
        self.parameters = pika.ConnectionParameters(
                host=host, port=port)
        self.on_close = lambda failure: None
        self.on_ready = lambda channel: None

        self._connect()

    def _connect(self):
        cc = protocol.ClientCreator(reactor, TwistedProtocolConnection,
                self.parameters)
        d = retry(5, 4,
                cc.connectTCP, self.parameters.host, self.parameters.port)
        d.addCallback(lambda protocol: protocol.ready)
        d.addCallback(self._on_connect)

    @defer.inlineCallbacks
    def _on_connect(self, connection):
        logger.info('Connected to %s:%s' % (self.parameters.host, self.parameters.port))
        d = defer.Deferred()
        d.addErrback(self._on_close)
        connection.ready = d

        self.channel = yield connection.channel()
        queue = yield self.channel.queue_declare(queue=self.queue_name)
        yield self._on_ready(self.channel)

    def _on_close(self, failure):
        return self.on_close(failure)

    def _on_ready(self, channel):
        return self.on_ready(channel)

    @defer.inlineCallbacks
    def consume(self, callback):

        @defer.inlineCallbacks
        def read(queue_object):
            ch, method, properties, body = yield queue_object.get()
            yield callback(body)
            yield ch.basic_ack(delivery_tag=method.delivery_tag)

        yield self.channel.basic_qos(prefetch_count=1)
        queue_object, consumer_tag = yield self.channel.basic_consume(
                queue=self.queue_name, no_ack=False)
        l = task.LoopingCall(read, queue_object)
        l.start(0.01)

        logger.info('Start consuming')

    def publish(self, message):
        return self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message)

def retry(times, sleep, func, *args, **kwargs):
    """retry a defer function

    @param times: how many times to retry
    @param sleep: seconds to sleep before retry
    @param func: defer function

    See: http://blog.ez2learn.com/2009/09/26/an-auto-retry-recipe-for-twisted/
    """
    errorList = []
    deferred = defer.Deferred()
    def run():
        logger.info('Try %s(*%s, **%s)', func.__name__, args, kwargs)
        d = func(*args, **kwargs)
        d.addCallbacks(deferred.callback, error)
    def error(error):
        errorList.append(error)
        # Retry
        if len(errorList) < times:
            logger.warn('Failed to try %s(*%s, **%s) %d times, retry...',
                    func.__name__, args, kwargs, len(errorList))
            reactor.callLater(sleep, run)
        # Fail
        else:
            logger.error('Failed to try %s(*%s, **%s) over %d times, stop',
                    func.__name__, args, kwargs, len(errorList))
            deferred.errback(errorList)
    run()
    return deferred

class Worker(object):

    def run(self):
        self.next_queue = TwistedMessageQueue('localhost', 5672, NEXT_QUEUE_NAME)
        self.next_queue.on_close = self.on_close

        self.prev_queue = TwistedMessageQueue('localhost', 5672, PREV_QUEUE_NAME)
        self.prev_queue.on_close = self.on_close
        self.prev_queue.on_ready = self.on_prev_queue_ready

        reactor.run()

    def on_prev_queue_ready(self, channel):
        logger.info('Prev queue is ready!')
        return self.prev_queue.consume(self.on_consume)

    def on_consume(self, body):
        logger.info('Received: %s' % body)
        body = body.upper()
        logger.info('Publishing: %s' % body)

        return self.next_queue.publish(body)

    def on_close(self, failure):
        logger.error('Connection closed: %s' % failure)
        if reactor.running:
            reactor.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    worker.run()
