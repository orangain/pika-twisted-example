# coding: utf-8

import logging
import pika
from pika.adapters import TwistedProtocolConnection
from twisted.internet import reactor, task, defer, protocol

PREV_QUEUE_NAME = 'prev_queue'
NEXT_QUEUE_NAME = 'next_queue'

logger = logging.getLogger(__name__)

def setup_consumer(queue_name, callback, on_close):

    @defer.inlineCallbacks
    def run(connection):
        d = defer.Deferred()
        d.addErrback(on_close)
        connection.ready = d

        channel = yield connection.channel()
        queue = yield channel.queue_declare(queue=queue_name)
        yield channel.basic_qos(prefetch_count=1)
        queue_object, consumer_tag = yield channel.basic_consume(
                queue=queue_name, no_ack=False)
        l = task.LoopingCall(read, queue_object)
        l.start(0.01)

    @defer.inlineCallbacks
    def read(queue_object):
        ch,method,properties,body = yield queue_object.get()
        yield callback(body)
        yield ch.basic_ack(delivery_tag=method.delivery_tag)

    parameters = pika.ConnectionParameters()
    cc = protocol.ClientCreator(reactor, TwistedProtocolConnection, parameters)
    d = retry(5, 4, lambda: cc.connectTCP(parameters.host, parameters.port))
    d.addCallback(lambda protocol: protocol.ready)
    d.addCallback(run)

def setup_publisher(queue_name, on_ready, on_close):

    @defer.inlineCallbacks
    def run(connection):
        connection.add_on_close_callback(on_close)
        channel = yield connection.channel()
        queue = yield channel.queue_declare(queue=queue_name)
        yield on_ready(channel)

    parameters = pika.ConnectionParameters()
    cc = protocol.ClientCreator(reactor, TwistedProtocolConnection, parameters)
    d = retry(5, 4, lambda: cc.connectTCP(parameters.host, parameters.port))
    d.addCallback(lambda protocol: protocol.ready)
    d.addCallback(run)

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
        setup_publisher(NEXT_QUEUE_NAME, self.on_ready, self.on_close)
        setup_consumer(PREV_QUEUE_NAME, self.on_consume, self.on_close)

        reactor.run()

    def on_consume(self, body):
        logger.info('Received: %s' % body)
        body = body.upper()
        logger.info('Publishing: %s' % body)

        return self.next_channel.basic_publish(
            exchange='',
            routing_key=NEXT_QUEUE_NAME,
            body=body)

    def on_ready(self, channel):
        logger.info('Ready!')
        self.next_channel = channel

    def on_close(self, failure):
        logger.error('Connection closed: %s' % failure)
        reactor.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    worker.run()
