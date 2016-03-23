"""Provide a coroutine to forward zmq pub/sub traffic."""

import socket
import asyncio
import aiozmq
from aiozmq import zmq
from enum import IntEnum
from collections import defaultdict


class SubscriptionType(IntEnum):
    Unsubscribe = 0
    Subscribe = 1


class BaseProtocol(aiozmq.ZmqProtocol):

    def __init__(self, handler):
        self.handler = handler
        self.transport = None
        self.wait_closed = None

    def connection_made(self, transport):
        self.transport = transport
        self.wait_closed = asyncio.Future(loop=transport._loop)

    def connection_lost(self, exc):
        self.handler.connection_lost(exc)
        self.wait_closed.set_result(exc)
        self.transport = None


class PublisherProtocol(BaseProtocol):

    def __init__(self, handler):
        super().__init__(handler)
        self.topic_dct = defaultdict(int)

    def msg_received(self, data):
        data = data[0]
        stype, topic = data[0], data[1:]
        if self.topic_dct[topic] == 0:
            assert stype == SubscriptionType.Subscribe
            self.new_subscription(topic)
        if stype == SubscriptionType.Subscribe:
            self.topic_dct[topic] += 1
        elif stype == SubscriptionType.Unsubscribe:
            self.topic_dct[topic] -= 1
        if self.topic_dct[topic] == 0:
            assert stype == SubscriptionType.Unsubscribe
            self.last_unsubscription(topic)

    def new_subscription(self, topic):
        self.handler.new_subscription(topic)

    def last_unsubscription(self, topic):
        self.handler.last_unsubscription(topic)

    def publish(self, topic, data=()):
        self.transport.write([topic] + list(data))

    def close(self):
        if self.transport:
            self.transport.close()


class SubscriberProtocol(BaseProtocol):

    def msg_received(self, data):
        topic, *data = data
        self.topic_received(topic, data)

    def topic_received(self, topic, data):
        self.handler.topic_received(topic, data)

    def subscribe(self, topic):
        self.transport.subscribe(topic)

    def unsubscribe(self, topic):
        self.transport.unsubscribe(topic)

    def close(self):
        if self.transport:
            self.transport.close()


class ForwardingHandler:

    def __init__(self, translate=None):
        if translate is None:
            translate = lambda arg, reverse=False: arg
        self.translate = translate

    def register_subscriber(self, subscriber):
        self.subscriber = subscriber

    def register_publisher(self, publisher):
        self.publisher = publisher

    def new_subscription(self, topic):
        new_topic = self.translate(topic, reverse=False)
        self.subscriber.subscribe(new_topic)

    def last_unsubscription(self, topic):
        new_topic = self.translate(topic, reverse=False)
        self.subscriber.unsubscribe(new_topic)

    def topic_received(self, topic, data):
        new_topic = self.translate(topic, reverse=True)
        self.publisher.publish(new_topic, data)

    def connection_lost(self, exc):
        self.close()

    def close(self):
        self.subscriber.close()
        self.publisher.close()

    @asyncio.coroutine
    def wait_closed(self):
        if self.publisher.wait_closed:
            yield from self.publisher.wait_closed
        if self.subscriber.wait_closed:
            yield from self.subscriber.wait_closed


@asyncio.coroutine
def pubsub_forwarding(host, port,
                      translate=None, bind_address='0.0.0.0', server_port=0,
                      *, loop=None):
    bind_address = socket.gethostbyname(bind_address)
    connect = 'tcp://{}:{}'.format(host, port)
    bind = 'tcp://{}:{}'.format(bind_address, server_port)
    handler = ForwardingHandler(translate)
    # Create subscriber
    _, subscriber = yield from aiozmq.create_zmq_connection(
        lambda: SubscriberProtocol(handler), zmq.SUB, connect=connect, loop=loop)
    handler.register_subscriber(subscriber)
    # Create publisher
    transport, publisher = yield from aiozmq.create_zmq_connection(
        lambda: PublisherProtocol(handler), zmq.XPUB, bind=bind, loop=loop)
    handler.register_publisher(publisher)
    # Return handler
    server_endpoint = list(transport.bindings())[0]
    bind_address, server_port = server_endpoint.split('/')[-1].split(':')
    return handler, bind_address, int(server_port)
