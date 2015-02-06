import logging
import pika
from pipeline import (is_control, is_data, extract_data, extract_control,
    datamessage, controlmessage, ENDOFLIST)
import json #TODO make the serialization format configurable
import uuid

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
#default rabbitmq settings
LOCALHOST_SETTINGS = {'USER':'',
                      'PASSWORD':'',
                      'HOST':'localhost'}

def deserialize(data):
    '''change the function to change serializing fmt
    '''
    return json.loads(data)

def serialize(obj):
    '''change the function to change serializing fmt
    '''
    return json.dumps(obj, ensure_ascii=False)


class simpleAsyncPhase(object):
    '''simple consumer and producer object'''
    EXCHANGE_TYPE = 'fanout'
    ROUTING_KEY = ''
    def __init__(self, component, conf={}, amqp_url=None, prevPhase=None, nextPhases = []):
        self._conf = conf
        self.set_parent(prevPhase)
        self.set_children(nextPhases)
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        if  amqp_url:
            self._url = amqp_url
        else:
            self._url = amqp_url

        #TODO instanciate the class here? or get an instanciated class ?
        # instanciating th class
        self.component = component
        uuid_str = str(uuid.uuid4())
        self._classname = self.component.__class__.__name__
        self.EXCHANGE = self._classname + '.exchange.' + uuid_str
        self.QUEUE = self._classname + '.queue.' + uuid_str
        self._app_id = self._classname

    def set_parent(self, parentPhase):
        self._parent = parentPhase
        if not self._parent:
            self._is_source = True
        else:
            self._is_source = False

    def set_children(self, nextPhases):
        if type(nextPhases) == type(()) or \
                type(nextPhases) == type([]):
            self._next = nextPhases
        else:
            self._next = (nextPhases,)
        if not self._next:
            self._is_sink = True
        else:
            for p in self._next:
                p.set_parent(self)
            self._is_sink = False



    def __iter__(self):
        #depth first search of the phase dag starting from self._phase
        yield self
        for pd in self._next:
            for phase in pd:
                yield phase

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        #TODO: get this from constructor
        return pika.TornadoConnection(pika.ConnectionParameters('127.0.0.1'),
                                      self.on_connection_open,
                                      stop_ioloop_on_close=self._is_sink)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('Connection opened')
        self._connection = unused_connection
        self.add_on_connection_close_callback()
        self.open_channel()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
            self._connection.ioloop.start()
            self._connection.ioloop.close()
        else:
            LOGGER.warning('Connection closed, not sure what to do: (%s) %s',
                    reply_code, reply_text)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.info('Channel %i was closed: (%s) %s',
                channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                        exchange_name,
                        self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s',
                self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        #bind to parent
        if not self._is_source:
            self._channel.queue_bind(self.on_bindok, self.QUEUE,
                    self._parent.EXCHANGE, self.ROUTING_KEY)
        else:
            self.start_producing()

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        self.acknowledge_message(basic_deliver.delivery_tag)
        LOGGER.info('Received message # %s from %s: %s',
            basic_deliver.delivery_tag, properties.app_id, body)
        #TODO check if message is control or data
        msg = deserialize(body)
        if is_control(msg):
            #TODO do control stuff
            msg_data = extract_control(msg)
            if self._is_sink:
                if msg_data == ENDOFLIST:
                    LOGGER.info('Stopping')
                    self.stop()
            #TODO send only if this queue is done processing
            message_to_send = controlmessage(msg_data)
            pass
        elif is_data(msg):
            msg_data = extract_data(msg)
            msg_data = self.component.process_data(msg_data)
            #check if result is None
            if msg_data is None:
                LOGGER.info("excluding filtered result from %s", properties.app_id)
                return
            message_to_send = datamessage(msg_data)

        self._push_to_next(message_to_send, properties)

    def _push_to_next(self, message_to_send, properties=None):
        #TODO push to exchange
        if not properties:
            headers = {}
        else:
            headers = properties.headers
        publish_properties = pika.BasicProperties(app_id=self._app_id,
                        content_type='application/json',
                        headers=headers)

        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                        serialize(message_to_send),
                        publish_properties)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                self.QUEUE)

    def start_producing(self):
        #jumpstart the wagon by calling the producer
        for msg in self.component.process_list([]):
            if msg is not None:
                self._push_to_next(datamessage(msg))

        #TODO take care of num_sent for data consistency
        self._push_to_next(controlmessage(ENDOFLIST))

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue bound')
        if self._is_source:
            self.start_producing()
        else:
            self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self.pipeline.stop()
        #self._connection.ioloop.start()
        LOGGER.info('Stopped')
