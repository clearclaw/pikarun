#! /usr/bin/env python

import logging, pika
from logtool import log_func

LOG = logging.getLogger (__name__)

# pylint: disable-msg=R0904
class MQ_Consumer (object):
  """This is an example consumer that will handle unexpected interactions
  with RabbitMQ such as channel and connection closures.

  If RabbitMQ closes the connection, it will reopen it. You should
  look at the output, as there are limited reasons why the connection may
  be closed, which usually are tied to permission related issues or
  socket timeouts.

  If the channel is closed, it will indicate a problem with one of the
  commands that were issued and that should surface in the output as well.
  """

  @log_func
  def __init__ (self, amqp_url, exchange = None, exchange_type = "topic",
                routing_key = None, pubsub_queue = None, rpc_queues = None):
    """Create a new instance of the consumer class, passing in the AMQP
    URL used to connect to RabbitMQ.

    :param str amqp_url: The AMQP url to connect with
    """
    self._connection = None
    self._channel = None
    self._pubsub_consumer_tag = None
    self._rpc_consumer_tags = list ()
    self._url = amqp_url
    self._exchange = exchange
    self._exchange_type = exchange_type
    self._routing_key = routing_key
    self._pubsub_queue = pubsub_queue
    self._rpc_queues = rpc_queues

  @log_func
  def connect (self):
    """Connect to RabbitMQ, returning the connection handle.  When
    the connection is established, the on_connection_open method
    will be invoked.

    :rtype: pika.SelectConnection
    """
    LOG.info ("Connecting to %s", self._url)
    return pika.SelectConnection (pika.URLParameters (self._url),
                                  self.on_connection_open)
  @log_func
  def close_connection (self):
    """This method closes the connection to RabbitMQ."""
    LOG.warning ("Closing connection")
    self._connection.close ()

  @log_func
  def add_on_connection_close_callback (self):
    """Adds an on close callback that will be invoked when RabbitMQ
    closes the connection to the publisher unexpectedly.
    """
    LOG.info ("Adding connection close callback")
    self._connection.add_on_close_callback (self.on_connection_closed)

  @log_func
  def on_connection_closed (self, connection, reply_code, reply_text):
    """Invoked when the connection to RabbitMQ is closed
    unexpectedly. Since it is unexpected, we will reconnect to
    RabbitMQ if it disconnects.

    :type connection: pika.SelectConnection
    :type int: Reply code for the closure
    :param str|unicode reply_text: Any message accompanying the closure
    """
    LOG.warning ("Server closed connection, reopening: %s(%s) %s",
                 connection, reply_code, reply_text)
    self._channel = None
    self._connection = self.connect ()

  @log_func
  def on_connection_open (self, connection): # pylint: disable-msg=W0613
    """This method is called by pika once the connection to RabbitMQ has
    been established. It passes the handle to the connection object in
    case we need it, but in this case, we"ll just mark it unused.

    :type connection: pika.SelectConnection
    """
    LOG.info ("Connection opened")
    self.add_on_connection_close_callback ()
    self.open_channel ()

  @log_func
  def add_on_channel_close_callback (self):
    """This method tells pika to call the on_channel_closed method if
    RabbitMQ unexpectedly closes the channel.
    """
    LOG.info ("Adding channel close callback")
    self._channel.add_on_close_callback (self.on_channel_closed)

  @log_func
  def on_channel_closed (self, method_frame):
    """Invoked by pika when RabbitMQ unexpectedly closes the channel.
    Channels are usually closed if you attempt to do something that
    violates the protocol, such as redeclare an exchange or queue with
    different paramters. In this case, we"ll close the connection
    to shutdown the object.

    :param pika.frame.Method method_frame: The Channel.Close method frame
    """
    LOG.warning ("Channel was closed: (%s) %s",
                 method_frame.method.reply_code,
                 method_frame.method.reply_text)
    self._connection.close ()

  @log_func
  def on_channel_open (self, channel):
    """Invoked by pika when the channel has been opened.  Since the
    channel is now open, declare the exchange to use.

    :param pika.channel.Channel channel: The channel object
    """
    LOG.info ("Channel opened")
    self._channel = channel
    self._channel.basic_qos (prefetch_count = 1)
    self.add_on_channel_close_callback ()
    if self._exchange:
      self.setup_exchange (self._exchange)
    if self._rpc_queues:
      for q in self._rpc_queues:
        self.setup_rpc_queue (q)
      self.start_rpc_consuming ()

  @log_func
  def setup_exchange (self, exchange_name):
    """Setup the exchange. When it is complete, the
    on_exchange_declareok method will be invoked.

    :param str|unicode exchange_name: The name of the exchange to declare
    """
    LOG.info ("Declaring exchange %s", exchange_name)
    self._channel.exchange_declare (self.on_exchange_declareok,
                                    exchange_name,
                                    self._exchange_type)

  @log_func
  def on_exchange_declareok (self, frame): # pylint: disable-msg=W0613
    """Invoked when the Exchange.Declare RPC has finished.

    :param pika.Frame.Method frame: Exchange.DeclareOk response frame
    """
    LOG.info ("Exchange declared")
    self.setup_pubsub_queue (self._pubsub_queue)

  @log_func
  def setup_pubsub_queue (self, queue_name):
    """Setup the queue. When it is complete, the
    on_pubsub_queue_declareok method will be invoked .

    :param str|unicode queue_name: The name of the queue to declare.
    """
    LOG.info ("Declaring queue %s", queue_name)
    self._channel.queue_declare (self.on_pubsub_queue_declareok, queue_name)

  @log_func
  def on_rpc_queue_declareok (self, method_frame):
    """Invoked when the Queue.Declare RPC call made in
    setup_rpc_queue has completed. Bind the queue and exchange
    together with the routing key. When complete, the on_bindok
    method will be invoked.

    :param pika.frame.Method method_frame: The Queue.DeclareOk frame
    """
    LOG.info ("Created RPC queue: %s", method_frame.method.queue)

  @log_func
  def on_pubsub_queue_declareok (self,
                                 method_frame): # pylint: disable-msg=W0613
    """Invoked when the Queue.Declare RPC call made in
    setup_pubsub_queue has completed. Bind the queue and exchange
    together with the routing key. When complete, the on_bindok
    method will be invoked.

    :param pika.frame.Method method_frame: The Queue.DeclareOk frame
    """
    LOG.info ("Binding %s to %s with %s",
              self._exchange, self._pubsub_queue, self._routing_key)
    self._channel.queue_bind (self.on_bindok, self._pubsub_queue,
                              self._exchange, self._routing_key)

  @log_func
  def add_on_cancel_callback (self):
    """Add a callback to be invoked if RabbitMQ cancels the consumer
    for some reason. If RabbitMQ does cancel the consumer,
    on_consumer_cancelled will be invoked by pika.

    """
    LOG.info ("Adding consumer cancellation callback")
    self._channel.add_on_cancel_callback (self.on_consumer_cancelled)

  @log_func
  def on_consumer_cancelled (self, method_frame):
    """Invoked when RabbitMQ sends a Basic.Cancel for a consumer
    receiving messages.

    :param pika.frame.Method method_frame: The Basic.Cancel frame
    """
    LOG.warning ("Consumer was cancelled remotely, shutting down: %r",
              method_frame)
    self._channel.close ()

  @log_func
  def acknowledge_message (self, delivery_tag):
    """Acknowledge message delivery by sending a Basic.Ack RPC
    method for the delivery tag.

    :param int delivery_tag: The delivery tag from the Basic.Deliver frame
    """
    LOG.debug ("Acknowledging message %s", delivery_tag)
    self._channel.basic_ack (delivery_tag)

  @log_func
  def reject_message (self, delivery_tag):
    """Reject message delivery by sending a Basic.Reject RPC
    method for the delivery tag.

    :param int delivery_tag: The delivery tag from the Basic.Deliver frame
    """
    LOG.info ("Rejecting message %s", delivery_tag)
    self._channel.basic_reject (delivery_tag = delivery_tag,
                                requeue = True)

  @log_func
  def handle_message (
      self, channel, method, properties, body): # pylint: disable-msg=W0613
    """Override in derived classes for message processing.  Should
    return a boolean.  If False, then the message will be
    acknowledged after processing.

    :param pika.channel.Channel channel: The channel object
    :param pika.Spec.Basic.Deliver: method
    :param pika.Spec.BasicProperties: properties
    :param str|unicode body: The message body
    """
    return False

  @log_func
  def on_message (self, channel, method, properties, body):
    """Invoked when a message is delivered from RabbitMQ. The
    method object that is passed in carries the exchange,
    routing key, delivery tag and a redelivered flag for the
    message. The properties passed in is an instance of
    BasicProperties with the message properties and the body is the
    message that was sent.

    :param pika.channel.Channel channel: The channel object
    :param pika.Spec.Basic.Deliver: method
    :param pika.Spec.BasicProperties: properties
    :param str|unicode body: The message body
    """
    LOG.debug ("Received message # %s from %s: %s",
              method.delivery_tag, properties.app_id, body)
    rc = self.handle_message (channel, method,
                              properties, body)
    if not rc:
      LOG.debug ("Message # %s from %s, marked DONE!",
                 method.delivery_tag, properties.app_id)
      self.acknowledge_message (method.delivery_tag)
    else:
      LOG.info ("Message # %s from %s, REJECTING!",
                 method.delivery_tag, properties.app_id)
      self.reject_message (method.delivery_tag)

  @log_func
  def on_cancelok (self, frame): # pylint: disable-msg=W0613
    """Invoked when RabbitMQ acknowledges the cancellation of a
    consumer.  At this point we will close the connection which will
    automatically close the channel if it"s open.

    :param pika.frame.Method frame: The Basic.CancelOk frame
    """
    LOG.warning ("RabbitMQ acknowledged the cancellation of the consumer")
    self.close_connection ()

  @log_func
  def stop_consuming (self):
    """Tell RabbitMQ that you would like to stop consuming by
    sending the Basic.Cancel RPC command.
    """
    LOG.debug ("Sending a Basic.Cancel RPC command to RabbitMQ")
    if self._pubsub_queue:
      self._channel.basic_cancel (self.on_cancelok, self._pubsub_consumer_tag)
    for tag in self._rpc_consumer_tags:
      self._channel.basic_cancel (self.on_cancelok, tag)
    # FIXME: Race condition on cancelok closing the connection.

  @log_func
  def start_pubsub_consuming (self):
    """Setup the consumer by first calling add_on_cancel_callback so
    that the object is notified if RabbitMQ cancels the consumer. It
    then issues the Basic.Consume RPC command which returns the
    consumer tag that is used to uniquely identify the consumer with
    RabbitMQ. We keep the value to use it when we want to cancel
    consuming. The on_message method is passed in as a callback to
    invoke when a message is fully received.
    """
    LOG.info ("Setting up pubsub consumer.")
    self.add_on_cancel_callback ()
    self._pubsub_consumer_tag = self._channel.basic_consume (
      self.on_message, self._pubsub_queue)

  @log_func
  def start_rpc_consuming (self):
    """Setup the consumer by first calling add_on_cancel_callback so
    that the object is notified if RabbitMQ cancels the consumer. It
    then issues the Basic.Consume RPC command which returns the
    consumer tag that is used to uniquely identify the consumer with
    RabbitMQ. We keep the value to use it when we want to cancel
    consuming. The on_message method is passed in as a callback to
    invoke when a message is fully received.
    """
    LOG.info ("Setting up rpc consumer.")
    self.add_on_cancel_callback ()
    for q in self._rpc_queues:
      LOG.debug ("RPC consuming: %s", q)
      self._rpc_consumer_tags.append (self._channel.basic_consume (
        self.on_message, queue = q))

  @log_func
  def on_bindok (self, frame): # pylint: disable-msg=W0613
    """Invoked when the Queue.Bind method has completed. At this
    point we will start consuming messages by calling
    start_pubsub_consuming which will invoke the needed RPC commands
    to start the process.

    :param pika.frame.Method frame: The Queue.BindOk response frame
    """
    LOG.info ("Queue bound")
    self.start_pubsub_consuming ()

  @log_func
  def close_channel (self):
    """Call to close the channel with RabbitMQ cleanly by issuing the
    Channel.Close RPC command.
    """
    LOG.warning ("Closing the channel")
    self._channel.close ()

  @log_func
  def setup_rpc_queue (self, queue_name):
    """Setup the RPC queue.

    :param str|unicode queue_name: The name of the queue to declare.
    """
    LOG.info ("Declaring RPC queue: %s", queue_name)
    # :param queue: The queue name
    # :type queue: str or unicode
    # :param bool passive: Only check to see if the queue exists
    # :param bool durable: Survive reboots of the broker
    # :param bool exclusive: Only allow access by the current connection
    # :param bool auto_delete: Delete after consumer cancels or disconnects
    # :param bool nowait: Do not wait for a Queue.DeclareOk
    # :param dict arguments: Custom key/value arguments for the queue
    self._channel.queue_declare (self.on_rpc_queue_declareok,
                                 queue = queue_name, durable = True,
                                 exclusive = False, auto_delete = False)

  @log_func
  def open_channel (self):
    """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
    command. When RabbitMQ responds that the channel is open, the
    on_channel_open callback will be invoked.

    """
    LOG.info ("Creating a new channel")
    self._connection.channel (on_open_callback = self.on_channel_open)

  @log_func
  def on_timeout (self):
    """Called when the IOLoop is interrupted by the timer expiring.
    Override for local customisation."""
    pass

  @log_func
  def run (self, timeout = None):
    """Run the example consumer by connecting to RabbitMQ and then
    starting the IOLoop to block and allow the SelectConnection to operate.

    :param integer timeout: How often to invoke the timeout method,  Default: None (disabled).
    """
    if timeout:
      self._connection.add_timout (timeout, # pylint: disable-msg=E1103
                                   self.on_timeout)
    self._connection = self.connect ()
    self._connection.ioloop.start ()

  @log_func
  def stop (self):
    """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
    with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
    will be invoked, which will then close the channel and
    connection. The IOLoop is started again because this method is invoked
    when CTRL-C is pressed raising a KeyboardInterrupt exception. This
    exception stops the IOLoop which needs to be running for pika to
    communicate with RabbitMQ. All of the commands issued prior to starting
    the IOLoop will be buffered but not processed.
    """
    LOG.warning ("Stopping")
    self.stop_consuming ()
    self._connection.ioloop.start ()

  @log_func
  def publish (self, queue = None, routing_key = None, msg = None):
    """Simplistic wrapper for publishing messages.  Uses the queue
    name to determine if it should be sent to the exchange or use
    the default exchange based on the queue name, and uses canned
    message properties.

    :param str|unicode queue: Queue to publish to
    :param str|unicode routing_key: Ruoting key for non-dispatch messages
    :param str|unicode msg: Body of the message to publish
    """
    if not queue or not msg:
      LOG.error ("Publish called without a queue or message.")
      raise ValueError
    if queue == self._pubsub_queue:
      self._channel.basic_publish (
        exchange = self._exchange,
        routing_key = routing_key,
        properties = pika.BasicProperties (
          delivery_mode = 2, # make message persistent
          content_type = "application/json",
          ),
        body = msg)
    else: # RPC message
      self._channel.basic_publish (
        exchange = "",
        routing_key = queue,
        properties = pika.BasicProperties (
          delivery_mode = 2, # make message persistent
          ),
        body = msg)
