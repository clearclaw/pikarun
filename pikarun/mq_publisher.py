#! /usr/bin/env python

import logging, pika
from logtool import log_func

LOG = logging.getLogger (__name__)

# pylint: disable-msg=R0904
class MQ_Publisher (object):
  """A publisher that will handle unexpected interactions with
  RabbitMQ such as channel and connection closures.

  If RabbitMQ closes the connection, it will reopen it. You should
  look at the output, as there are limited reasons why the connection may
  be closed, which usually are tied to permission related issues or
  socket timeouts.

  It uses delivery confirmations and illustrates one way to keep track of
  messages that have been sent and if they've been confirmed by RabbitMQ.
  """

  @log_func
  def __init__ (self, amqp_url, exchange = None, exchange_type = "topic",
                routing_key = None, pubsub_queue = None,
                interval = 5):
    """Setup the example publisher object, passing in the URL we will use
    to connect to RabbitMQ.

    :param str amqp_url: The URL for connecting to RabbitMQ
    """
    self._connection = None
    self._channel = None
    self._deliveries = list ()
    self._acked = 0
    self._nacked = 0
    self._message_number = 0
    self._stopping = False
    self._url = amqp_url
    self._exchange = exchange
    self._exchange_type = exchange_type
    self._routing_key = routing_key
    self._pubsub_queue = pubsub_queue
    self._interval = interval

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
    """Close the connection to RabbitMQ."""
    LOG.info ("Closing connection")
    self._connection.close ()

  @log_func
  def add_on_connection_close_callback (self):
    """This method adds an on close callback that will be invoked
    when RabbitMQ closes the connection to the publisher
    unexpectedly.
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
  def on_connection_open (self, connection):
    """Called once the connection to RabbitMQ has been
    established. It passes the handle to the connection object.

    :type connection: pika.SelectConnection
    """
    LOG.info ("Connection opened")
    self._connection = connection
    self.add_on_connection_close_callback ()
    self.open_channel ()

  @log_func
  def add_on_channel_close_callback (self):
    """Sets up to call the on_channel_closed method if RabbitMQ
    unexpectedly closes the channel.
    """
    LOG.info ("Adding channel close callback")
    self._channel.add_on_close_callback (self.on_channel_closed)
    self._channel.confirm_delivery (self.on_delivery_confirmation)

  @log_func
  def on_channel_closed (self, method_frame):
    """Invoked when RabbitMQ unexpectedly closes the channel.
    Channels are usually closed if you attempt to do something that
    violates the protocol, such as redeclare an exchange or queue
    with different paramters. In this case, we'll close the
    connection to shutdown the object.

    :param pika.frame.Method method_frame: The Channel.Close method frame
    """
    LOG.warning ("Channel was closed:  (%s) %s",
               method_frame.method.reply_code,
               method_frame.method.reply_text)
    self._connection.close ()
    # FIXME: Not re-open the connection?

  @log_func
  def on_channel_open (self, channel):
    """This method is invoked when the channel has been opened.
    Since the channel is now open, we'll declare the exchange to
    use.

    :param pika.channel.Channel channel: The channel object
    """
    LOG.info ("Channel opened")
    self._channel = channel
    self.add_on_channel_close_callback ()
    if self._exchange:
      self.setup_exchange (self._exchange)
    else: # No pubsub queue
      self.start_publishing ()

  @log_func
  def setup_exchange (self, exchange_name):
    """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
    command. When it is complete, the on_exchange_declareok method will
    be invoked.

    :param str|unicode exchange_name: The name of the exchange to declare
    """
    LOG.info ("Declaring exchange %s", exchange_name)
    self._channel.exchange_declare (self.on_exchange_declareok,
                                    exchange_name, self._exchange_type)

  @log_func
  def on_exchange_declareok (self, frame): # pylint: disable-msg=W0613
    """Invoked when RabbitMQ has finished the Exchange.Declare RPC
    command.

    :param pika.Frame.Method frame: Exchange.DeclareOk response frame
    """
    LOG.info ("Exchange declared")
    self.setup_queue (self._pubsub_queue)

  @log_func
  def setup_queue (self, queue_name):
    """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
    command. When it is complete, the on_queue_declareok method will
    be invoked.

    :param str|unicode queue_name: The name of the queue to declare.
    """
    LOG.info ("Declaring queue %s", queue_name)
    self._channel.queue_declare (self.on_queue_declareok, queue_name)

  @log_func
  def on_queue_declareok (self, method_frame): # pylint: disable-msg=W0613
    """Method invoked by pika when the Queue.Declare RPC call made in
    setup_queue has completed. In this method we will bind the queue
    and exchange together with the routing key by issuing the Queue.Bind
    RPC command. When this command is complete, the on_bindok method will
    be invoked.

    :param pika.frame.Method method_frame: The Queue.DeclareOk frame
    """
    LOG.info ("Binding %s to %s with %s",
              self._exchange, self._pubsub_queue, self._routing_key)
    self._channel.queue_bind (self.on_bindok, self._pubsub_queue,
                              self._exchange, self._routing_key)

  @log_func
  def on_delivery_confirmation (self, method_frame):
    """Invoked when RabbitMQ responds to a Basic.Publish RPC
    command, passing in either a Basic.Ack or Basic.Nack frame with
    the delivery tag of the message that was published. The delivery
    tag is an integer counter indicating the message number that was
    sent on the channel via Basic.Publish. Here we're just doing
    house keeping to keep track of stats and remove message numbers
    that we expect a delivery confirmation of from the list used to
    keep track of messages that are pending confirmation.

    :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame
    """
    confirmation_type = method_frame.method.NAME.split (".")[1].lower ()
    LOG.debug ("Received %s for delivery tag: %i",
              confirmation_type, method_frame.method.delivery_tag)
    if confirmation_type == "ack":
      self._acked += 1
    elif confirmation_type == "nack":
      self._nacked += 1
    self._deliveries.remove (method_frame.method.delivery_tag)
    LOG.debug ("Published %i messages, %i have yet to be confirmed, "
               "%i were acked and %i were nacked",
               self._message_number, len (self._deliveries),
               self._acked, self._nacked)

  @log_func
  def enable_delivery_confirmations (self):
    """Send the Confirm.Select RPC method to RabbitMQ to enable
    delivery confirmations on the channel. The only way to turn this
    off is to close the channel and create a new one.

    When the message is confirmed from RabbitMQ, the
    on_delivery_confirmation method will be invoked passing in a
    Basic.Ack or Basic.Nack method from RabbitMQ that will indicate
    which messages it is confirming or rejecting.
    """
    LOG.debug ("Issuing Confirm.Select RPC command")
    self._channel.confirm_delivery (self.on_delivery_confirmation)

  @log_func
  def step (self):
    """Called in each iteration of the loop.  Override!"""
    pass

  @log_func
  def process (self):
    """If the class is not stopping, perform the defined
    self.step().  Once the step has been done, schedule the next
    step.
    """
    self.step ()
    if self._stopping:
      return
    self.schedule_next_iteration ()

  @log_func
  def schedule_next_iteration (self):
    """If we are not closing our connection to RabbitMQ, schedule another
    message to be delivered in self._interval seconds.
    """
    if self._stopping:
      return
    if not self._interval:
      self.step ()
      self.stop () # We only run once
      return
    LOG.info ("Scheduling next iteration for %0.1f seconds", self._interval)
    self._connection.add_timeout (self._interval, self.process)

  @log_func
  def start_publishing (self):
    """This method will enable delivery confirmations and schedule the
    first message to be sent to RabbitMQ
    """
    LOG.info ("Issuing consumer related RPC commands")
    self.enable_delivery_confirmations ()
    self.schedule_next_iteration ()

  @log_func
  def publish (self, queue = None, routing_key = None, msg = None):
    """Simplistic wrapper for publishing messages.  Uses the queue
    name to determine if it should be sent to the exchange or use
    the default exchange based on the queue name, and uses canned
    message properties.

    After publishing the message to RabbitMQ, appends a list of
    deliveries with the message number that was sent.  This list
    will be used to check for delivery confirmations in the
    on_delivery_confirmations method.

    :param str|unicode queue: Queue to publish to
    :param str|unicode routing_key: Ruoting key for non-dispatch messages
    :param str|unicode msg: Body of the message to publish
    """
    if not queue or not msg:
      LOG.error ("Publish called without a queue or message.")
      raise ValueError
    properties = pika.BasicProperties (
      # app_id = "example-publisher", # FIXME: Need proper string
      delivery_mode = 2, # make message persistent
      content_type = "application/json")
    exch = self._exchange if queue == self._pubsub_queue else ""
    q = routing_key if queue == self._pubsub_queue else queue
    self._channel.basic_publish (
      exchange = exch,
      routing_key = q,
      properties = properties,
      body = msg)
    self._message_number += 1
    self._deliveries.append (self._message_number)
    LOG.debug ("Published message # %i", self._message_number)

  @log_func
  def on_bindok (self, frame): # pylint: disable-msg=W0613
    """This method is invoked by pika when it receives the Queue.BindOk
    response from RabbitMQ. Since we know we're now setup and bound, it's
    time to start publishing."""
    LOG.info ("Queue bound")
    self.start_publishing ()

  @log_func
  def close_channel (self):
    """Invoke this command to close the channel with RabbitMQ by sending
    the Channel.Close RPC command.
    """
    LOG.warning ("Closing the channel")
    self._channel.close ()

  @log_func
  def open_channel (self):
    """This method will open a new channel with RabbitMQ by issuing the
    Channel.Open RPC command. When RabbitMQ confirms the channel is open
    by sending the Channel.OpenOK RPC reply, the on_channel_open method
    will be invoked.
    """
    LOG.warning ("Creating a new channel")
    self._connection.channel (on_open_callback = self.on_channel_open)

  @log_func
  def run (self):
    """Run by connecting and then starting the IOLoop.
    """
    self._connection = self.connect ()
    self._connection.ioloop.start ()

  @log_func
  def stop (self):
    """Stop by closing the channel and connection. The IOLoop is
    started because this method is invoked by the Try/Catch below
    when KeyboardInterrupt is caught.  Starting the IOLoop again
    will allow the publisher to cleanly disconnect from RabbitMQ.
    """
    LOG.warning ("Stopping")
    self._stopping = True
    self.close_channel ()
    self.close_connection ()
    self._connection.ioloop.start ()
