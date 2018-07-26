const mq = require('amqplib');
const EventEmitter = require('events');

class AMPQ extends EventEmitter {
  /**
   * @name constructor
   * @description Initialise the class with required options
   * to successfully facilitate a connection to the message
   * queue.
   *
   * @param {*} incomingConfiguration - connection configuration
   * required to connect to the MQ broker
   * @param {*} topic - the topic to connect to
   */
  constructor(
    incomingConfiguration,
    topic,
    opts = {
      consume: true,
      acknowledge: true,
    },
  ) {
    super();

    /**
     * Declare variables that we will need later.
     */
    this.channel = null;
    this.connection = null;
    this.topic = topic;
    this.configuration = incomingConfiguration;
    this.opts = opts;

    /**
     * If we do not have the required information to connect to the
     * message broker, throw a hard error.
     */
    if (!incomingConfiguration) {
      throw new Error('No incoming configuration was found!');
    }

    /**
     * Attempt to connect to the MQ broker.
     */
    mq.connect(incomingConfiguration)
      .then(async newConnection => {
        /**
         * If we have a successful connecton, store the
         * reference to it and continue to connectToTopic.
         */
        this.mqConnection = newConnection;
        await this.connectToTopic();
      })
      .catch(e => {
        /**
         * If there was an issue, emit an error.
         */
        this.emit('error', e.message);
      });
  }

  /**
   * @name connectToTopic
   * @description Attempt to connect to the topic previously
   * specified. If no topic is found, or is null, a random
   * topic ID is generated.
   */
  async connectToTopic() {
    /**
     * Await a connection instance and proceed to create a
     * "channel" to the broker. It's important to note that
     * a "channel" is an open, long-running TCP connection to
     * the message broker.
     */
    this.connection = await this.mqConnection;
    this.channel = await this.connection.createChannel();

    this.connection.on('error', connectionError => {
      this.emit('error', connectionError);
    });

    /**
     * Now that we have a channel to the broker, ensure we're
     * listening to any events that may be emitted by the
     * channel.
     */
    this.channel.on('error', channelError => {
      this.emit('error', channelError);
    });

    /**
     * What sort of exchange do we need / want?
     */
    await this.channel.assertExchange(
      this.configuration.exchange.name,
      this.configuration.exchange.type,
    );

    /**
     * Check queue (topic) has no issues against it. If does
     * not exist, it will be created. If it exists, it will
     * return metadata regarding the queue.
     */
    const result = await this.channel.assertQueue(this.topic);

    /**
     * Once we have our exchange and our queue - we need to
     * bind the queue to the exchange. This ensures that the
     * exchange knows to deliver the message to the queue.
     */
    await this.channel.bindQueue(
      this.topic,
      this.configuration.exchange.name,
      this.topic,
    );

    /**
     * Emit the result of the previous function to the "system"
     * event stream.
     */
    this.emit('system', result);

    /**
     * Attempt to consume whatever messages may be in the,
     * if any.
     */
    if (this.opts.consume === true) {
      this.channel.consume(this.topic, msg => {
        let jsonMessage = null;
        /**
         * Construct a new object from a cloned "msg"
         * object with a new key containing a decoded
         * message buffer for convenience.
         */

        try {
          /**
           * Attempt to JSON parse the message.
           */
          jsonMessage = JSON.parse(msg.content.toString());

          /**
           * Construct a new object with a new "message"
           * property for concenience.
           */
          const outgoingMessage = Object.assign(
            {
              message: jsonMessage,
            },
            msg,
          );

          /**
           * Emit this message to the "message" event
           * stream.
           */
          this.emit('message', outgoingMessage);
        } catch (e) {
          const error = new SyntaxError(e.message);
          this.emit('error', error);
        } finally {
          /**
           * Acknowledge the message by letting the message
           * broker know that the message was consumed. The
           * message will then be removed from the message
           * queue of the broker.
           */
          if (this.opts.acknowledge === true) {
            this.channel.ack(msg);
          }
        }
      });
    }
  }

  /**
   * @name pushToTopic
   * @description Push a message to the topic on the
   * message broker.
   *
   * @param {*} message - the message to publish.
   */
  async pushToTopic(message) {
    /**
     * It's possible that a user may attempt to call this
     * method before a connection has been established. If
     * this occurs, emit an error and return. The user can
     * try again.
     */
    if (!this.channel) {
      this.emit(
        'error',
        'No open communication channel found. Was the connection to the message queue server successful?',
      );

      /**
       * Return false - allow the user to test for this and
       * try again / perform another action.
       */
      return false;
    }

    /**
     * Send to the queue!
     */
    const result = await this.channel.publish(
      this.configuration.exchange.name,
      this.topic,
      Buffer.from(message),
    );

    /**
     * Emit a message to the system channel with the result
     * from the queue and the message sent for reference.
     */
    const messageToEmit = {
      pushResult: result,
      message,
    };

    this.emit('system', messageToEmit);

    /**
     * Return true to indicate that the option has finished.
     */
    return true;
  }
}

/**
 * Export.
 */
module.exports = AMPQ;
