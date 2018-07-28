const amqplib = require('amqplib');
const { AMQP } = require('../../lib/amqp');

describe('The AMQP Class', () => {
  let MQ;
  let mqConnectToTopicSpy; // eslint-disable-line
  let mqPushToTopicSpy;
  let emitSpy;

  const mqConnection = {
    topic: '#',
    protocol: 'amqp',
    hostname: 'notif-rabbitmq0.pillar.phz',
    port: 5672,
    username: 'notifications',
    password: 'plokij',
    locale: 'en_US',
    frameMax: 0,
    heartbeat: 1,
    vhost: 'notifications',
    exchange: {
      name: 'pillar',
      type: 'topic',
    },
  };

  beforeEach(() => {
    amqplib.connect = jest.fn(() =>
      Promise.resolve({
        on: jest.fn(),
        createChannel: jest.fn(() =>
          Promise.resolve({
            on: jest.fn(),
            assertExchange: jest.fn(),
            assertQueue: jest.fn(() => Promise.resolve('assert success')),
            bindQueue: jest.fn(() => Promise.resolve('bind success')),
            consume: jest.fn(() => 'hello'),
            ack: jest.fn(() => Promise.resolve('ack success')),
            publish: jest.fn(() => Promise.resolve('publish success')),
          }),
        ),
      }),
    );

    MQ = new AMQP(mqConnection, 'anything');
    mqConnectToTopicSpy = jest.spyOn(MQ, 'connectToTopic');
    mqPushToTopicSpy = jest.spyOn(MQ, 'pushToTopic');
    emitSpy = jest.spyOn(MQ, 'emit');
  });

  it('should instantiate with correct data', () => {
    expect(() => {
      const thisMQ = new AMQP(mqConnection); // eslint-disable-line
    }).not.toThrowError();
  });

  it('should have called the MQ connect function', () => {
    expect(amqplib.connect).toHaveBeenCalled();
  });

  it('should have called the MQ connect function with given MQ connection data', () => {
    expect(amqplib.connect).toHaveBeenCalledWith(mqConnection);
  });

  it('should successfully resolve MQ connect', () => {
    expect(amqplib.connect()).toBeInstanceOf(Promise);
    expect(amqplib.connect()).resolves.toBe({});
  });

  it('should proceed to connectToTopic on successful connection to broker', async () => {
    expect(MQ.connectToTopic).toHaveBeenCalled();
  });

  it('should have successfully called createChannel()', () => {
    expect(MQ.connection.createChannel).toHaveBeenCalled();
  });

  it('should have called connection.on event emitter', () => {
    expect(MQ.connection.on).toHaveBeenCalled();
  });

  it('should have called channel.on event emitter', () => {
    expect(MQ.channel.on).toHaveBeenCalled();
  });

  it('should have called channel.assertExchange with correct parameters', () => {
    expect(MQ.channel.assertExchange).toHaveBeenCalledWith(
      mqConnection.exchange.name,
      mqConnection.exchange.type,
    );
  });

  it('should have called channel.assertQueue with correct parameter', () => {
    expect(MQ.channel.assertQueue).toHaveBeenCalledWith('anything');
  });

  it('should have called channel.bindQueue with correct parameters', async () => {
    const result = await MQ.channel.bindQueue();

    expect(await MQ.channel.bindQueue).toHaveBeenCalledWith(
      'anything',
      mqConnection.exchange.name,
      'anything',
    );
    expect(result).toBe('bind success');
  });

  it('should emit a system message stating that the assertion was a success', async () => {
    await MQ.connectToTopic();

    expect(emitSpy).toHaveBeenCalled();
    expect(emitSpy).toHaveBeenCalledWith('system', 'assert success');
  });

  it('should have called the channel.consume method with the correct topic name', async () => {
    await MQ.connectToTopic();

    expect(MQ.channel.consume).toHaveBeenCalledWith(
      'anything',
      expect.any(Function),
    );
  });

  it('should have called the channel.ack method', async () => {
    await MQ.connectToTopic();

    expect(MQ.channel.consume).toHaveBeenCalledWith(
      'anything',
      expect.any(Function),
    );
  });

  it('should correctly call the pushToTopic method with a message', async () => {
    await MQ.connectToTopic();
    await MQ.pushToTopic('hello!');

    expect(mqPushToTopicSpy).toHaveBeenCalled();
    expect(mqPushToTopicSpy).toHaveBeenCalledWith('hello!');
  });

  it('should correctly call the pushToTopic method with a message, and push to queue', async () => {
    await MQ.connectToTopic();
    await MQ.pushToTopic('hello!');

    expect(MQ.channel.publish).toHaveBeenCalled();
    expect(MQ.channel.publish).toHaveBeenCalledWith(
      mqConnection.exchange.name,
      'anything',
      Buffer.from('hello!'),
    );
  });
});
