'use strict'

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const run_1 = () => new Promise((resolve, reject) => {
  var Kafka = require('node-rdkafka')

  const globalConfig = {
    'group.id': 'noderdkafka-test',
    'metadata.broker.list': 'localhost:9092',
    offset_commit_cb: (err, topicPartitions1) => {
      if (err) {
        console.error('RUN_1::', 'OFFSET COMMIT ERROR', err)
        reject()
      }
      else {
        console.log('RUN_1::', 'OFFSET COMMIT, topicPartitions:', topicPartitions1)
        // resolve()
        consumer.disconnect()
        producerIN.disconnect()
        producerOUT.disconnect()
      }
    }
  }

  var consumer = Kafka.KafkaConsumer(globalConfig, {
    'auto.offset.reset': 'earliest',
  });
  var producerIN = new Kafka.Producer(globalConfig)
  var producerOUT = new Kafka.Producer(globalConfig)
  
  producerIN.on('ready', () => {
    console.log('RUN_1::', 'producerIN ready. Producing test message.')
    producerIN.produce('TEST_IN', null, Buffer.from('test-message-test-1'))
    // producerIN.poll()
  })
  producerIN.on('event.error', (err) => {
    console.error('RUN_1:: producerIN:: Error from producer')
    console.error(err)
    reject()
  })

  producerOUT.on('event.error', (err) => {
    console.error('RUN_1:: producerOUT:: Error from producer')
    console.error(err)
    reject()
  })
  producerOUT.connect()

  // Flowing mode
  consumer.connect()

  consumer
    .on('ready', function() {
      console.log('RUN_1::', 'Consumer ready. Subscribing to topic...')
      consumer.subscribe(['TEST_IN'])

      // Consume from the librdtesting-01 topic. This is what determines
      // the mode we are running in. By not specifying a callback (or specifying
      // only a callback) we get messages as soon as they are available.
      console.log('RUN_1::', 'Consumer start consuming')
      consumer.consume()

      console.log('RUN_1::', 'Connecting producerIN')
      producerIN.connect()
    })
    .on('data', async (data) => {
      // Output the actual message contents
      console.log('RUN_1::', 'onData(): Message incoming')
      const msg = data.value.toString()
      console.log('RUN_1::', 'onData(): Message:', msg)
      await sleep(1000)
      console.log('RUN_1::', 'onData(): Message sending to OUT')
      producerOUT.produce('TEST_OUT', null, Buffer.from(msg))
      console.log('RUN_1::', 'onData(): Message sending to OUT.. Done!')
    })
    .on('disconnected', () => {
      resolve()
    })
})

const run_2 = () => new Promise((resolve, reject) => {
  var Kafka = require('node-rdkafka')

  const globalConfig = {
    'group.id': 'noderdkafka-test',
    'metadata.broker.list': 'localhost:9092',
    offset_commit_cb: async (err, topicPartitions2) => {
      if (err) {
        console.error('RUN_2::', 'OFFSET COMMIT ERROR', err)
        reject()
      }
      else {
        console.log('RUN_2::', 'OFFSET COMMIT, topicPartitions:', topicPartitions2)

        await sleep(2000)
        consumer.disconnect()
        producerIN.disconnect()
        producerOUT1.disconnect()
        producerOUT2.disconnect()
      }
    }
  }

  var consumer = Kafka.KafkaConsumer(globalConfig, {
    'auto.offset.reset': 'earliest',
  });
  var producerIN = new Kafka.Producer(globalConfig)
  var producerOUT1 = new Kafka.Producer({
    'group.id': 'noderdkafka-test',
    'metadata.broker.list': 'localhost:9092'
  })
  var producerOUT2 = new Kafka.Producer({
    'group.id': 'noderdkafka-test',
    'metadata.broker.list': 'localhost:9092'
  })
  
  producerIN.on('ready', () => {
    console.log('RUN_2::', 'producerIN ready. Producing test message.')
    producerIN.produce('TEST_IN', null, Buffer.from('test-message-test-2'))
    // producerIN.poll()
  })
  producerIN.on('event.error', (err) => {
    console.error('RUN_2:: producerIN:: Error from producer')
    console.error(err)
    reject()
  })

  producerOUT1.on('event.error', (err) => {
    console.error('RUN_2:: producerOUT:: Error from producer')
    console.error(err)
    reject()
  })
  producerOUT1.connect()
  producerOUT2.connect()

  // Flowing mode
  consumer.connect()

  consumer
    .on('ready', function() {
      console.log('RUN_2::', 'Consumer ready. Subscribing to topic...')
      consumer.subscribe(['TEST_IN'])

      // Consume from the librdtesting-01 topic. This is what determines
      // the mode we are running in. By not specifying a callback (or specifying
      // only a callback) we get messages as soon as they are available.
      console.log('RUN_2::', 'Consumer start consuming')
      consumer.consume()

      console.log('RUN_2::', 'Connecting producerIN')
      producerIN.connect()
    })
    .on('data', async (data) => {
      // Output the actual message contents
      console.log('RUN_2::', 'onData(): Message incoming')
      const msg = data.value.toString()
      console.log('RUN_2::', 'onData(): Message:', msg)

      await sleep(1000)
      console.log('RUN_2::', 'onData(): Message sending to OUT2')
      producerOUT2.produce('TEST_OUT2', null, Buffer.from('new-test-message'))
      console.log('RUN_2::', 'onData(): Message sending to OUT2.. Done!')
      
      await sleep(1000)
      console.log('RUN_2::', 'onData(): Message sending to OUT1')
      producerOUT1.produce('TEST_OUT', null, Buffer.from(msg))
      console.log('RUN_2::', 'onData(): Message sending to OUT.. Done!')
    })
    .on('disconnected', () => {
      resolve()
    })
})

;(async () => {
  console.log('Starting multi-func-test-noderdkafka')
  try {
    console.log('>> Task starting: run_1')
    await run_1()
    console.log('>> Task finished: run_1')

    console.log('>> Task starting: run_2')
    await run_2()
    console.log('>> Task finished: run_2')

  } catch (e) {
    console.error(e)
  }
})()
