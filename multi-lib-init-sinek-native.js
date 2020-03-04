'use strict'

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

const run_1 = () => new Promise(async (resolve, reject) => {
  const { 
    NConsumer, 
    NProducer,
  } = require("sinek")

  const config = {
    noptions: {
        "metadata.broker.list": "localhost:9092",
        "group.id": "example-group",
        "enable.auto.commit": false,
        "socket.keepalive.enable": true,
        "api.version.request": true,
        "socket.blocking.max.ms": 100,
        async offset_commit_cb (err, partitions1) {
          console.log('RUN_1::', 'OFFSET_COMMIT_CB', partitions1)
          if (err) {
            console.error('RUN_1::', 'OFFSET COMMIT ERROR', err)
            reject()
          }
          else {
            await consumer.pause()
            await consumer.close()
            await producer.close()
            resolve()
          }
        }
    },
    tconf: {
        "auto.offset.reset": "earliest",
    },
  }

  const consumer = new NConsumer(['TEST_IN'], config)
  consumer.on("error", (error) => console.error(error));

  const producer = new NProducer(config)
  
  await consumer.connect();
  consumer.consume(async (messages, callback) => {
    console.log('RUN_1::', 'message received:', messages.topic, messages.offset, messages.value)  
    await sleep(1000)

    console.log('RUN_1::', 'Producing message to TEST_OUT')
    producer.send('TEST_OUT', messages.value)

    console.log('RUN_1::', 'message handled, sending callback')
    callback();
  }, true, false); // batchOptions are only supported with NConsumer

  await producer.connect()
  console.log('RUN_1::', 'Producing message to TEST_IN')
  await producer.send('TEST_IN', 'test-message-test-1')
})

const run_2 = () => new Promise(async (resolve, reject) => {
  const { 
    NConsumer, 
    NProducer,
  } = require("sinek")

  const config = {
    noptions: {
        "metadata.broker.list": "localhost:9092",
        "group.id": "example-group",
        "enable.auto.commit": false,
        "socket.keepalive.enable": true,
        "api.version.request": true,
        "socket.blocking.max.ms": 100,
        async offset_commit_cb (err, partitions1) {
          console.log('RUN_2::', 'OFFSET_COMMIT_CB', partitions1)
          if (err) {
            console.error('RUN_2::', 'OFFSET COMMIT ERROR', err)
            reject()
          }
          else {
            // consumer.pause()
            // consumer.close(true)
            // producer.close()
            resolve()
          }
        }
    },
    tconf: {
        "auto.offset.reset": "earliest",
    },
  }

  const consumer = new NConsumer(['TEST_IN'], config)
  consumer.on("error", (error) => console.error(error));

  const producer = new NProducer(config)
  
  await consumer.connect();
  consumer.consume(async (messages, callback) => {
    console.log('RUN_2::', 'message received:', messages.topic, messages.offset, messages.value)  
    await sleep(1000)

    console.log('RUN_2::', 'Producing message to TEST_OUT')
    producer.send('TEST_OUT', messages.value)

    console.log('RUN_2::', 'message handled, sending callback')
    callback();
  }, true, false); // batchOptions are only supported with NConsumer

  await producer.connect()
  console.log('RUN_2::', 'Producing message to TEST_IN')
  await producer.send('TEST_IN', 'test-message-test-1')
})


;(async () => {
  try {
    console.log('Starting multi-func-test')

    console.log('>> Task starting: run_1')
    await run_1()
    console.log('>> Task finished: run_1')

    console.log('>> Task starting: run_2')
    await run_2()
    console.log('>> Task finished: run_2')

  } catch (e) {
    console.log(e)
  }
})()