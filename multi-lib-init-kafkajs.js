'use strict'

const run_1 = () => new Promise(async (resolve, reject) => {
  const { Kafka } = require('kafkajs')
  
  const kafka = new Kafka({
    clientId: 'kafkajs-test',
    brokers: ['localhost:9092']
  })
  
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: 'test-group' })
  
  const { 
    COMMIT_OFFSETS, 
    DISCONNECT,
  } = consumer.events
  const commitOffsetListenerRemove = consumer.on(COMMIT_OFFSETS, (e) => {
    console.log('RUN_1::', 'COMMIT_OFFSETS', e)
    consumer.disconnect()
    producer.disconnect()
  })

  const disconnectListenerRemove = consumer.on(DISCONNECT, (e) => {
    console.log('RUN_1::', 'Consumer Disconnected', e)
    resolve()
  })
  
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'TEST_IN', fromBeginning: true })

  await producer.connect()
  await producer.send({
    topic: 'TEST_IN',
    messages: [
      { value: 'test-1-message-1' },
    ],
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('RUN_1:: new message recieved:', topic, message.offset, message.value.toString())

      console.log('RUN_1:: Sending message to TEST_OUT topic')
      producer.send({
        topic: 'TEST_OUT',
        messages: [ message ]
      })
      console.log('RUN_1:: Received message handle done.')
    }
  })
})

const run_2 = () => new Promise(async (resolve, reject) => {
  const { Kafka } = require('kafkajs')
  
  const kafka = new Kafka({
    clientId: 'kafkajs-test',
    brokers: ['localhost:9092']
  })
  
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: 'test-group' })
  
  const { 
    COMMIT_OFFSETS, 
    DISCONNECT,
  } = consumer.events
  const commitOffsetListenerRemove = consumer.on(COMMIT_OFFSETS, (e) => {
    console.log('RUN_2::', 'COMMIT_OFFSETS', e)
    consumer.disconnect()
    producer.disconnect()
  })

  const disconnectListenerRemove = consumer.on(DISCONNECT, (e) => {
    console.log('RUN_2::', 'Consumer Disconnected', e)
    resolve()
  })
  
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'TEST_IN', fromBeginning: true })

  await producer.connect()
  await producer.send({
    topic: 'TEST_IN',
    messages: [
      { value: 'test-2-message-1' },
    ],
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('RUN_2:: new message recieved:', topic, message.offset, message.value.toString())

      console.log('RUN_2:: Sending message to TEST_OUT_2 topic')
      producer.send({
        topic: 'TEST_OUT_2',
        messages: [ {
          value: 'test-2-message-2'
        } ]
      })

      console.log('RUN_2:: Sending message to TEST_OUT topic')
      producer.send({
        topic: 'TEST_OUT',
        messages: [ message ]
      })
      console.log('RUN_2:: Received message handle done.')
    }
  })
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