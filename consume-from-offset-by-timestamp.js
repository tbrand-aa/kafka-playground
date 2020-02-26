'use strict'

var Kafka = require('node-rdkafka')
var TopicPartition = require('node-rdkafka/lib/topic-partition')

const kafkaBrokers = 'localhost:9092'
const KAFKA_TOPIC = 'test-offset'
const REQUESTED_TIMESTAMP = (new Date().getTime()) - (24 * 3600000) // 24h ago
const REQUEST_OFFSET_TIMEOUT = 30000; // ms

let topics;

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'shg-kafka-getoffset',
  'metadata.broker.list': kafkaBrokers,
  'enable.auto.commit': false,
  'offset_commit_cb': function(err, topicPartitions) {
    if (err) {
      console.error('OFFSET_COMMIT_CB:: ERROR:', err);
    } else {
      console.log('OFFSET_COMMIT_CB:: topicPartitions:', topicPartitions);
    }
  },
},{
  'auto.offset.reset': 'earliest',
})

// Flowing mode
consumer.connect();

consumer
  .on('ready', async (i, metadata) => {
    // Finding metadata for requested topic
    const topicMetadata = metadata.topics.find(t => t.name === KAFKA_TOPIC)
    if (!topicMetadata) {
      console.error(`Required topic ${KAFKA_TOPIC} not found!`)
      return
    }
    console.log('Topic partitions:', topicMetadata.partitions.length, "\n", topicMetadata.partitions)

    // Building data for requesting partition offset for timestamp
    const topPars = topicMetadata.partitions.map(t => {
      return new TopicPartition(KAFKA_TOPIC, t.id, REQUESTED_TIMESTAMP)
    })
    
    // Requesting topic partition offsets for timestamp
    consumer.offsetsForTimes(topPars, REQUEST_OFFSET_TIMEOUT, async (err, tp) => {
      if (err) {
        console.error((new Date()), 'offsetForTimes error: ', err)
        return
      }
      console.log('offsetForTimes: toppars:', tp)

      // Building topics/partitions array for assignment and keeping track of a finished state
      topics = tp.reduce((acc, cur) => {
        if (cur.offset > -1) {
          acc.push({
            ...cur,
            highOffset: -1,
            finished: false
          })
        }
        
        return acc
      }, [])

      
      // Requesting current highOffset for all assigned topic partitions
      // we use this highOffset to check if we have processed all messages untill the moment the service stated, 
      // otherwise the consumer keeps consuming.
      await topics.reduce(async (acc, cur) => {
        await acc

        await new Promise((resolve, reject) => {
          consumer.queryWatermarkOffsets(KAFKA_TOPIC, cur.partition, 3000, (err, offsets) => {
            console.log('watermarkOffsets for partition:', cur.partition, offsets)
            
            const topic = topics.find(t => t.partition === cur.partition)
            topic.highOffset = offsets.highOffset
            
            resolve()
          })
        })

        return
      }, Promise.resolve())

      // Assigning topics to consumer to consume
      console.log('topics:', topics)
      consumer.assign(tp)

      console.log('Starting consumer...')
      consumer.consume()
    })
  })
  .on('data', function(data) {
    // Output the actual message contents
    console.log(Date.now(), data.topic, data.partition, data.offset, `${data.value.toString()}`);

    // Update the current finished state of the topic which we received a message for
    const topic = topics.find(t => t.partition === data.partition)
    topic.finished = (topic.highOffset-1 === data.offset)

    if (topics.filter(t => !t.finished).length === 0) {
      // All topics are finished, close connection
      console.log("All assigned topics are consumed. Disconnecting...")
      consumer.disconnect()
    }
  })
  .on('disconnected', () => {
    console.log('Consumer disconnected.')
  })
