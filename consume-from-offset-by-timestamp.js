'use strict'

var Kafka = require('node-rdkafka')
var TopicPartition = require('node-rdkafka/lib/topic-partition')

const kafkaBrokers = 'localhost:9092'
const KAFKA_TOPIC = 'test-getoffset'
const REQUESTED_TIMESTAMP = (new Date().getTime()) - (24 * 3600000) // 24h ago


var consumer = new Kafka.KafkaConsumer({
  'group.id': 'shg-kafka-getoffset-5',
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
  .on('ready', function(i, metadata) {
    const topicMetadata = metadata.topics.find(t => t.name === KAFKA_TOPIC)
    if (!topicMetadata) {
      console.error(`Required topic ${KAFKA_TOPIC} not found!`)
      return
    }

    console.log('Topic partitions:', topicMetadata.partitions.length)

    const topPars = topicMetadata.partitions.map(t => {
      return new TopicPartition(KAFKA_TOPIC, t.id, REQUESTED_TIMESTAMP)
    })
    console.log('Requesting TopicPartitions...')
    consumer.offsetsForTimes(topPars, 30000, (err, tp) => {
      if (err) {
        console.error('offsetForTimes error: ', err)
        return
      }
    
      console.log('offsetForTimes: toppars:', tp)

      consumer.assign(tp)

      console.log('Starting consumer...')
      consumer.consume()
    })
  })
  .on('data', function(data) {
    // Output the actual message contents
    console.log(data.value.toString());
  });
