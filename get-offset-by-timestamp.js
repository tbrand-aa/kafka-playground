'use strict'

var kafka = require('node-rdkafka')
var TopicPartition = require('node-rdkafka/lib/topic-partition')

const kafkaBrokers = 'localhost:9092'
const KAFKA_TOPIC = 'test-getoffset'
const REQUESTED_TIMESTAMP = 1582554080634


var consumer = new kafka.KafkaConsumer({
  'group.id': 'shg-kafka-getoffset',
  'metadata.broker.list': kafkaBrokers,
  'offset_commit_cb': function(err, topicPartitions) {
    if (err) {
      console.error('OFFSET_COMMIT_CB:: ERROR:', err);
    } else {
      console.log('OFFSET_COMMIT_CB:: topicPartitions:', topicPartitions);
    }
  }
})

// Flowing mode
consumer.connect();

consumer
  .on('ready', function(i, metadata) {
    const topicMetadata = metadata.topics.find(t => t.name === KAFKA_TOPIC)
    if (!topicMetadata) {
      console.error(`Requested topic ${KAFKA_TOPIC} not found!`)
      return
    }

    console.log('Topic partitions:', topicMetadata.partitions)

    const topPars = topicMetadata.partitions.map(t => {
      return new TopicPartition(KAFKA_TOPIC, t.id, REQUESTED_TIMESTAMP)
    })
    console.log('Requesting TopicPartitions:', topPars)
    consumer.offsetsForTimes(topPars, 30000, (err, tp) => {
      if (err) {
        console.error('offsetForTimes error: ', err)
        return
      }
    
      console.log('offsetForTimes: toppars:', tp)
    })
  })
  .on('data', function(data) {
    // Output the actual message contents
    console.log(data.value.toString());
  });
