#!/usr/bin/env sh

java -cp /opt/app/jars/splice-adapter-kafka-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
    com.splicemachine.sample.KafkaTopicProducer \
    $@

# Args: kafkaBroker topicName numRecords numThreads
