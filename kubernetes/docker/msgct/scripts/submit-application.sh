#!/usr/bin/env sh

jars=/opt/app/jars

cp=$jars/splice-machine-spark-connector_2.11-0.3.0-SNAPSHOT.jar:$jars/kafka-clients-2.2.1-cdh6.3.0.jar:$jars/scala-library-2.11.12.jar:$jars/slf4j-api-1.7.15.jar:$jars/db-client-3.1.0.1965-SNAPSHOT.jar:$jars/scala-java8-compat_2.11-0.7.0.jar

java -cp $cp \
    MessageCounter \
    $@ \
    > /var/log/app/out.log

# Args: kafkaBroker topicName
