ls -l docker/kafka*/jars

echo
echo COPY
echo

SSDS=~/prj/ssds/splice-machine-spark-connector/spark3.0/target/scala-2.12/splice-machine-spark-connector_2.12-3.1.0.2002.jar 

cp -p $SSDS docker/kafkaconsumer/jars

cp -p $SSDS docker/kafkaspark/jars

ls -l docker/kafka*/jars
