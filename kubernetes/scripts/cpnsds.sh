ls -l docker/kafka*/jars

echo
echo COPY
echo

NSDS=~/prj/3.1.0.2002/spliceengine/splice_spark2/target/splice_spark2-3.1.0.2002-dbaas3.0.jar 

cp -p $NSDS docker/kafkaconsumer/jars

cp -p $NSDS docker/kafkaspark/jars

ls -l docker/kafka*/jars
