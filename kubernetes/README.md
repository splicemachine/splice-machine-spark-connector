# Kafka Consumer

This helm chart provides an implementation of kafka consumer.

The way it works is, a jar file is created containing the Spark Application in this case it will the consumer of a Kafka queue.
This jar is included in a docker. This docker can run the specifed spark application via spark-submit.

Then a pod is created with docker image to the cluster to run a spark application

The deployment definition of the pod is defined in Helm Charts.

The main components in this implementation are
1. kafkaconsumer  Helm Chart : This Chart contains the definition for the Pod deployment along with configurations
2. splicemachine/demo_kafka_consumer docker : The docker image for Kafka Consumer. This will include the jar containing the spark application
3. splicemachine/demo_kafka_spark docker : Docker image for Spark executors

## Steps to deploy:
This assumes you have checkout out the fiels from git repo
1. Changes to splicemachine/demo_kafka_consumer docker

Currently splice-machine-spark-connector-assembly-0.3.0-SNAPSHOT.jar is the spark application included in the docker.
In case a different jar is needed or a different version of this jar is needed perform following steps
* Copy the new jar to docker/kafkaconsumer/jars forlder
* Change the Docker image in the Makefile (docker/kafkaconsumer/Makefile)
* Build docker and push the docker from the folder docker/kafkaconsumer run the following commands
    
    make
    
    make push

Note the jar will be copied to /opt/spark/jars in the docker image.

2. Changes to helm chart
The file file you will have to change is values.yaml. The following configurations in this file may need to be modified

* image/tag : This is corresponds to the version of splicemachine/demo_kafka_consumer  docker. So in case new version is built in above step
     set this to the new version
* appjar: In case a different jar needs to be used, change this value. Note the jar in the docker will be in /opt/spark/jars.
    the value should include complete path i.e. /opt/spark/jars/<app jar name>

* classname: The class name in the jar that needs to be run : include the complete package for ex : org.apache.spark.examples.SparkPi
* appargs: These are arguments the class needs. The argument values are space seperated. 
    Note: do not add quotes around the values, as the quutes will be included in the value and be invalid

    If jdbc url is required as argument the format is
    "jdbc:splice://splicedb-hregion.DB_CLUSTER_NAME.svc.cluster.local:1527/splicedb;user=DB_USER;password=DB_PASSWORD;"

    Substitute DB_CLUSTER_NAME, DB_USER and DB_PASSWORD with your values

    The kafak address will be in the format
    splicedb-kafka.DB_CLUSTER_NAME.svc.cluster.local:9092"

    Substitute DB_CLUSTER_NAME with your values

    Example value for app_args looks like this

    \"kafkareader\"  splicedb-kafka.test.svc.cluster.local:9092 weather jdbc:splice://splicedb-hregion.test.svc.cluster.local:1527/splicedb;user=uuuu;password=xxxx; weather splicedb-kafka.test.svc.cluster.local:9092

* appname: This is the spark application name used in the naming of executors.

There are other optional configurations

* spark_executor_memory: "4G"  - This is for the memeory of the spark executors
* spark_executor_cores: "5"  - This is for the number of cores of spark executors
* spark_executor_count: "2" - This is for the number of spark executors to create
* spark_image: splicemachine/demo_kafka_spark:0.0.1 - Spark docker the executor pods will use, This should not be change.


Since the pod deployed will run the spark application and exit, in case you need the pod to be running even after it finishs the job,
say for debug purpose, you may need to log into the pod, and run spark-submit scripts are check environment variables etc.
then set this configuration to true

* debug: true


3. Install Helmchart command
* From the main folder (i.e KAFKA_SSDS or folder that contains the chart folder), run this command, after replacing
DB_CLUSTER_NAME with your dattabase cluster name

helm install --name  splicesales --namespace DB_CLUSTER_NAME  charts/kafkaconsumer

(ex : helm install --name  splicesales --namespace test  charts/kafkaconsumer )


This command will launch the  pod splicesales-kafkaconsumer_xxxx, which will run the specified spark application.

4 Uninstall Chart

* From the main folder (i.e KAFKA_SSDS or folder that contains the chart folder), run this command

helm del --purge splicesales
