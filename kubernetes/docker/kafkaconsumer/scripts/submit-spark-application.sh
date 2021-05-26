#!/usr/bin/env bash

export CLASS_NAME="${CLASS_NAME}"
export APPLICATION_JAR="${APP_JAR}"

#SPARK_IMAGE=${DOCKER_IMAGE:-"jpanko/ssds_dev:0.0.1"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"4G"}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-"4"}
SPARK_EXECUTORS_COUNT=${SPARK_EXECUTORS_COUNT:-"2"}

export SPARK_JARS_DIR="${SPARK_HOME}/jars"

my_host_ip=`echo $(hostname -i)`
my_host_name=`echo $(hostname)`


echo SUBMIT APP WITH $@

exec "${SPARK_HOME}"/bin/spark-submit \
	--class ${CLASS_NAME} \
	--master  k8s://https://kubernetes.default.svc.cluster.local:443 \
    --deploy-mode client \
    --name ${APP_NAME} \
    --packages ${PACKAGES_LIST} \
    --driver-memory ${DRIVER_MEMORY} \
    --conf spark.driver.host=${my_host_ip} \
    --conf spark.kubernetes.driver.pod.name=${my_host_name} \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
	--conf spark.kubernetes.authenticate.oauthTokenFile=/var/run/secrets/kubernetes.io/serviceaccount/token \
    --conf spark.kubernetes.authenticate.caCertFile=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
	--conf spark.kubernetes.container.image=${DOCKER_IMAGE} \
	--conf spark.kubernetes.namespace=${NAMESPACE} \
	--conf spark.executor.instances=${SPARK_EXECUTORS_COUNT} \
	--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY}  \
    --conf spark.executor.cores=${SPARK_EXECUTOR_CORES} \
    local://${APPLICATION_JAR} \
  "$@"



