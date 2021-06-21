#!/usr/bin/env bash


#./submit-spark-application.sh "$JOB_NAME" "$EXT_KAFKA_BROKER" "$EXT_TOPIC"  "$JDBC_URL"  "$TABLE_NAME" "$SPLICE_KAFKA_BROKER"

./submit-spark-application.sh $APP_ARGS

if [[ "${DEBUG_MODE}" == "true" ]]; then
   i=1
   while [ "$i" -ne 0 ]
   do
     echo "waiting......."
     sleep 10
   done

fi