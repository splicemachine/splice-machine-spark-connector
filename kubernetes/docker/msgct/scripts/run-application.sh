#!/usr/bin/env sh

./submit-application.sh $APP_ARGS

if [[ "${DEBUG_MODE}" == "true" ]]; then
   i=1
   while [ "$i" -ne 0 ]
   do
     echo "waiting......."
     sleep 10
   done

fi
