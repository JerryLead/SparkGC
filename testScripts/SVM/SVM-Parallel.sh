#!/bin/bash

if [ $# -ne 7 ]; then
  echo "Usage: ./SVM-Parallel.sh <corePerExecutor> <memPerExecutor> <data_percentage> <input_file> <iter> <dimension> <logFile>"
  exit 1
fi

/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "SVM-Parallel-$1-$2-$3" \
                                             --class applications.ml.SVMWithSGDExample \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                             --executor-cores $1 \
                                             --executor-memory $2 \
                                             --driver-memory 16g \
                                             --conf spark.driver.maxResultSize=8g \
                                             --conf spark.default.parallelism=32 \
                                             --conf spark.executor.extraJavaOptions="-XX:+UseParallelGC -XX:+UseParallelOldGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime" \
                                             /root/lijie/selectedApps/SparkGC.jar \
                                             $4 $5 $6 2>&1 | tee $7

