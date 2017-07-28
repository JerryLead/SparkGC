#!/usr/bin/env bash

input_file=/usr/lijie/data/ml/kdd12-0.5-dim

logFile=logs/sample.log

iter=10
#dimension=27343226
dimension=54686452

#/root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
#sleep 5
# --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -javaagent:/root/apps/gcProfiler/aprof.jar" \
/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "SVM-test-1-7G" \
                                             --class applications.ml.SVMWithSGDExample \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                             --executor-cores 1 \
                                             --executor-memory 7G \
                                             --driver-memory 16g \
                                             --conf spark.driver.maxResultSize=8g \
                                             --conf spark.default.parallelism=32 \
                                             --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime" \
                                             /root/lijie/selectedApps/SparkGC.jar \
$input_file $iter $dimension 2>&1 | tee $logFile
