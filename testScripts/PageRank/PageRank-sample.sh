#!/usr/bin/env bash

input_file=/usr/yxt/0.5twitter.txt
output_file=/usr/lijie/output/graph/PageRank

logFile=logs/sample.log

iter=10

#/root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
#sleep 5
# --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -javaagent:/root/apps/gcProfiler/aprof.jar" \
/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "PageRank-test-1-7G" \
                                             --class applications.graph.PageRank \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                             --executor-cores 1 \
                                             --executor-memory 7G \
                                             --driver-memory 16g \
                                             --conf spark.driver.maxResultSize=8g \
                                             --conf spark.default.parallelism=32 \
                                             --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime" \
                                             /root/lijie/selectedApps/SparkGC.jar \
$input_file $iter $output_file 2>&1 | tee $logFile
