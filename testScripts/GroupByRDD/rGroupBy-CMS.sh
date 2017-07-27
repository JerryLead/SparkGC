#!/bin/bash

if [ $# -ne 6 ]; then
  echo "Usage: ./rGroupBy-CMS.sh <corePerExecutor> <memPerExecutor> <data_percentage> <table_hdfs_file> <output_hdfs_dir> <logFile>"
  exit 1
fi

/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "GroupByRDD-CMS-$1-$2-$3" \
                                             --class applications.sql.rdd.RDDGroupByTest \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                             --executor-cores $1 \
                                             --executor-memory $2 \
                                             --conf spark.default.parallelism=32 \
                                             --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime" \
                                             /root/lijie/selectedApps/SparkGC.jar \
                                             $4 $5 2>&1 | tee $6
  
