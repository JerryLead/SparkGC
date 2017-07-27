#/bin/bash

table_hdfs_file=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits/UserVisits.dat0
output_hdfs_dir=/usr/lijie/output/sql/RDDGroupByTest


$HADOOP_HOME/bin/hdfs dfs -rm -r $output_hdfs_dir
logFile=logs/sample.log

#/root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
#sleep 5
# --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -javaagent:/root/apps/gcProfiler/aprof.jar" \
/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "RDDGroupBy-test-1-7G" \
                                             --class applications.sql.rdd.RDDGroupByTest \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                              --executor-cores 1 \
                                              --executor-memory 7G \
                                              --conf spark.default.parallelism=32 \
                                              --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime" \
                                             /root/lijie/selectedApps/SparkGC.jar \
$table_hdfs_file $output_hdfs_dir 2>&1 | tee $logFile 
