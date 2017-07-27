#/bin/bash

table1_hdfs_file=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/rankings/{Rankings.dat0,Rankings.dat1,Rankings.dat2,Rankings.dat3}
table2_hdfs_file=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits/UserVisits.dat0
output_hdfs_dir=/usr/lijie/output/sql/RDDJoinTest


$HADOOP_HOME/bin/hdfs dfs -rm -r $output_hdfs_dir
logFile=/root/lijie/logs/RDDJoinTest/sample.log

#/root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
#sleep 5
/root/spark/spark-2.1.4.19-bin-2.7.1/bin/spark-submit --name "RDDJoin-test-1-7G" \
                                             --class applications.sql.rdd.RDDJoinTest \
                                             --deploy-mode client \
                                             --total-executor-cores 32 \
                                              --executor-cores 1 \
                                              --executor-memory 7G \
                                              --conf spark.default.parallelism=32 \
                                              --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy" \
                                             /root/lijie/selectedApps/SparkGC.jar \
$table1_hdfs_file $table2_hdfs_file $output_hdfs_dir 2>&1 | tee $logFile 
