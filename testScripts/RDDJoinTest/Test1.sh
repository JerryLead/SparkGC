#!/bin/bash

table1=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/rankings
table2=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits
output=/usr/lijie/output/sql/RDDJoinTest
logDir=/root/lijie/logs/RDDJoinTest

s=3
e=4

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-Parallel-4-28-n$i.log
  ./rjoin-Parallel-4-28.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-CMS-4-28-n$i.log
  ./rjoin-CMS-4-28.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-G1-4-28-n$i.log
  ./rjoin-G1-4-28.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-Parallel-2-14-n$i.log
  ./rjoin-Parallel-2-14.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-CMS-2-14-n$i.log
  ./rjoin-CMS-2-14.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-G1-2-14-n$i.log
  ./rjoin-G1-2-14.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-Parallel-1-7-n$i.log
  ./rjoin-Parallel-1-7.sh $table1 $table2 $output tee $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-CMS-1-7-n$i.log
  ./rjoin-CMS-1-7.sh $table1 $table2 $output $logFile
done

for ((i=$s; i<=$e; i++))
do
  $HADOOP_HOME/bin/hdfs dfs -rm -r $output
  /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
  ./doCommand.sh "sync"
  ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
  logFile=$logDir/rjoin-G1-1-7-n$i.log
  ./rjoin-G1-1-7.sh $table1 $table2 $output $logFile
done
