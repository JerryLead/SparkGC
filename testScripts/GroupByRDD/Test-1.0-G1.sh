#!/bin/bash

appName=rGroupBy
#table=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits/{UserVisits.dat0,UserVisits.dat1,UserVisits.dat2,UserVisits.dat3}
table=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits
output=/usr/lijie/output/sql/GroupByRDD
logDir=logs/GroupByRDD-1.0

s=6   # Running times: [start, end]
e=10
p=1.0 # The percentage of data used for testing

Parallel=0
CMS=0
G1=1

if [ $Parallel -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-Parallel-4-28G-$p-n$i.log
     ./$appName-Parallel.sh 4 28G $p $table $output $logFile
   done
fi

if [ $Parallel -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-Parallel-2-14G-$p-n$i.log
     ./$appName-Parallel.sh 2 14G $p $table $output $logFile
   done
fi

if [ $Parallel -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
   sleep 5
   logFile=$logDir/$appName-Parallel-1-7G-$p-n$i.log
   ./$appName-Parallel.sh 1 7G $p $table $output $logFile
   done
fi

if [ $CMS -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-4-28G-$p-n$i.log
     ./$appName-CMS.sh 4 28G $p $table $output $logFile
   done
fi

if [ $CMS -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-2-14G-$p-n$i.log
     ./$appName-CMS.sh 2 14G $p $table $output $logFile
   done
fi

if [ $CMS -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-1-7G-$p-n$i.log
     ./$appName-CMS.sh 1 7G $p $table $output $logFile
   done
fi

if [ $G1 -ge 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-G1-2-14G-$p-n$i.log
     ./$appName-G1.sh 2 14G $p $table $output $logFile
   done
fi


