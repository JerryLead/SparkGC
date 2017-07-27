#!/bin/bash

appName=rjoin
table1=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/rankings/{Rankings.dat0,Rankings.dat1,Rankings.dat2,Rankings.dat3}
table2=/usr/lijie/data/sql/Rankings-1GB-UserVisits-2GB/uservisits/{UserVisits.dat0,UserVisits.dat1,UserVisits.dat2,UserVisits.dat3}
output=/usr/lijie/output/sql/RDDJoinTest
logDir=/root/lijie/testScripts/RDDJoinTest/logs/RDDJoinTest-0.5-2

s=1   # Running times: [start, end]
e=5
p=0.5 # The percentage of data used for testing

Parallel=1
CMS=1
G1=1

E1=1
E2=1
E4=1


if [ $Parallel -ge 1 ] && [ $E4 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-Parallel-4-28G-$p-n$i.log
     ./$appName-Parallel.sh 4 28G $p $table1 $table2 $output $logFile
   done
fi

if [ $CMS -ge 1 ] && [ $E4 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-4-28G-$p-n$i.log
     ./$appName-CMS.sh 4 28G $p $table1 $table2 $output $logFile
   done
fi

if [ $G1 -ge 1 ] && [ $E4 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-G1-4-28G-$p-n$i.log
     ./$appName-G1.sh 4 28G $p $table1 $table2 $output $logFile
   done
fi


if [ $Parallel -ge 1 ] && [ $E2 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-Parallel-2-14G-$p-n$i.log
     ./$appName-Parallel.sh 2 14G $p $table1 $table2 $output $logFile
   done
fi

if [ $CMS -ge 1 ] && [ $E2 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-2-14G-$p-n$i.log
     ./$appName-CMS.sh 2 14G $p $table1 $table2 $output $logFile
   done
fi

if [ $G1 -ge 1 ] && [ $E2 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 50
     logFile=$logDir/$appName-G1-2-14G-$p-n$i.log
     ./$appName-G1.sh 2 14G $p $table1 $table2 $output $logFile
   done
fi

if [ $Parallel -ge 1 ] && [ $E1 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-Parallel-1-7G-$p-n$i.log
     ./$appName-Parallel.sh 1 7G $p $table1 $table2 $output $logFile
   done
fi

if [ $CMS -ge 1 ] && [ $E1 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-CMS-1-7G-$p-n$i.log
     ./$appName-CMS.sh 1 7G $p $table1 $table2 $output $logFile
   done
fi

if [ $G1 -ge 1 ] && [ $E1 -eq 1 ]; then
   for ((i=$s; i<=$e; i++))
   do
     $HADOOP_HOME/bin/hdfs dfs -rm -r $output
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/stop-slaves.sh
     sleep 5
     /root/spark/spark-2.1.4.19-bin-2.7.1/sbin/start-slaves.sh
     ./doCommand.sh "sync"
     ./doCommand.sh "echo 3 > /proc/sys/vm/drop_caches"
     sleep 5
     logFile=$logDir/$appName-G1-1-7G-$p-n$i.log
     ./$appName-G1.sh 1 7G $p $table1 $table2 $output $logFile
   done
fi

