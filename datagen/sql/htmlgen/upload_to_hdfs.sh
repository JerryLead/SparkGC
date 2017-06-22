#!/usr/bin/env bash

# Make sure we have all the arguments
if [ $# -ne 3 ]; then
   printf "Usage: $0 <slaves_file> <path to the data dir> <path in hdfs to save dir>\n"
   exit -1
fi

# Get the input data
declare SLAVES_FILE=$1;
declare LOCAL_DIR=$2
declare HDFS_PATH=$3

${HADOOP_HOME}/bin/hadoop dfs -mkdir ${HDFS_PATH}
${HADOOP_HOME}/bin/hadoop dfs -mkdir ${HDFS_PATH}/rankings
${HADOOP_HOME}/bin/hadoop dfs -mkdir ${HDFS_PATH}/uservisits

# Error checking
if test ! -e $SLAVES_FILE; then
   printf "ERROR: The file '$SLAVES_FILE' does not exist. Exiting\n"
   exit -1
fi

for slave in `cat "$SLAVES_FILE"`; do
{
(
   printf "Running on host: $slave\n"
   ssh $slave "${HADOOP_HOME}/bin/hadoop dfs -put ${LOCAL_DIR}/Rank* ${HDFS_PATH}/rankings/"
   ssh $slave "${HADOOP_HOME}/bin/hadoop dfs -put ${LOCAL_DIR}/User* ${HDFS_PATH}/uservisits/"
#   ssh $slave "rm -rf ${LOCAL_DIR}"
)&
}
done
wait
# Done
printf "\nData upload completed at: "
date

