#!/usr/bin/env bash

# Make sure we have all the arguments
if [ $# -ne 2 ]; then
   printf "Usage: $0 <slaves_file> <path to the data dir>\n"
   exit -1
fi

# Get the input data
declare SLAVES_FILE=$1;
declare LOCAL_DIR=$2

# Error checking
if test ! -e $SLAVES_FILE; then
   printf "ERROR: The file '$SLAVES_FILE' does not exist. Exiting\n"
   exit -1
fi

for slave in `cat "$SLAVES_FILE"`; do
{
(
   printf "Running on host: $slave\n"
   ssh $slave "rm -rf ${LOCAL_DIR}"
)&
}
done
wait
# Done
printf "\nData deletion completed at: "
date

