#!/usr/bin/env bash

# Make sure we have all the arguments
if [ $# -ne 1 ]; then
   printf "Usage: $0 <slaves_file>\n"
   exit -1
fi

# Get the input data
declare SLAVES_FILE=$1;

# Error checking
if test ! -e $SLAVES_FILE; then
   printf "ERROR: The file '$SLAVES_FILE' does not exist. Exiting\n"
   exit -1
fi

for slave in `cat "$SLAVES_FILE"`; do
{
   printf "Installing on host: $slave\n"
   ssh $slave "yum -y install openssl openssl-devel words"
}
done

# Done
printf "\nInstallation completed at: "
date

