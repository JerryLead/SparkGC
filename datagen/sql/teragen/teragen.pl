#!/usr/bin/env perl
#/***************************************************************************
# *   Copyright (C) 2008 by Andy Pavlo, Brown University                    *
# *   http://www.cs.brown.edu/~pavlo/                                       *
# *                                                                         *
# *   Permission is hereby granted, free of charge, to any person obtaining *
# *   a copy of this software and associated documentation files (the       *
# *   "Software"), to deal in the Software without restriction, including   *
# *   without limitation the rights to use, copy, modify, merge, publish,   *
# *   distribute, sublicense, and/or sell copies of the Software, and to    *
# *   permit persons to whom the Software is furnished to do so, subject to *
# *   the following conditions:                                             *
# *                                                                         *
# *   The above copyright notice and this permission notice shall be        *
# *   included in all copies or substantial portions of the Software.       *
# *                                                                         *
# *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
# *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
# *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
# *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
# *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
# *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
# *   OTHER DEALINGS IN THE SOFTWARE.                                       *
# ***************************************************************************/
use strict;
use warnings;

# MODIFY THESE PARAMETERS
my $NUM_OF_RECORDS = 500000; #   1GB ~= 10000000;
my $NUM_OF_MAPS = 10; # Degree of parallelism
my $BASE_OUTPUT_DIR   = "/data"; # The queries have the '/data' HDFS dir hardcoded

my $PATTERN_STRING    = "XYZ";
my $PATTERN_FREQUENCY = 108299;
my $TERAGEN_JAR       = "teragen.jar";
my $HADOOP_COMMAND    = $ENV{'HADOOP_HOME'}."/bin/hadoop";

# Preparation
my $output_dir = $BASE_OUTPUT_DIR."/grep";
system("$HADOOP_COMMAND fs -rmr $output_dir");
print "Generating $NUM_OF_MAPS files in '$output_dir'\n";
   
##
## EXEC: hadoop jar teragen.jar 10000000000 /data/SortGrep/ XYZ 108299 100
##
my @args = ( $NUM_OF_RECORDS,
             $output_dir,
             $PATTERN_STRING,
             $PATTERN_FREQUENCY,
             $NUM_OF_MAPS );
my $cmd = "$HADOOP_COMMAND jar $TERAGEN_JAR ".join(" ", @args);
print "$cmd\n";
system($cmd) == 0 || die("ERROR: $!");

exit(0);
