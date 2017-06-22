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

my $CUR_HOSTNAME = `hostname -s`;
chomp($CUR_HOSTNAME);

## Where we are going to grab the files from in HDFS
my $REMOTE_OUTPUT_DIR = $ARGV[0];

## Where we are going to put the files on the local machine
my $LOCAL_OUTPUT_DIR = $ARGV[1];

my $HADOOP_COMMAND    = $ENV{'HADOOP_HOME'}."/bin/hadoop";

##
## All of the input files are going to be named as 'part-#####'
## To make our life easier, we map the hostname number to the part number
##
$CUR_HOSTNAME =~ m/^d-([\d]+)\.*?/;
my $BASE_HOST_IDX = int($1);

##
## File Sizes
##
my %file_sizes   = ( "535MB" => 566597981,
                     "20GB"  => 20200000000,
                     "10GB"  => 10100000000,
                     "40GB"  => 40400000000 );
##
## Number of file parts (maps)
##
my %num_of_parts = ( "535MB" => 100,
                     "10GB"  => 100,
                     "20GB"  => 50,
                     "40GB"  => 25 );

##
## Now run through the different files we need to grab and put it on our local machine
##
system("mkdir -p $LOCAL_OUTPUT_DIR") unless (-d $LOCAL_OUTPUT_DIR);
for my $target (keys %num_of_parts) {
   my $host_idx = ($BASE_HOST_IDX - 1) % $num_of_parts{$target};
   my $remote_file = "$REMOTE_OUTPUT_DIR/SortGrep$target/".sprintf("part-%05d", $host_idx);
   my $local_file = "$LOCAL_OUTPUT_DIR/SortGrep$target";

   ##
   ## Grab the file on the remote filesystem and store it locally
   ##
   unless (-f $local_file) {
      my $cmd = "$HADOOP_COMMAND fs -copyToLocal $remote_file $local_file";
      print "$cmd\n";
      print `$cmd`;
   } else {
      print "File '$local_file' already exists! Skipping...\n";
   } # UNLESS
   system("chmod 0644 $local_file");
   
   ##
   ## Check to make sure it's the right size
   ##
   die("$CUR_HOSTNAME: Missing $local_file!!!\n") unless (-f $local_file);
   my $expected_size = $file_sizes{$target};
   my $actual_size   = (stat($local_file))[7];
   die("$CUR_HOSTNAME: $local_file SIZE :: $expected_size != $actual_size\n") if ($actual_size != $expected_size);
} # FOR
exit(0);
