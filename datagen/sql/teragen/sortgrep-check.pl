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

my %files = ( "SortGrep535MB" => 566597981,
              "SortGrep20GB"  => 20200000000,
              "SortGrep10GB"  => 10100000000,
              "SortGrep40GB"  => 40400000000
            );
for my $file (keys %files) {
   my $file_path = "/scratch/pavlo/cacm_data/$file";
   die("$CUR_HOSTNAME: Missing $file!!!\n") unless (-f $file_path);
   
   my $expected_size = $files{$file};
   my $actual_size   = (stat($file_path))[7];
   die("$CUR_HOSTNAME: $file $expected_size != $actual_size\n") if ($actual_size != $expected_size);
}
exit(0);
