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
$CUR_HOSTNAME =~ m/^d-([\d]+)\.*?/;
my $host_idx = int($1);

my $TARGET_DIR = "/scratch/pavlo/cacm_data";

my %files = ( #"SortGrep535MB" => 100,
              #"SortGrep10GB"  => 100,
              #"SortGrep20GB"  => 50,
              "SortGrep40GB"  => 25,
            );
for my $file (keys %files) {
   my $file_path = "$TARGET_DIR/$file";
   die("$CUR_HOSTNAME: Missing $file!!!\n") unless (-f $file_path);
   #next if ($host_idx-100 > $files{$file});
   
   my @targets = ( );
   if ($file eq "SortGrep20GB") {
      push(@targets, "d-".($host_idx + 50));
   } elsif ($file eq "SortGrep40GB") {
      #push(@targets, "d-".($host_idx + 25));
      push(@targets, "d-".($host_idx + 50)) if ($host_idx <= 125);
      push(@targets, "d-".($host_idx + 50)) if ($host_idx > 125 && $host_idx <= 150);
      #push(@targets, "d-".($host_idx + 75));
   }
   foreach (@targets) {
      my $cmd = "scp -q $file_path $_:$file_path";
      print "$CUR_HOSTNAME: $cmd\n";
      print `$cmd`;
   }
}
exit(0);
