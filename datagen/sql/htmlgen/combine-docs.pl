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

my $INPUT_DIR = $ARGV[0];
my $OUTPUT_DIR = $ARGV[1];

#unless (-d $OUTPUT_DIR) {
   system("mkdir -p $OUTPUT_DIR");
   foreach my $dir (glob("$INPUT_DIR/*")) {
      my $id;
      if ($dir =~ m/.*?\/([\d]+)/) {
         $id = $1;
      } else {
         die("ERROR: Invalid $dir name\n");
      }
      system("touch $OUTPUT_DIR/$id.html");
      for my $html (glob("$dir/*.html")) {
         system("cat $html >> $OUTPUT_DIR/$id.html");
         system("echo >> $OUTPUT_DIR/$id.html");
      } # FOR
   } # FOR
#} # UNLESS
exit;