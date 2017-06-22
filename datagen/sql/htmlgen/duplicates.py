#!/usr/bin/python

# This gathers identical url and adds up the total rankings
# Also add the average time spent on site column

import sys, ColumnGenerator

input  = open(sys.argv[1], 'r')
output = open(sys.argv[2], 'w')
delim  = sys.argv[3]

all = {}
counter = 0
line = "Nada"
avgTime = ColumnGenerator.ColumnGenerator("","", range(1, 100))

while not line == "":
   line = input.readline()
   counter = counter+1
   sp = line.split(',')
   if (len(sp) == 2):
      if all.has_key(sp[1]):
         all[sp[1]]+=int(sp[0])
      else:
         all[sp[1]]=int(sp[0])

for ki in all.keys():
   output.write(str(all[ki])+delim+ki.strip()+delim+avgTime.getNextValue()+"\n")

#print "Uniquefied data file, removing %.3f of lines" % (100.0-100.0*len(all.keys())/counter)

input.close()
output.close()
