#!/usr/bin/python

import sys

a = open(sys.argv[1],'r')

ln = a.readline()
#err1 = open("/tmp/error_arasin.txt", 'a')
#mapReduce = open("/scratch/cacm_data/RankingsMR.dat", 'w')

counter = 1
tcounter = 1

while ln != None and ln!="":

#   print " TC ", str(tcounter), " LN !", ln , "!"+str(len(ln))
   tcounter = tcounter + 1
   if ( len(ln) > 3 ):
      counter = counter + 1
      sp = ln.split(',')
      print sp[1],
#         mapReduce.write(sp1[1]+"\t"+sp[0]+"\n")
#      except:
#         print "ERROR: coul "+str(ln)+"<=\n")

   ln = a.readline()


#err1.write("URL extraction "+str(tcounter)+" out of "+ str(counter)+"\n")  
#mapReduce.close()
#err1.close()
