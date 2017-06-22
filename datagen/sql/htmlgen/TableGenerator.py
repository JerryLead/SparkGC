#!/usr/bin/python

import ColumnGenerator

# This file would generate a table data file based on a
# column delimter.  Takes 

class TableGenerator:

    # Initialize the table generator - set the columns in the
    # proper order and set the delimiter
    def __init__(self, cList, delim):
        
        self.colList = cList
	# If there are escape characters or too many characters in the delimiter, take
	# the last char.  For some reason using escape characters causes it to come in as
	# \ plus delimiter
	delim = delim[-1]
	self.delim = delim

    def genFile(self, rCnt, fName):

	# This should probably error check and refuse to overwrite?
        file = open(fName,'w')

	while rCnt > 0:
            file = open(fName, 'a')
	    str = ""
            counter = 1 
	    for c in self.colList:
               str = str + c.getNextValue() + self.delim

            # Remove the last delimiter (no need for it) and add EOL
	    clrStr = str.strip()  #[:-1]
#            file.write( clrStr )
            file.write(clrStr[:-1]+"\n") 
	    rCnt = rCnt-1

        file.close()

#col1 = ColumnGenerator.ColumnGenerator('DATE')
#col2 = ColumnGenerator.ColumnGenerator('custom',['1','2','3'])
#col3 = ColumnGenerator.ColumnGenerator('FLOAT')

#tg = TableGenerator([col1,col2,col3],'|')
#tg.genFile( 200, 'tmp.dat' )
