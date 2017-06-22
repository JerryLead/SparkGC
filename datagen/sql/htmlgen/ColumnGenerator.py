#!/usr/bin/python

import random, time

# This is a class that generates values for a given field 
# There are no correlations observed between different cols 
class ColumnGenerator:

    def __init__(self, valType, rDelim = "", valList = [] ):
        
        self.valueList = valList
        self.valType = valType 
	self.valRange = len(valList)
        self.delim = rDelim

    def repDelim( self, str ):
        if self.delim == "" :
           return str

        # Delimiter can contain escape characters, so take last symbol
        return str.replace(",", self.delim[-1])

    # Might need the rowid (such as for unique key) so have an option of
    # passing it in.  Currently not used
    def getNextValue( self, rowID = -1 ):
	
	#  There is no value pool to select from 
	if self.valRange == 0:
             
            if self.valType == 'DATE': 
                return self.repDelim(self.genDateValue())

            if self.valType == 'FLOAT': 
                return self.repDelim(self.genFloatValue())

	    # valList will eventually serve as a distribution spec
#            if self.valType == 'INTEGER': 
#                return self.repDelim(self.genIntValue(valList))

            if self.valType == 'IP': 
                return self.repDelim(self.genIPValue())

	else:
	    # Can record it here in case duplicate indexes are a bad thing 
	    rnd = int(random.random()*self.valRange)
	
       	    return self.repDelim(str(self.valueList[rnd]).strip())


    def genDateValue(self):

	# Generate a date field.  Random and uniform between 1973 and 2004
	sec = 100000000 + int(random.random()*1000000000)
	tm = time.localtime(sec)
	return str(tm[0])+"-"+str(tm[1])+"-"+str(tm[2])

    # This is a pretty arbitrary random IPs
    def genIPValue(self):

	fst = 150+int(random.random()*30)
	snd = 100+int(random.random()*35)
	trd = 1+int(random.random()*48)
	fth = 1+int(random.random()*48)
	return str(fst)+"."+str(snd)+"."+str(trd)+"."+str(fth)

    # Return a float value, but make sure it is a string, uniform around
    def genFloatValue(self):

	# Generate a float value (revenue and such). 
 	return str(500*random.random())

    # Return a float value, but make sure it is a string
    # Not Used
#    def genIntValue(self):

	# Generate a float value (revenue). 

