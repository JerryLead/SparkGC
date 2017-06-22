#!/usr/bin/python

import ColumnGenerator, TableGenerator, sys

# This file is called from the data generating script - so the parameters
# will be passed in the necessary order (instead of properly supplying them)

numRows = int(sys.argv[1])
urlFile = sys.argv[2]
outFile = sys.argv[3]
remoteDir = sys.argv[4]
delim = sys.argv[5]

# All files are assumed to be in the remote directory
uAgentFile = "%s/user_agents.dat" % remoteDir
cCodeFile = "%s/country_codes_plus_languages.dat" % remoteDir
sKeywords = "%s/keywords.dat" % remoteDir

uAgentFD = open(uAgentFile, 'r')
cCodeFD = open(cCodeFile, 'r')
sKeywordsFD = open(sKeywords, 'r')

# Build up the necessary columns

IP = ColumnGenerator.ColumnGenerator("IP", "", [])
urlList = open(urlFile,'r')
dates = ColumnGenerator.ColumnGenerator("DATE","",  [])
profit = ColumnGenerator.ColumnGenerator("FLOAT", "", [])
# Need to fix
uAgent = ColumnGenerator.ColumnGenerator("", "", uAgentFD.readlines())
sKeywords = ColumnGenerator.ColumnGenerator("", "", map(lambda x:x.strip(), sKeywordsFD.readlines()))

URL =ColumnGenerator.ColumnGenerator("", "",urlList.readlines())
cCodes =ColumnGenerator.ColumnGenerator("", delim, cCodeFD.readlines())
# Need a distribution of values
tOnSite = ColumnGenerator.ColumnGenerator("INT","",range(1,10))


urlList.close()
uAgentFD.close()
cCodeFD.close()
sKeywordsFD.close()

tableGen = TableGenerator.TableGenerator( [IP, URL, dates, profit, uAgent, cCodes, sKeywords, tOnSite], delim )

tableGen.genFile(numRows, outFile)
