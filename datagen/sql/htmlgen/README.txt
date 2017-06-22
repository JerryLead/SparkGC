
This is the CACM data generator.  Contact Alex Rasin (alexr@cs.brown.edu) with
any question or comments.

run ./generateData.py to generate the data files.  It will not take any command line
parameters except for the name of a config file.  The config file contains all the
modifyable parameters.  Default configuration file is supplied (config.txt).
Please specify the set of machines and the destination at which you want the files
to be generated.
You should not be running any other files unless you know what you are doing.   

Defaults are set to generate ~1G Rankings.dat and ~20G UserVisits.dat
Data is random and not very meaningful in most cases.

Relevant SQL Schema is in CACM_schema.sql file in top level directory
