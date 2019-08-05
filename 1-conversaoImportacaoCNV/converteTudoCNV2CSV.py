import os
from os import listdir
from os.path import isfile, join
from CNV2CSV import CNV2CSV

onlyfiles = [f for f in listdir('.') if isfile(join('.', f))]

for file in onlyfiles:
	fileLen=len(file)
	if(fileLen>=5):
		last3=file[fileLen-3:fileLen]
	else:
		last3=None
	if(last3!=None and (last3=='cnv' or last3=='CNV')):
		CNV2CSV(file)