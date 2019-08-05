'''este scriptconverte tudo que esta na mesma pasta que elee possui terminacao ".csv" paracsv atraves do software dbf2csv: https://gist.github.com/bertspaan/8220892'''

import os
from os import listdir
from os.path import isfile, join

onlyfiles = [f for f in listdir('.') if isfile(join('.', f))]

for file in onlyfiles:
	fileLen=len(file)
	if(fileLen>=5):
		last3=file[fileLen-3:fileLen]
	else:
		last3=None
	if(last3!=None and (last3=='csv' or last3=='CSV')):
		nome=file.split('.')[0].lower()+"_cnv"
		os.system('mongoimport -d projeto2 --drop --type=csv --headerline --file='+file+' -c '+nome)
