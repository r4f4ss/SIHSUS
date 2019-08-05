import codecs

def CNV2CSV(fileName):
	countLinha=1
	nomeSaida=fileName.split('.')[0]+'.csv'
	saida=open(nomeSaida,'w')
	saida.write('valor,codigo\n')
	with codecs.open(fileName,'r',encoding='iso8859-1') as f:
		for line in f:
			if(countLinha>1):
				#line=line.strip(' ')
				line=line.strip('\n')
				line=line.strip('\r')
				line=line.strip(',')
				campos=divide_duas_colunas(line)
				campos[0]=remove_contagem(campos[0])
				campos[1]=campos[1].strip(' ')
				if(campos!=[]):
					camposFinal=pegaSubCampos(campos)
					for linhaSaida in camposFinal:
						arraySaida=juntaLinha(linhaSaida)
						saida.write(arraySaida)
			countLinha=countLinha+1


def remove_contagem(campo):
	primeiro=0
	campo=campo.strip(' ')
	for i in range(len(campo)):
		if(campo[i]==' '):
			primeiro=i
			break
	for i in range(primeiro,len(campo)):
		if(campo[i]!=' '):
			primeiro=i
			break
	return campo[primeiro:len(campo)]


def divide_duas_colunas(line):
	'''final_codigo=len(line)

	for i in range(59,len(line)):
		if(not(line[i].isdigit() or line[i]==',' or line[i]=='-')):
			final_codigo=i
			break
	codigo=line[59:final_codigo]
	if(final_codigo<len(line)):
		valor=line[0:59]+line[final_codigo:len(line)]
	else:
		valor=line[0:59]
	return [valor,codigo]'''
	return [line[0:59],line[59:len(line)]]

def juntaLinha(array):
	arraySaida=''
	cont=0
	for i in array:
		if(cont!=0):
			arraySaida=arraySaida+','+i
		else:
			arraySaida=arraySaida+i
		cont=cont+1
	arraySaida=arraySaida+'\n'
	return arraySaida

def pegaSubCampos(array):
	retorno=[]
	codigos=array[len(array)-1].split(',')
	for codigo in codigos:
		limites=codigo.split('-')
		if(len(limites)==2 ):
			for i in range(int(limites[0]),int(limites[1])+1):
				retorno.append([array[0],str(i)])
		elif(len(limites)==1):
			retorno.append([array[0],limites[0]])
	return retorno