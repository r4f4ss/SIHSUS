from pymongo import MongoClient

#pega a database 
projeto=MongoClient().projeto2

#indexa o campo que sofrera a primeira juncao, que deve ser a juncao mais cara computacionalmente
projeto.sih.create_index("DIAG_PRINC")

#array com as juncoes a serem feitas
pipeline=[]

#projecao que seleciona campos usados
camposUsados={'UF_ZI':1,'ANO_CMPT':1,'MES_CMPT':1,'ESPEC':1,'CGC_HOSP':1,'N_AIH':1,'IDENT':1,'CEP':1,'MUNIC_RES':1,'NASC':1,'SEXO':1,'UTI_MES_TO':1,'MARCA_UTI':1,'UTI_INT_TO':1,'DIAR_ACOM':1,'QT_DIARIAS':1,'PROC_REA':1,'VAL_SH':1,'VAL_SP':1,'VAL_TOT':1,'VAL_UTI':1,'US_TOT':1,'DT_INTER':1,'DT_SAIDA':1,'DIAG_PRINC':1,'DIAG_SECUN':1,'COBRANCA':1,'NATUREZA':1,'NAT_JUR':1,'GESTAO':1,'IND_VDRL':1,'MUNIC_MOV':1,'COD_IDADE':1,'IDADE':1,'DIAS_PERM':1,'MORTE':1,'NACIONAL':1,'CAR_INT':1,'HOMONIMO':1,'NUM_FILHOS':1,'INSTRU':1,'CID_NOTIF':1,'CONTRACEP1':1,'CONTRACEP2':1,'GESTRISCO':1,'SEQ_AIH5':1,'CBOR':1,'CNAER':1,'VINCPREV':1,'CNES':1,'CID_ASSO':1,'CID_MORTE':1,'COMPLEX':1,'RACA_COR':1,'ETNIA':1,'VAL_UCI':1,'MARCA_UCI':1,'DIAGSEC1':1,'DIAGSEC2':1,'DIAGSEC3':1,'DIAGSEC4':1,'DIAGSEC5':1,'DIAGSEC6':1,'DIAGSEC7':1,'DIAGSEC8':1,'DIAGSEC9':1,'TPDISEC1':1,'TPDISEC2':1,'TPDISEC3':1,'TPDISEC4':1,'TPDISEC5':1,'TPDISEC6':1,'TPDISEC7':1,'TPDISEC8':1,'TPDISEC9':1}
project={ "$project" : camposUsados}
pipeline.append(project)

#juncao de DIAG_PRINC - CID10.CNV 
projeto['cid10_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cid10_cnv', "localField": "DIAG_PRINC", "foreignField": "codigo", "as": "DIAG_PRINC"}}
unwind11={ "$addFields": {"DIAG_PRINC": { "$arrayElemAt": [ "$DIAG_PRINC", 0 ] }}}
pipeline.append(lookup)
project11={ "$project" : {"DIAG_PRINC._id" : 0}}

#juncao de PROC_REA - TB_SIGTAP.DBF
projeto["tb_sigtap_dbf"].create_index("IP_COD")
lookup={"$lookup": { "from": "tb_sigtap_dbf", "localField": "PROC_REA", "foreignField": "IP_COD", "as": "PROC_REA"}}
unwind1={ "$addFields": {"PROC_REA": { "$arrayElemAt": [ "$PROC_REA", 0 ] }}}
pipeline.append(lookup)
project1={ "$project" : {"PROC_REA._id" : 0}}

#juncao de CNES - TCNESBR.DBF
projeto['tcnesbr_dbf'].create_index("CNES")
lookup={"$lookup": { "from": 'tcnesbr_dbf', "localField": "CNES", "foreignField": "CNES", "as": "CNES"}}
unwind2={ "$addFields": {"CNES": { "$arrayElemAt": [ "$CNES", 0 ] }}}
pipeline.append(lookup)
project2={ "$project" : {"CNES._id" : 0}}

#juncao de UF_ZI - br_municgestor.cnv
projeto['br_municgestor_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'br_municgestor_cnv', "localField": "UF_ZI", "foreignField": "codigo", "as": "UF_ZI"}}
unwind3={ "$addFields": {"UF_ZI": { "$arrayElemAt": [ "$UF_ZI", 0 ] }}}
pipeline.append(lookup)
project3={ "$project" : {"UF_ZI._id" : 0}}


#juncao de ESPEC - LEITOS.CNV 
projeto['leitos_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'leitos_cnv', "localField": "ESPEC", "foreignField": "codigo", "as": "ESPEC"}}
unwind4={ "$addFields": {"ESPEC": { "$arrayElemAt": [ "$ESPEC", 0 ] }}}
pipeline.append(lookup)
project4={ "$project" : {"ESPEC._id" : 0}}

#juncao de CGC_HOSP - TCHBR.DBF
projeto['tchbr_dbf'].create_index("CGC_HOSP")
lookup={"$lookup": { "from": 'tchbr_dbf', "localField": "CGC_HOSP", "foreignField": "CGC_HOSP", "as": "CGC_HOSP"}}
unwind5={ "$addFields": {"CGC_HOSP": { "$arrayElemAt": [ "$CGC_HOSP", 0 ] }}}
pipeline.append(lookup)
project5={ "$project" : {"CGC_HOSP._id" : 0}}

#juncao de IDENT - IDENT.CNV
projeto['ident_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'ident_cnv', "localField": "IDENT", "foreignField": "codigo", "as": "IDENT"}}
unwind6={ "$addFields": {"IDENT": { "$arrayElemAt": [ "$IDENT", 0 ] }}}
pipeline.append(lookup)
project6={ "$project" : {"IDENT._id" : 0}}

#juncao de MUNIC_RES - br_municip.cnv
projeto['br_municip_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'br_municip_cnv', "localField": "MUNIC_RES", "foreignField": "codigo", "as": "MUNIC_RES"}}
unwind7={ "$addFields": {"MUNIC_RES": { "$arrayElemAt": [ "$MUNIC_RES", 0 ] }}}
pipeline.append(lookup)
project7={ "$project" : {"MUNIC_RES._id" : 0}}

#juncao de UTI_MES_TO - DIARIASUTI.CNV
projeto['diariasuti_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'diariasuti_cnv', "localField": "UTI_MES_TO", "foreignField": "codigo", "as": "UTI_MES_TO"}}
unwind8={ "$addFields": {"UTI_MES_TO": { "$arrayElemAt": [ "$UTI_MES_TO", 0 ] }}}
pipeline.append(lookup)
project8={ "$project" : {"UTI_MES_TO._id" : 0}}

#juncao de MARCA_UTI - MARCAUTI.CNV
projeto['marcauti_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'marcauti_cnv', "localField": "MARCA_UTI", "foreignField": "codigo", "as": "MARCA_UTI"}}
unwind9={ "$addFields": {"MARCA_UTI": { "$arrayElemAt": [ "$MARCA_UTI", 0 ] }}}
pipeline.append(lookup)
project9={ "$project" : {"MARCA_UTI._id" : 0}}

#juncao de SEXO - SEXO.CNV
projeto['sexo_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'sexo_cnv', "localField": "SEXO", "foreignField": "codigo", "as": "SEXO"}}
unwind10={ "$addFields": {"SEXO": { "$arrayElemAt": [ "$SEXO", 0 ] }}}
pipeline.append(lookup)
project10={ "$project" : {"SEXO._id" : 0}}


#juncao de DIAG_SECUN - CID10.CNV
projeto['cid10_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cid10_cnv', "localField": "DIAG_SECUN", "foreignField": "codigo", "as": "DIAG_SECUN"}}
unwind12={ "$addFields": {"DIAG_SECUN": { "$arrayElemAt": [ "$DIAG_SECUN", 0 ] }}}
pipeline.append(lookup)
project12={ "$project" : {"DIAG_SECUN._id" : 0}}

#juncao de COBRANCA - SAIDAPERM.CNV
projeto['saidaperm_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'saidaperm_cnv', "localField": "COBRANCA", "foreignField": "codigo", "as": "COBRANCA"}}
unwind13={ "$addFields": {"COBRANCA": { "$arrayElemAt": [ "$COBRANCA", 0 ] }}}
pipeline.append(lookup)
project13={ "$project" : {"COBRANCA._id" : 0}}

#juncao de NATUREZA - NATUREZA.CNV
projeto['natureza_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'natureza_cnv', "localField": "NATUREZA", "foreignField": "codigo", "as": "NATUREZA"}}
unwind14={ "$addFields": {"NATUREZA": { "$arrayElemAt": [ "$NATUREZA", 0 ] }}}
pipeline.append(lookup)
project14={ "$project" : {"NATUREZA._id" : 0}}

#juncao de NAT_JUR - NATJUR.CNV
projeto['natjur_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'natjur_cnv', "localField": "NAT_JUR", "foreignField": "codigo", "as": "NAT_JUR"}}
unwind15={ "$addFields": {"NAT_JUR": { "$arrayElemAt": [ "$NAT_JUR", 0 ] }}}
pipeline.append(lookup)
project15={ "$project" : {"NAT_JUR._id" : 0}}

#juncao de GESTAO - GESTAO.CNV
projeto['gestao_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'gestao_cnv', "localField": "GESTAO", "foreignField": "codigo", "as": "GESTAO"}}
unwind16={ "$addFields": {"GESTAO": { "$arrayElemAt": [ "$GESTAO", 0 ] }}}
pipeline.append(lookup)
project16={ "$project" : {"GESTAO._id" : 0}}

#juncao de IND_VDRL - SIMNAO.CNV
projeto['simnao_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'simnao_cnv', "localField": "IND_VDRL", "foreignField": "codigo", "as": "IND_VDRL"}}
unwind17={ "$addFields": {"IND_VDRL": { "$arrayElemAt": [ "$IND_VDRL", 0 ] }}}
pipeline.append(lookup)
project17={ "$project" : {"IND_VDRL._id" : 0}}

#juncao de MUNIC_MOV - br_municip.cnv
projeto['br_municip_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'br_municip_cnv', "localField": "MUNIC_MOV", "foreignField": "codigo", "as": "MUNIC_MOV"}}
unwind18={ "$addFields": {"MUNIC_MOV": { "$arrayElemAt": [ "$MUNIC_MOV", 0 ] }}}
pipeline.append(lookup)
project18={ "$project" : {"MUNIC_MOV._id" : 0}}

#juncao de COD_IDADE - IDADEBAS.CNV
projeto['idadebas_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'idadebas_cnv', "localField": "COD_IDADE", "foreignField": "codigo", "as": "COD_IDADE"}}
unwind19={ "$addFields": {"COD_IDADE": { "$arrayElemAt": [ "$COD_IDADE", 0 ] }}}
pipeline.append(lookup)
project19={ "$project" : {"COD_IDADE._id" : 0}}

#juncao de DIAS_PERM - PERM.CNV
projeto['perm_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'perm_cnv', "localField": "DIAS_PERM", "foreignField": "codigo", "as": "DIAS_PERM"}}
unwind20={ "$addFields": {"DIAS_PERM": { "$arrayElemAt": [ "$DIAS_PERM", 0 ] }}}
pipeline.append(lookup)
project20={ "$project" : {"DIAS_PERM._id" : 0}}

#juncao de MORTE - MORTES.CNV
projeto['mortes_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'mortes_cnv', "localField": "MORTE", "foreignField": "codigo", "as": "MORTE"}}
unwind21={ "$addFields": {"MORTE": { "$arrayElemAt": [ "$MORTE", 0 ] }}}
pipeline.append(lookup)
project21={ "$project" : {"MORTE._id" : 0}}

#juncao de NACIONAL - NACION3D.CNV
projeto['nacion3d_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'nacion3d_cnv', "localField": "NACIONAL", "foreignField": "codigo", "as": "NACIONAL"}}
unwind22={ "$addFields": {"NACIONAL": { "$arrayElemAt": [ "$NACIONAL", 0 ] }}}
pipeline.append(lookup)
project22={ "$project" : {"NACIONAL._id" : 0}}

#juncao de CAR_INT - CARATEND.CNV
projeto['caratend_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'caratend_cnv', "localField": "CAR_INT", "foreignField": "codigo", "as": "CAR_INT"}}
unwind23={ "$addFields": {"CAR_INT": { "$arrayElemAt": [ "$CAR_INT", 0 ] }}}
pipeline.append(lookup)
project23={ "$project" : {"CAR_INT._id" : 0}}

#juncao de NUM_FILHOS - NUMFILH.CNV
projeto['numfilh_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'numfilh_cnv', "localField": "NUM_FILHOS", "foreignField": "codigo", "as": "NUM_FILHOS"}}
unwind24={ "$addFields": {"NUM_FILHOS": { "$arrayElemAt": [ "$NUM_FILHOS", 0 ] }}}
pipeline.append(lookup)
project24={ "$project" : {"NUM_FILHOS._id" : 0}}

#juncao de INSTRU - INSTRU.CNV
projeto['instru_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'instru_cnv', "localField": "INSTRU", "foreignField": "codigo", "as": "INSTRU"}}
unwind25={ "$addFields": {"INSTRU": { "$arrayElemAt": [ "$INSTRU", 0 ] }}}
pipeline.append(lookup)
project25={ "$project" : {"INSTRU._id" : 0}}

#juncao de CID_NOTIF - CID10.CNV
projeto['cid10_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cid10_cnv', "localField": "CID_NOTIF", "foreignField": "codigo", "as": "CID_NOTIF"}}
unwind26={ "$addFields": {"CID_NOTIF": { "$arrayElemAt": [ "$CID_NOTIF", 0 ] }}}
pipeline.append(lookup)
project26={ "$project" : {"CID_NOTIF._id" : 0}}

#juncao de CONTRACEP1 - CONTRAC.CNV
projeto['contrac_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'contrac_cnv', "localField": "CONTRACEP1", "foreignField": "codigo", "as": "CONTRACEP1"}}
unwind27={ "$addFields": {"CONTRACEP1": { "$arrayElemAt": [ "$CONTRACEP1", 0 ] }}}
pipeline.append(lookup)
project27={ "$project" : {"CONTRACEP1._id" : 0}}

#juncao de CONTRACEP2 - CONTRAC.CNV
projeto['contrac_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'contrac_cnv', "localField": "CONTRACEP2", "foreignField": "codigo", "as": "CONTRACEP2"}}
unwind28={ "$addFields": {"CONTRACEP2": { "$arrayElemAt": [ "$CONTRACEP2", 0 ] }}}
pipeline.append(lookup)
project28={ "$project" : {"CONTRACEP2._id" : 0}}

#juncao de GESTRISCO - SIMNAO.CNV
projeto['simnao_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'simnao_cnv', "localField": "GESTRISCO", "foreignField": "codigo", "as": "GESTRISCO"}}
unwind29={ "$addFields": {"GESTRISCO": { "$arrayElemAt": [ "$GESTRISCO", 0 ] }}}
pipeline.append(lookup)
project29={ "$project" : {"GESTRISCO._id" : 0}}

#juncao de SEQ_AIH5 - SEQAIH.CNV 
projeto['seqaih_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'seqaih_cnv', "localField": "SEQ_AIH5", "foreignField": "codigo", "as": "SEQ_AIH5"}}
unwind30={ "$addFields": {"SEQ_AIH5": { "$arrayElemAt": [ "$SEQ_AIH5", 0 ] }}}
pipeline.append(lookup)
project30={ "$project" : {"SEQ_AIH5._id" : 0}}

#juncao de CBOR - CBO2002.CNV
projeto['cbo2002_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cbo2002_cnv', "localField": "CBOR", "foreignField": "codigo", "as": "CBOR"}}
unwind31={ "$addFields": {"CBOR": { "$arrayElemAt": [ "$CBOR", 0 ] }}}
pipeline.append(lookup)
project31={ "$project" : {"CBOR._id" : 0}}

#juncao de CNAER - CNAE.CNV
projeto['cnaeg_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'cnaeg_cnv', "localField": "CNAER", "foreignField": "codigo", "as": "CNAER"}}
unwind32={ "$addFields": {"CNAER": { "$arrayElemAt": [ "$CNAER", 0 ] }}}
pipeline.append(lookup)
project32={ "$project" : {"CNAER._id" : 0}}

#juncao de VINCPREV - VINCPREV.CNV
projeto['vinvprev_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'vincprev_cnv', "localField": "VINCPREV", "foreignField": "codigo", "as": "VINCPREV"}}
unwind33={ "$addFields": {"VINCPREV": { "$arrayElemAt": [ "$VINCPREV", 0 ] }}}
pipeline.append(lookup)
project33={ "$project" : {"VINCPREV._id" : 0}}

#juncao de COMPLEX - COMPLEX2.CNV
projeto['complex2_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'complex2_cnv', "localField": "COMPLEX", "foreignField": "codigo", "as": "COMPLEX"}}
unwind34={ "$addFields": {"COMPLEX": { "$arrayElemAt": [ "$COMPLEX", 0 ] }}}
pipeline.append(lookup)
project34={ "$project" : {"COMPLEX._id" : 0}}

#juncao de RACA_COR - RACACOR.CNV
projeto['racacor_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'racacor_cnv', "localField": "RACA_COR", "foreignField": "codigo", "as": "RACA_COR"}}
unwind35={ "$addFields": {"RACA_COR": { "$arrayElemAt": [ "$RACA_COR", 0 ] }}}
pipeline.append(lookup)
project35={ "$project" : {"RACA_COR._id" : 0}}

#juncao de ETNIA - ETNIA.CNV
projeto['etnia_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'etnia_cnv', "localField": "ETNIA", "foreignField": "codigo", "as": "ETNIA"}}
unwind36={ "$addFields": {"ETNIA": { "$arrayElemAt": [ "$ETNIA", 0 ] }}}
pipeline.append(lookup)
project36={ "$project" : {"ETNIA._id" : 0}}

#juncao de DIAGSEC1 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC1", "foreignField": "CD_COD", "as": "DIAGSEC1"}}
unwind37={ "$addFields": {"DIAGSEC1": { "$arrayElemAt": [ "$DIAGSEC1", 0 ] }}}
pipeline.append(lookup)
project37={ "$project" : {"DIAGSEC1._id" : 0}}


#juncao de DIAGSEC2 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC2", "foreignField": "CD_COD", "as": "DIAGSEC2"}}
unwind38={ "$addFields": {"DIAGSEC2": { "$arrayElemAt": [ "$DIAGSEC2", 0 ] }}}
pipeline.append(lookup)
project38={ "$project" : {"DIAGSEC2._id" : 0}}

#juncao de DIAGSEC3 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC3", "foreignField": "CD_COD", "as": "DIAGSEC3"}}
unwind39={ "$addFields": {"DIAGSEC3": { "$arrayElemAt": [ "$DIAGSEC3", 0 ] }}}
pipeline.append(lookup)
project39={ "$project" : {"DIAGSEC3._id" : 0}}

#juncao de DIAGSEC4 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC4", "foreignField": "CD_COD", "as": "DIAGSEC4"}}
unwind40={ "$addFields": {"DIAGSEC4": { "$arrayElemAt": [ "$DIAGSEC4", 0 ] }}}
pipeline.append(lookup)
project40={ "$project" : {"DIAGSEC4._id" : 0}}

#juncao de DIAGSEC5 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC5", "foreignField": "CD_COD", "as": "DIAGSEC5"}}
unwind41={ "$addFields": {"DIAGSEC5": { "$arrayElemAt": [ "$DIAGSEC5", 0 ] }}}
pipeline.append(lookup)
project41={ "$project" : {"DIAGSEC5._id" : 0}}

#juncao de DIAGSEC6 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC6", "foreignField": "CD_COD", "as": "DIAGSEC6"}}
unwind42={ "$addFields": {"DIAGSEC6": { "$arrayElemAt": [ "$DIAGSEC6", 0 ] }}}
pipeline.append(lookup)
project42={ "$project" : {"DIAGSEC6._id" : 0}}

#juncao de DIAGSEC7 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC7", "foreignField": "CD_COD", "as": "DIAGSEC7"}}
unwind43={ "$addFields": {"DIAGSEC7": { "$arrayElemAt": [ "$DIAGSEC7", 0 ] }}}
pipeline.append(lookup)
project43={ "$project" : {"DIAGSEC7._id" : 0}}

#juncao de DIAGSEC8 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC8", "foreignField": "CD_COD", "as": "DIAGSEC8"}}
unwind44={ "$addFields": {"DIAGSEC8": { "$arrayElemAt": [ "$DIAGSEC8", 0 ] }}}
pipeline.append(lookup)
project44={ "$project" : {"DIAGSEC8._id" : 0}}

#juncao de DIAGSEC9 - cid10.dbf
projeto['cid10_dbf'].create_index("CD_COD")
lookup={"$lookup": { "from": 'cid10_dbf', "localField": "DIAGSEC9", "foreignField": "CD_COD", "as": "DIAGSEC9"}}
unwind45={ "$addFields": {"DIAGSEC9": { "$arrayElemAt": [ "$DIAGSEC9", 0 ] }}}
pipeline.append(lookup)
project45={ "$project" : {"DIAGSEC9._id" : 0}}

#juncao de TPDISEC1 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC1", "foreignField": "codigo", "as": "TPDISEC1"}}
unwind46={ "$addFields": {"TPDISEC1": { "$arrayElemAt": [ "$TPDISEC1", 0 ] }}}
pipeline.append(lookup)
project46={ "$project" : {"TPDISEC1._id" : 0}}

#juncao de TPDISEC2 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC2", "foreignField": "codigo", "as": "TPDISEC2"}}
unwind47={ "$addFields": {"TPDISEC2": { "$arrayElemAt": [ "$TPDISEC2", 0 ] }}}
pipeline.append(lookup)
project47={ "$project" : {"TPDISEC2._id" : 0}}

#juncao de TPDISEC3 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC3", "foreignField": "codigo", "as": "TPDISEC3"}}
unwind48={ "$addFields": {"TPDISEC3": { "$arrayElemAt": [ "$TPDISEC3", 0 ] }}}
pipeline.append(lookup)
project48={ "$project" : {"TPDISEC3._id" : 0}}

#juncao de TPDISEC4 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC4", "foreignField": "codigo", "as": "TPDISEC4"}}
unwind49={ "$addFields": {"TPDISEC4": { "$arrayElemAt": [ "$TPDISEC4", 0 ] }}}
pipeline.append(lookup)
project49={ "$project" : {"TPDISEC4._id" : 0}}

#juncao de TPDISEC5 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC5", "foreignField": "codigo", "as": "TPDISEC5"}}
unwind50={ "$addFields": {"TPDISEC5": { "$arrayElemAt": [ "$TPDISEC5", 0 ] }}}
pipeline.append(lookup)
project50={ "$project" : {"TPDISEC5._id" : 0}}

#juncao de TPDISEC6 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC6", "foreignField": "codigo", "as": "TPDISEC6"}}
unwind51={ "$addFields": {"TPDISEC6": { "$arrayElemAt": [ "$TPDISEC6", 0 ] }}}
pipeline.append(lookup)
project51={ "$project" : {"TPDISEC6._id" : 0}}

#juncao de TPDISEC7 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC7", "foreignField": "codigo", "as": "TPDISEC7"}}
unwind52={ "$addFields": {"TPDISEC7": { "$arrayElemAt": [ "$TPDISEC7", 0 ] }}}
pipeline.append(lookup)
project52={ "$project" : {"TPDISEC7._id" : 0}}

#juncao de TPDISEC8 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC8", "foreignField": "codigo", "as": "TPDISEC8"}}
unwind53={ "$addFields": {"TPDISEC8": { "$arrayElemAt": [ "$TPDISEC8", 0 ] }}}
pipeline.append(lookup)
project53={ "$project" : {"TPDISEC8._id" : 0}}

#juncao de TPDISEC9 - TP_DIAGSEC.CNV
projeto['tp_diagsec_cnv'].create_index("codigo")
lookup={"$lookup": { "from": 'tp_diagsec_cnv', "localField": "TPDISEC9", "foreignField": "codigo", "as": "TPDISEC9"}}
unwind54={ "$addFields": {"TPDISEC9": { "$arrayElemAt": [ "$TPDISEC9", 0 ] }}}
pipeline.append(lookup)
project54={ "$project" : {"TPDISEC9._id" : 0}}


#executa os unwinds para retirar dos arrays os dados
pipeline.append(unwind1)
pipeline.append(unwind2)
pipeline.append(unwind3)
pipeline.append(unwind4)
pipeline.append(unwind5)
pipeline.append(unwind6)
pipeline.append(unwind7)
pipeline.append(unwind8)
pipeline.append(unwind9)
pipeline.append(unwind10)
pipeline.append(unwind11)
pipeline.append(unwind12)
pipeline.append(unwind13)
pipeline.append(unwind14)
pipeline.append(unwind15)
pipeline.append(unwind16)
pipeline.append(unwind17)
pipeline.append(unwind18)
pipeline.append(unwind19)
pipeline.append(unwind20)
pipeline.append(unwind21)
pipeline.append(unwind22)
pipeline.append(unwind23)
pipeline.append(unwind24)
pipeline.append(unwind25)
pipeline.append(unwind26)
pipeline.append(unwind27)
pipeline.append(unwind28)
pipeline.append(unwind29)
pipeline.append(unwind30)
pipeline.append(unwind31)
pipeline.append(unwind32)
pipeline.append(unwind33)
pipeline.append(unwind34)
pipeline.append(unwind35)
pipeline.append(unwind36)
pipeline.append(unwind37)
pipeline.append(unwind38)
pipeline.append(unwind39)
pipeline.append(unwind40)
pipeline.append(unwind41)
pipeline.append(unwind42)
pipeline.append(unwind43)
pipeline.append(unwind44)
pipeline.append(unwind45)
pipeline.append(unwind46)
pipeline.append(unwind47)
pipeline.append(unwind48)
pipeline.append(unwind49)
pipeline.append(unwind50)
pipeline.append(unwind51)
pipeline.append(unwind52)
pipeline.append(unwind53)
pipeline.append(unwind54)



pipeline.append(project1)
pipeline.append(project2)
pipeline.append(project3)
pipeline.append(project4)
pipeline.append(project5)
pipeline.append(project6)
pipeline.append(project7)
pipeline.append(project8)
pipeline.append(project9)
pipeline.append(project10)
pipeline.append(project11)
pipeline.append(project12)
pipeline.append(project13)
pipeline.append(project14)
pipeline.append(project15)
pipeline.append(project16)
pipeline.append(project17)
pipeline.append(project18)
pipeline.append(project19)
pipeline.append(project20)
pipeline.append(project21)
pipeline.append(project22)
pipeline.append(project23)
pipeline.append(project24)
pipeline.append(project25)
pipeline.append(project26)
pipeline.append(project27)
pipeline.append(project28)
pipeline.append(project29)
pipeline.append(project30)
pipeline.append(project31)
pipeline.append(project32)
pipeline.append(project33)
pipeline.append(project34)
pipeline.append(project35)
pipeline.append(project36)
pipeline.append(project37)
pipeline.append(project38)
pipeline.append(project39)
pipeline.append(project40)
pipeline.append(project41)
pipeline.append(project42)
pipeline.append(project43)
pipeline.append(project44)
pipeline.append(project45)
pipeline.append(project46)
pipeline.append(project47)
pipeline.append(project48)
pipeline.append(project49)
pipeline.append(project50)
pipeline.append(project51)
pipeline.append(project52)
pipeline.append(project53)
pipeline.append(project54)


out={ "$out" : "sih_completa" }
pipeline.append(out)

#agregacao onde ocorrem as juncoes
projeto.sih.aggregate(pipeline)

cursor=projeto.sih_completa.find()
if __name__ == '__main__':
	i=0
	saida=open("juncoes.out","w")
	for ele in cursor:
		#print(ele)
		if(i==60):break
		saida.write(str(ele)+"\n")
		i=i+1
	saida.close()
