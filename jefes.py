import pandas as pd
from pymongo import MongoClient
client = MongoClient()
fijo_ChV=pd.read_csv("/home/tecnoboy/TRABAJO/trabajadadores/ModeloDatos/ChV/resp/sw_personal_201910010940.csv")
var_ChV=pd.read_csv("/home/tecnoboy/TRABAJO/trabajadadores/ModeloDatos/ChV/resp/sw_variablepersona_201910010940.csv")
fijo_cnn=pd.read_csv("/home/tecnoboy/TRABAJO/trabajadadores/ModeloDatos/cnn/resp/sw_personal_201910010925.csv")
var_cnn=pd.read_csv("/home/tecnoboy/TRABAJO/trabajadadores/ModeloDatos/cnn/resp/sw_variablepersona_201910010936.csv")

# Buscamos los jefes de CHV
jefes_chv=var_ChV.loc[(var_ChV['codVariable'] == 'P348')]
jefes_chv2=jefes_chv.sort_values(by='mes', ascending=False)
jefes_chv2=jefes_chv2.drop_duplicates('ficha', keep='first')
jefes_chv2=jefes_chv2.drop(['codVariable', 'mes','flag'], axis=1)
jefes_chv2.rename(columns={'valor':'jefe'},inplace=True)
jefes_chv2['jefe_esnum']=jefes_chv2['jefe'].str.isnumeric()
jefes_chv2Num=jefes_chv2.loc[jefes_chv2['jefe_esnum']==True]
jefes_chv2Alf=jefes_chv2.loc[jefes_chv2['jefe_esnum']==False]

# Preparamos la base para comparar ChV
fijo_ChV.ficha=fijo_ChV.ficha.str.lstrip('0')
fijo_ChV["ficha"]=fijo_ChV["ficha"].str.lower()
fijo_ChV["ficha"]=fijo_ChV["ficha"].str.replace('v','')
fijo_ChV["ficha"]=fijo_ChV["ficha"].str.replace('n','')
fijo_ChV["lastname1"]=fijo_ChV["appaterno"].str.lower()
new = fijo_ChV["nombre"].str.split(" ", n = 1, expand = True)
fijo_ChV["firstname1"]= new[0].str.lower()
fijo_ChV=fijo_ChV.drop(['nombres', 'appaterno','nombre'], axis=1)
fijo_ChV.rename(columns={'ficha':'jefe'},inplace=True)

# Fundimos con criterio jefe_esnum
nombrJef1= pd.merge(jefes_chv2Num, fijo_ChV, on=['jefe'], how='left')
nombrJef1['Manager_loc']=nombrJef1['firstname1']+' '+nombrJef1['lastname1']
nombrJef1=nombrJef1.drop(['jefe_esnum', 'firstname1','lastname1'], axis=1)

# Fundimos con criterio jefes_chv2Alf
new = jefes_chv2Alf["jefe"].str.split(" ", n = 1, expand = True)
jefes_chv2Alf["firstname1"]= new[0].str.lower()
jefes_chv2Alf['lastname1']=new[1].str.lower()
jefes_chv2Alf=jefes_chv2Alf.drop(['jefe', 'jefe_esnum'], axis=1)
nombrJef2= pd.merge(jefes_chv2Alf, fijo_ChV, on=['lastname1','firstname1'], how='left')
nombrJef2['Manager_loc']=nombrJef2['firstname1']+' '+nombrJef2['lastname1']
nombrJef2.rename(columns={'jefe':'register'},inplace=True)
nombrJef2.rename(columns={'ficha':'Manager_id'},inplace=True)
nombrJef2=nombrJef2.drop(['firstname1', 'lastname1'], axis=1)

jefes_chv_total=pd.concat([nombrJef1,nombrJef2])


# Ahora vamos por los jefes de cnn
var_cnn=var_cnn.loc[(var_cnn['codVariable'] == 'P348')]
jefes_cnn2=var_cnn.sort_values(by='mes', ascending=False)
jefes_cnn2=jefes_cnn2.drop_duplicates('ficha', keep='first')
jefes_cnn2=jefes_cnn2.drop(['codVariable', 'mes','flag'], axis=1)
jefes_cnn2.rename(columns={'valor':'jefe'},inplace=True)

# Preparamos la base para comparar cnn
fijo_cnn.ficha=fijo_cnn.ficha.str.lstrip('0')
fijo_cnn["ficha"]=fijo_cnn["ficha"].str.lower()
fijo_cnn["ficha"]=fijo_cnn["ficha"].str.replace('v','')
fijo_cnn["ficha"]=fijo_cnn["ficha"].str.replace('n','')


fijo_cnn["lastname1"]=fijo_cnn["appaterno"].str.lower()
new2 = fijo_cnn["nombre"].str.split(" ", n = 1, expand = True)
fijo_cnn["firstname1"]= new2[0].str.lower()
fijo_cnn=fijo_cnn.drop(['nombres', 'appaterno','nombre'], axis=1)
fijo_cnn.rename(columns={'ficha':'jefe'},inplace=True)
fijo_cnn=fijo_cnn.drop(['codBancoSuc', 'codEstudios', 'codCajaCompens', 'nombres',
       'rut', 'direccion', 'codComuna', 'codCiudad', 'telefono1', 'telefono2',
       'telefono3', 'fax', 'fechaNacimient', 'sexo', 'estadoCivil',
       'nacionalidad', 'situacionMilit', 'numCargasSimp', 'numCargasInval',
       'numCargasMater', 'fechaIngreso', 'fechaPrimerCon', 'fechaContratoV',
       'codINE', 'fechaFiniquito', 'tipoPago', 'formPagoAntic',
       'formPagoLiquiM', 'formPagoRentAc', 'formPagoFiniq', 'tipoCotizIsapr',
       'adicional2pcie', 'AdicSegAFP', 'codExCaja', 'numCtaCte',
       'numTarjetaConH', 'certSueldos', 'certHonorar', 'certHonorPart', 'foto',
       'firma', 'rentaAccesoria', 'appaterno', 'apmaterno', 'nombre', 'Email',
       'WebSite', 'FecCalVac', 'AnoOtraEm', 'CodSucurBan', 'TipoDeposito',
       'TipoVvista', 'CodTipEfe', 'FecCertVacPro', 'RolPrivado',
       'FecTermContrato', 'Usuario', 'Id_ArchivoFoto', 'Id_ArchivoFirma',
       'ActivoPortal', 'UsuarioOT', 'JefeDirecto', 'Art145L', 'Anexo'], axis=1)
nombrJefcnn1= pd.merge(jefes_cnn2, fijo_cnn, on=['jefe'], how='left')
nombrJefcnn1.rename(columns={'ficha':'register',
                            'jefe':'manager_id'
                            },inplace=True)
nombrJefcnn1.rename(columns={'manager_id':'Manager_id'                            
                            },inplace=True)

nombrJefcnn1=nombrJefcnn1.drop(['firstname1', 'lastname1'], axis=1)

jefes_totales=pd.concat([nombrJefcnn1,jefes_chv_total])

jefes_cdf=pd.read_csv("/home/tecnoboy/TRABAJO/Python/analy_virtual/jefes/jefes_cdf.csv")
jefes_cdf.rename(columns={'manager_id':'Manager_id' ,
                             'manager_loc':'Manager_loc' ,
                            },inplace=True)

jefes_totalisimos=pd.concat([jefes_totales,jefes_cdf])


print(personal.columns)