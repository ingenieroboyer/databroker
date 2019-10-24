import pandas as pd
from pymongo import MongoClient
client = MongoClient()
db = client.erme


fijo_chv=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/ChV/sw_personal_201910151156.csv")
fijo_chv['company']='chilevision'
fijo_cnn=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/cnn/sw_personal_201910151154.csv")
fijo_cnn['company']='cnn'
frames=[fijo_chv,fijo_cnn]
personal=pd.concat(frames)
personal=personal.drop([ 'codBancoSuc', 'codEstudios', 'codCajaCompens','codComuna', 'codCiudad',  'fax', 'estadoCivil',
       'nacionalidad', 'situacionMilit', 'numCargasSimp', 'numCargasInval',       'numCargasMater', 'fechaPrimerCon', 'fechaContratoV',
       'codINE',  'tipoPago', 'formPagoAntic',  'formPagoLiquiM', 'formPagoRentAc', 'formPagoFiniq', 'tipoCotizIsapr',
       'adicional2pcie', 'AdicSegAFP', 'codExCaja', 'numCtaCte',       'numTarjetaConH', 'certSueldos', 'certHonorar', 'certHonorPart', 'foto',
       'firma', 'rentaAccesoria', 'FecCalVac', 'AnoOtraEm', 'CodSucurBan', 'TipoDeposito',
       'TipoVvista', 'CodTipEfe', 'FecCertVacPro', 'RolPrivado',       'FecTermContrato', 'Usuario', 'Id_ArchivoFoto', 'Id_ArchivoFirma',
       'ActivoPortal', 'JefeDirecto','UsuarioOT', 'Art145L'], axis=1)
personal.rename(columns={'direccion':'address','telefono1':'phone1',
                          'telefono2':'phone2',
                         'telefono3':'phone3',
                         'WebSite':'ntuser',
                         'fechaNacimient':'birth_date',
                         'fechaIngreso':'admission_date',
                         'appaterno':'lastname1',
                         'apmaterno':'lastname2',
                         'Anexo':'ext',
                         'Email':'email',
                         'ficha':'register',
                        'sexo':'sex',
                        'fechaFiniquito':'discharge_date',}, 
                 inplace=True)


new = personal["nombre"].str.split(" ", n = 1, expand = True)
personal["firstname1"]= new[0]  
personal['firstname2']=new[1]
personal=personal.drop(['nombre','nombres'], axis=1)
personal=personal.loc[personal['discharge_date']=='9999-12-31 00:00:00']
personal['state']:'activo'

def formatlf(df):
    
    df.phone1=df.phone1.str.replace('(','')
    df.phone1=df.phone1.str.replace(')','')
    df.phone1=df.phone1.str.replace('-','')
    df.phone1=df.phone1.str.replace('.','')
    df.phone1=df.phone1.str.replace(' ','')
    df.phone1=df.phone1.str.lstrip('0')

  
    df.phone2=df.phone2.str.replace('(','')
    df.phone2=df.phone2.str.replace(')','')
    df.phone2=df.phone2.str.replace('-','')
    df.phone2=df.phone2.str.replace('.','')
    df.phone2=df.phone2.str.replace(' ','')
    df.phone2=df.phone2.str.lstrip('0')
    

    df.phone3=df.phone3.str.replace('(','')
    df.phone3=df.phone3.str.replace(')','')
    df.phone3=df.phone3.str.replace('-','')
    df.phone3=df.phone3.str.replace('.','')
    df.phone3=df.phone3.str.replace(' ','')
    df.phone3=df.phone3.str.lstrip('0')
    
    return df

def formarut(df):
    df.rut=df.rut.str.lstrip('0')
    df.rut=df.rut.str.replace('-','')
    df.rut=df.rut.str.replace('.','')
    df.rut=df.rut.str.replace(' ','')

    return df
    
def minusculas(df):
    df.firstname1=df.firstname1.str.lower()
    df.firstname2=df.firstname2.str.lower()
    df.lastname1=df.lastname1.str.lower()
    df.lastname2=df.lastname2.str.lower()
    df.address=df.address.str.lower()
    df.email=df.email.str.lower()
    df.sex=df.sex.str.lower()
    df.ntuser=df.ntuser.str.lower()
    


    return df




personal=formatlf(personal)
personal=formarut(personal)
personal=minusculas(personal)

admin=personal
admin=admin.drop(['register', 'company','address', 'phone1', 'phone2', 'phone3','sex',  'lastname1',
       'lastname2', 'email', 'ntuser', 'ext', 'firstname1', 'firstname2','rut'], axis=1)
admin2=admin.to_dict('records')

per=personal
per=per.drop(['register','company', 'birth_date', 'sex', 'admission_date', 'discharge_date', 'lastname1',
       'lastname2', 'email', 'ntuser', 'ext', 'firstname1', 'firstname2','rut'],  axis=1)
per2=per.to_dict('records')


personal=personal.drop(['birth_date', 'admission_date', 'discharge_date','address', 'phone1', 'phone2', 'phone3'], axis=1)

personal['admin']=admin2
personal['personal']=per2

### COMENZAMOS A FORMAR LA TABLA SINDICATO
var_ChV=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/ChV/sw_variablepersona_201910151156.csv")
var_cnn=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/cnn/sw_variablepersona_201910151155.csv")
frames = [var_ChV, var_cnn]
result = pd.concat(frames)
resultado=result.loc[(result['codVariable'] == 'P065')]
sind=resultado.sort_values(by='mes', ascending=False)
sind=sind.drop_duplicates('ficha', keep='first')
sind=sind.drop(['codVariable', 'mes','flag'],  axis=1)
sind.rename(columns={'ficha':'register',
                     'valor':'lab_union'      
                            }, 
                 inplace=True)

dbcon= pd.merge(personal, sind, on='register',how='left')
dbcon['state']='activo'
dbcon=dbcon.fillna('null')



### AHORA TOMAMOS LOS CDF DE SU CSV ORIGEN Y DEL CSV QUE ENVIO SU SINDICATO

fijo_cdf=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/cdf/bd_cdf.csv",sep='|')
fijo_cdf['company']='cdf'
fijo_cdf=fijo_cdf.loc[fijo_cdf['estado']=='Activo']
fijo_cdf=fijo_cdf.loc[fijo_cdf['empresa']=='CDF']

fijo_cdf=fijo_cdf.drop(['id','foto',
       'comuna_id', 'cargo_id', 'nacionalidade_id', 'localizacione_id',
       'sistema_previsione_id', 'sistema_pensione_id', 'horario_id', 'created',
       'modified', 'tipo_contrato_id', 'jefe_id',
       'nivel_educacion_id', 'movil_corporativo', 'dimensione_id',
       'sistema_salud_moneda', 'sistema_salud_valor', 'estudios_titulo',
       'estudios_institucion', 'estados_civile_id','estado','fecha_ingreso',
       'descripcion_cargo', 'empresa'], axis=1)

fijo_cdf.rename(columns={'direccion':'address',
                         'telefono_movil':'phone1',
                          'telefono_particular':'phone2',
                         'telefono_emergencia':'phone3',
                         'fecha_nacimiento':'birth_date',
                         'fecha_indefinido':'admission_date',
                         'nombre':'firstname1',
                         'apellido_paterno':'lastname1',
                         'apellido_materno':'lastname2',
                         'anexo':'ext',
                        'sexo':'sex'}, 
                 inplace=True)

fijo_cdf['firstname2']='null'



fijo_cdf = fijo_cdf.dropna(axis=0, subset=['sex']) ## puede cambiar este criterio
# Cambiar valor de sexo por str
fijo_cdf.loc[fijo_cdf.sex==0.0,['sex']]='m'
fijo_cdf.loc[fijo_cdf.sex==1.0,['sex']]='f'
fijo_cdf['ntuser']='noTiene'
fijo_cdf= minusculas(fijo_cdf)
fijo_cdf= formarut(fijo_cdf)
fijo_cdf= formatlf(fijo_cdf)

fijo_cdf=fijo_cdf[fijo_cdf.rut != '111111111']  ##ELIMINAR
fijo_cdf=fijo_cdf[fijo_cdf.rut != '12345674']  ##ELIMINAR

fijo_cdf['birth_date']=fijo_cdf['birth_date']+' '+'00:00:00'
fijo_cdf['admission_date']=fijo_cdf['admission_date']+' '+'00:00:00'
fijo_cdf['discharge_date']='9999-12-31 00:00:00'
fijo_cdf['state']='activo'

sindicato_cdf=pd.read_csv("/home/tecnoboy/CRECER/trato_data/data/trabajadadores/ModeloDatos/cdf/sindicato_cdf.csv")
dbcon2= pd.merge(fijo_cdf, sindicato_cdf, on='rut',how='left')
dbcon2.loc[dbcon2.sindicato==3.0,['sindicato']]='3'
dbcon2.rename(columns={'sindicato':'lab_union'}, 
                 inplace=True)

admin_cdf=dbcon2
admin_cdf=admin_cdf.drop(['rut','ntuser', 'firstname1','company', 'lastname1', 'lastname2', 'sex',
       'address', 'phone2', 'phone1', 'phone3', 'email', 'ext','firstname2', 'state', 'lab_union'], axis=1)
admin_cdf2=admin_cdf.to_dict('records')
dbcon2['admin']=admin_cdf2

personal_cdf=dbcon2.drop(['rut', 'ntuser','company','firstname1', 'lastname1', 'lastname2', 'sex', 'birth_date',
        'email', 'ext','admin',
       'admission_date', 'firstname2', 'discharge_date', 'state', 'lab_union'], axis=1)
personal_cdf2=personal_cdf.to_dict('records')
dbcon2['personal']=personal_cdf2

dbcon2=dbcon2.fillna('null')
dbcon2=dbcon2.drop([ 'birth_date',
       'address', 'phone2', 'phone1', 'phone3',
       'admission_date',  'discharge_date'
       ], axis=1)

empleados = pd.concat([dbcon2, dbcon])

collection = db.repo
collection.insert_many(empleados.to_dict('records'))


print(personal.columns)


