#!/usr/bin/env python
# coding: utf-8

# In[138]:


from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import parametros
import re
import time
# Aqui comence con los cambios, quite algunas librerias que no se estan usando [GM]


# ## Conectando a ElasticSearch

# La ultima línea se utiliza para garantizar la ejecución de la consulta
# * timeout es el tiempo para cada ejecución
# * max_retries el número de intentos si la conexión falla
# * retry_on_timeout para activar los reitentos

# In[139]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=60, max_retries=3, retry_on_timeout=True
) 


# ### Calculando fechas para la ejecución

# * Se calculan las fechas para asociar al nombre del indice
# * fecha_hoy es usada para concatenar al nombre del indice principal previa inserción

# In[140]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))
ahora_format = "%Y-%m-%d"'T'"%H:%M:%S"
ahora = str(now.strftime(ahora_format))
ahora_cdd = str(now.strftime("%Y-%m-%d"' '"%H:%M:%S"))
fechaAhora = str(now.strftime("%Y%m%d%H%M%S"))
datos_logs =""
#fechaAhora


# ### Definiendo indice principal con fecha de hoy
# * 1.- Primer detalle (aqui se deberia de quitar esa fecha_hoy ? )
# *     [ indice parameter= "dev-mintic-concat" ]   [GM]
# 
# 

# In[141]:


indice = parametros.disponibilidad_tableros_index
indice_control = parametros.tableros_mintic_control


# ### Función para generar JSON compatible con ES

# In[142]:


def filterKeys(document, use_these_keys):
    return {key: document.get(key) for key in use_these_keys }


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[143]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
           "_source": ["gestion.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                              "field":"jerarquia_disponibilidad1"
                    }
                  }
              ]
            }
          }
        },
        size=total_docs
    )
    #print(es.info())
    elastic_docs = response["hits"]["hits"]
    fields = {}
    for num, doc in enumerate(elastic_docs):
        fecha_ejecucion = doc["_source"]['gestion.fechaControl']
except:
    response["hits"]["hits"] = []
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-06-01T00:00:00'
print("ultima fecha para control de ejecucion gestion_estado_incidentes:",fecha_ejecucion)


# ### 1.- leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# 
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede.)

# In[144]:


total_docs = 10000
try:
    response = es.search(
        index= parametros.semilla_inventario_index,
        body={
               "_source": ['site_id','nombre_municipio', 'nombre_departamento', 'nombre_centro_pob','energiadesc'
                           ,'nombreSede','latitud','longitud','id_Beneficiario','COD_ISO','codDanesede',
                           'cod_servicio','codDaneMuni','nombre_centro_pob','codCentroPoblado','codDaneInstitucionEdu',
                           'tipoSitio','detalleSitio','energia','region','matricula','DDA','grupoDesc','estadoInstalacion',
                           'nombreInstitucionEd']
        },
        size=total_docs
    )
    #print(es.info())
    elastic_docs = response["hits"]["hits"]
    fields = {}
    for num, doc in enumerate(elastic_docs):
        source_data = doc["_source"]
        for key, val in source_data.items():
            try:
                fields[key] = np.append(fields[key], val)
            except KeyError:
                fields[key] = np.array([val])

    datos_semilla = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) #pd.DataFrame(fields)
except Exception as e:
    print(e)
    print("fecha:",now,"- Error en lectura de datos semilla")
    #exit()


# In[145]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return '4.596389' if y=='lat' else '-74.074639'


# In[146]:


datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['gestion.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['gestion.location']=datos_semilla['gestion.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# ### 2.- Leyendo datos de Cambium-Device-Devices

# In[147]:


# Se extraen los datos de cambium-devicedevices,  'cambium-devicedevices'

def traerDDevices():
#     print(fecha_max)
#     print(fecha_tope)
    
    total_docs = 30000
    try:
        response = es.search(
            index= parametros.cambium_d_d_index,
            body={
                "_source": ["registration_date","mac","ip","ap_group","site_id","status","name"]
                ,"query": {
                    "match_all": {}
                  }
            },
            size=total_docs
        )
        elastic_docs = response["hits"]["hits"]
        datos_DDevices = pd.DataFrame([x["_source"] for x in elastic_docs])
        
#         datos_DDevices = datos_DDevices[(datos_DDevices["registration_date"] >= fecha_max) & (datos_DDevices["registration_date"] < fecha_tope)]
#         print(datos_DDevices)
        
        datos_DDevices['site_id'] = datos_DDevices['site_id'].str.strip()
        datos_DDevices  = datos_DDevices.rename(columns={'ap_group':'gestion.ptos_acceso'})
        datos_DDevices  = datos_DDevices.dropna(subset=['site_id'])
        datos_DDevices.fillna('', inplace=True)
        datos_DDevices = datos_DDevices.drop(datos_DDevices[(datos_DDevices['site_id']=='')].index)
        datos_DDevices.sort_values(['site_id','gestion.ptos_acceso'], inplace=True)
        
        datos_DDevices['gestion.ptos_acceso'] = datos_DDevices['gestion.ptos_acceso'].str.split("-", n = 1, expand = True)[0]
        datos_DDevices['gestion.ptos_acceso'] = datos_DDevices['gestion.ptos_acceso'].str.split("_", n = 1, expand = True)[0]
        datos_DDevices['gestion.ptos_acceso'] = datos_DDevices['gestion.ptos_acceso'].str.split(".", n = 1, expand = True)[0]
        datos_DDevices = datos_DDevices.drop(datos_DDevices[(datos_DDevices['gestion.ptos_acceso']=='')].index)
        datos_DDevices = datos_DDevices.drop_duplicates('mac')
        
        return datos_DDevices 
    except Exception as e:
        print(e)
        return pd.DataFrame()
    


# Realizando bucle hasta conseguir datos de servicemanager-incidentes o hasta la fecha actual para realizar la carga de datos

# In[148]:


def drop_invalid_values(df, fields=[], values = ['NA','',np.NaN,None],operator='or'):
    fields = df.columns if fields==[] else fields
    if operator=='or':
        freduce = np.logical_or.reduce
    elif operator=='and':
        freduce = np.logical_and.reduce
    else:
        return
    
    df.drop(df[freduce([df[c].isin(values) for c in fields])].index,inplace=True)


# In[149]:


datos_CDD=pd.DataFrame(columns=['registration_date', 
                                    'gestion.ptos_acceso', 
                                    'ip', 
                                    'site_id', 
                                    'mac',
                                    'status'])


# In[150]:


# fecha_max_mintic = fecha_ejecucion.replace('T',' ')

# fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, "%Y-%m-%d %H:%M:%S")+timedelta(minutes=60)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
# datos_CDD = traerDDevices(fecha_max_mintic,fecha_tope_mintic)
datos_CDD = traerDDevices()
#datos_CDD = traerDDevices(fecha_max_mintic,fecha_tope_mintic)

# if datos_CDD is None or datos_CDD.empty:
#     while (datos_CDD is None or datos_CDD.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S.%f"))):
#         fecha_max_mintic = (datetime.strptime(fecha_max_mintic, "%Y-%m-%d %H:%M:%S")+timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
#         fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, "%Y-%m-%d %H:%M:%S")+timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
#         datos_CDD = traerDDevices(fecha_max_mintic,fecha_tope_mintic)
# else:
#     pass

if datos_CDD.empty:
    datos_CDD=pd.DataFrame(columns=['registration_date', 
                                    'gestion.ptos_acceso', 
                                    'ip', 
                                    'site_id', 
                                    'mac',
                                    'status'])


if datos_CDD is None or datos_CDD.empty:
    datos_logs = datos_logs +"\n No trajo datos en este rango de fecha. "
    datos_CDD2=pd.DataFrame(columns=['site_id', 'status'])
else:
    drop_invalid_values(datos_CDD,['gestion.ptos_acceso','site_id','status'])
    
    datos_CDD  = datos_CDD.dropna(subset=['site_id'])

    datos_CDD = datos_CDD.drop(datos_CDD[(datos_CDD['site_id']=='')].index)
    datos_logs= datos_logs + "\n total reg.: " + str(datos_CDD["site_id"].size) + "    viene con (registration_date,gestion.ptos_acceso,ip,site_id,mac,status)"

    #datos_CDD2 = datos_CDD[["site_id","registration_date", "status", "gestion.ptos_acceso"]]
    datos_CDD2 = datos_CDD[["site_id", "status"]]    #datos_CDD2 = datos_CDD2[["site_id","registration_date", "status", "gestion.ptos_acceso"]].groupby(["site_id", "status", "gestion.ptos_acceso"]).agg(['max']).reset_index()
    datos_CDD2 = datos_CDD2[["site_id","status"]].groupby(["site_id"]).agg(['count']).reset_index()

    datos_CDD2.columns = datos_CDD2.columns.droplevel(1)       
    datos_logs = datos_logs +"\n se quitan los repetidos status=(offline, online, onboaring) total reg.: " + str(datos_CDD2["site_id"].size)
 
#print (datos_logs)    # ojoooooo  cuando se pase a Produccion se quita esta linea


# In[151]:


def disponibilidad(x):
    resp = 'No disponible'
    if 'online' in list(x):
        resp='Disponible'
    return resp

df_dispo = pd.concat([datos_CDD.groupby(['site_id'])['status'].apply(lambda x: disponibilidad(x)),
                      datos_CDD.groupby(['site_id']).registration_date.max('registration_date')],
                      axis=1)


df_dispo.rename(columns={'status':'disponibilidad','registration_date':'max_fecha'},inplace=True)

df_dispo.reset_index(inplace=True)


df_dispo= pd.merge(df_dispo,datos_CDD[['site_id','mac','ip','gestion.ptos_acceso','status']],on=['site_id'],how='inner')

df_dispo.rename(columns={'status':'status.macRed','gestion.ptos_acceso':'ap_group'},inplace=True)


# Se limpian datos mal formados de ap_group

# In[152]:


if not df_dispo.empty:
    df_dispo['ap_group'] = df_dispo['ap_group'].str.split("-", n = 1, expand = True)[0]
    df_dispo['ap_group'] = df_dispo['ap_group'].str.split("_", n = 1, expand = True)[0]
    df_dispo['ap_group'] = df_dispo['ap_group'].str.split(".", n = 1, expand = True)[0]
    df_dispo = df_dispo.drop(df_dispo[(df_dispo['ap_group']=='')].index)
    df_dispo = df_dispo.drop(df_dispo[(df_dispo['ap_group']=='PRUEBA OUTDOOR')].index)


# ## Lectura de estados
# * estos estados guardan el ultimo estado "offline" en que estuvo el centro 
# 

# In[153]:


total_docs = 10000
try:
    response = es.search(
        index= "edo_sitio-" + indice,
        body={
               "_source": ['site_id','fechahora','itsm_incident_id']
        },
        size=total_docs
    )    
    elastic_docs = response["hits"]["hits"]
    fields = {}
    for num, doc in enumerate(elastic_docs):
        source_data = doc["_source"]
        for key, val in source_data.items():
            try:
                fields[key] = np.append(fields[key], val)
            except KeyError:
                fields[key] = np.array([val])

    edo_sitio = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
    
    if edo_sitio.empty:
        edo_sitio = pd.DataFrame(columns=['site_id','fechahora','itsm_incident_id'])

except Exception as e:
#     print(e.message)
    edo_sitio = pd.DataFrame(columns=['site_id','fechahora','itsm_incident_id'])
    
edo_sitio["fechahora"] = fechaAhora
#edo_sitio["itsm_incident_id"]    = "0"
  


# In[154]:


print(datos_CDD2)
try:
    concat0 = pd.merge(datos_semilla, datos_CDD , on=['site_id'],how='inner')
except Exception as e:
#     print(e.message)
    concat0 = pd.DataFrame()

datos_logs = datos_logs +"\n se cruza con SEMILLA  total reg.: " + str(concat0["site_id"].size)


# Haciendo merge entre semilla e incidentes

# In[155]:


##esto es el deber ser, lo politicamente correcto 
if(concat0.empty):
    fecha_ejecucion = (datetime.strptime(fecha_ejecucion, "%Y-%m-%d"'T'"%H:%M:%S")+timedelta(minutes=60)).strftime("%Y-%m-%d"'T'"%H:%M:%S")[0:15] + '0:00'

    if fecha_ejecucion > str(now.strftime("%Y-%m-%d"'T'"%H:%M:%S"))[0:15] + '0:00':
        fecha_ejecucion = str(now.strftime("%Y-%m-%d"'T'"%H:%M:%S"))[0:15] + '0:00'

    response = es.index(
        index = indice_control,
        id = 'jerarquia_disponibilidad1',
        body = { 'jerarquia_disponibilidad1': 'jerarquia_disponibilidad1','gestion.fechaControl' : fecha_ejecucion}
    )
    print("actualizada fecha control de ejecucion:",fecha_ejecucion)
    exit()

result1=concat0
result2=concat0

result1['cantidad'] = 0
result2['cantidad'] = 0

result1 = result1.groupby(['site_id'])['site_id'].count().to_frame()
result2 = result2.groupby(['site_id', 'status'])['site_id'].count().to_frame()

result1 = result1.rename(columns={'site_id' :  'cantidad_x'})
result2 = result2.rename(columns={'site_id' :  'cantidad_y'})
result1 = result1.reset_index()
result2 = result2.reset_index()
#result1 = result1[['site_id',"cantidad"]].groupby(['site_id']).agg(['count'])
#result2 = result2[['site_id', 'status',"cantidad"]].groupby(['site_id', 'status']).agg(['count'])
#result1 = result1.columns.droplevel(1)
#result2 = result2.columns.droplevel(1)
#datos_CDD2[["site_id","status"]].groupby(["site_id"]).agg(['count']).reset_index()
result_completo = pd.merge(result1, result2,on=['site_id'],how='inner')
#result_completo["gestion.estadoCentro"] = result_completo.apply(lambda row: 'DESCONECTADOS' if( row.status == 'offline' and row.cantidad_x == row.cantidad_y) else 'CONECTADOS', axis=1)
result_completo["gestion.estadoCentro"] = result_completo.apply(lambda row: 'DESCONECTADOS' if( row.status  == 'offline' and row.cantidad_x == row.cantidad_y) else 'CONECTADOS', axis=1)

completo  = pd.merge(concat0, result_completo,on=['site_id'],how='inner')


#completo 
conectados = 0
desconectados = 0

bb = completo[(completo["gestion.estadoCentro"]=="DESCONECTADOS")].reset_index()
aa = completo[(completo["gestion.estadoCentro"]!="DESCONECTADOS")].reset_index()

conectados = aa["site_id"].size
desconectados = bb["site_id"].size
totales = desconectados + conectados
print (" Conectados: " , conectados , " desconectados: ", desconectados , " total: " , totales)
print(completo)
#i=0
#for index2 in todos_off:    
 #   print (i," reg. site_id: " )
 #   print (index2)
 #   i=i+1
    # df.loc[df['A'] > 2, 'B'] = new_val    ejemplo 

# if len(todos_off)>0:
#     print ("tienes registro .. todo_off")
# else:    
#     print ("NO  NOOOOO tienes registro .. todo_off")


# ### Realizando inserción en ultimo estado del sitio

# In[156]:


#use_these_keys = ['site_id','gestion.estadoCentro','fechahora', 'itsm_incident_id']
#def doc_generator(df,use_these_keys):
#    df_iter = df.iterrows()
#    for index, document in df_iter:
#        yield {
#                "_index": "edo_sitio-" + indice, 
#                "_id": f"{'Estado-'+str(document['site_id'])}",
#                "_source": filterKeys(document,use_these_keys),
#            }
#if len(todos_off)>0:        
#    salida = helpers.bulk(es, doc_generator(todos_off,use_these_keys))
#    print("Fecha: ", now,"- Ult.Estado de los Centros Desconectados  insertadas en indice edo_sitio:",salida[0])


# In[157]:


# es.indices.delete(index="edo_sitio-" + parametros.mintic_concat_index, ignore=[400, 404])
# aqui elimino todo el indice para poder ingresar los nuevos registros con los campos nuevos  GM


# ## 10.- insercion en el indice
# 

# In[158]:


# primero INSERTAR FINAL
completo.fillna({'fechahora':fechaAhora},inplace=True)
completo.fillna({'gestion.estadoCentro':'CONECTADOS'},inplace=True)
completo.fillna({'itsm_incident_id':0},inplace=True)

try:
    completo = completo.rename(columns={'id_Beneficiario' :  'gestion.estado.id_Beneficiario'
                                                                ,'nombreSede':'gestion.estado.nombreSede'
                                                                ,'site_id':'gestion.estado.site_id'
                                                                ,'nombre_departamento':'gestion.estado.dptoGestion'
                                                                ,'nombre_municipio':'gestion.estado.muniGestion'
                                                                ,'nombre_centro_pob':'gestion.estado.nombre_centro_pob'
                                                                ,'COD_ISO':'gestion.estado.COD_ISO'
                                                                ,'codDanesede':'gestion.estado.codDanesede'
                                                                ,'energiadesc':'gestion.estado.energiadesc'
                                                                ,'DDA':'gestion.estado.DDA'
                                                                ,'gestion.location':'gestion.estado.location'
                                                                ,'estadoInstalacion':'gestion.estado.estadoInstalacion'
                                                                ,'nombreInstitucionEd':'gestion.estado.nombreInstitucionEd'
                                                                ,'matricula':'gestion.estado.matricula'
                                                                ,'municipioPDET':'gestion.estado.municipioPDET'    
                                        ,'cod_servicio':'gestion.estado.cod_servicio'
                                        ,'codDaneMuni':'gestion.estado.codDaneMuni'
                                        ,'codCentroPoblado':'gestion.estado.codCentroPoblado'
                                        ,'codDaneInstitucionEdu':'gestion.estado.codDaneInstitucionEdu'
                                        ,'tipoSitio':'gestion.estado.tipoSitio'
                                        ,'detalleSitio':'gestion.estado.detalleSitio'
                                        ,'energia':'gestion.estado.energia'
                                        ,'region':'gestion.estado.region'
                                        ,'grupoDesc' :'gestion.estado.grupoDesc'
                                        })
                
    completo["gestion.fechaControl"] =  ahora_cdd
    completo["gestion.fecha"]=  completo["gestion.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    completo["gestion.hora"]=completo["gestion.fechaControl"].str.split(" ", n=1, expand=True)[1].str.split(":", n = 2, expand = True)[0]
    completo["gestion.minuto"]=completo["gestion.fechaControl"].str.split(" ", n=1, expand=True)[1].str.split(":", n = 2, expand = True)[1]
    completo["gestion.anyo"] =  completo["gestion.fecha"].str[0:4]
    completo["gestion.mes"]  =  completo["gestion.fecha"].str[5:7]
    completo["gestion.dia"]  =  completo["gestion.fecha"].str[8:10]
    completo["gestion.totales.cantDev"]  = totales  
    completo["gestion.totales.cantDevConectados"]  =  conectados
    completo["gestion.totales.cantDevdesConectados"]  =  desconectados
    
    print(completo)
except Exception as e:
    completo = pd.DataFrame(columns=['gestion.estado.nombreSede', 'gestion.estado.DDA','gestion.estado.estadoInstalacion', 'gestion.estado.COD_ISO',
   'gestion.estado.energia', 'gestion.estado.dptoGestion','gestion.estado.codCentroPoblado', 'gestion.estado.codDanesede',
   'gestion.estado.tipoSitio', 'gestion.estado.codDaneMuni','gestion.estado.nombre_centro_pob', 'gestion.estado.site_id',
   'gestion.estado.matricula', 'gestion.estado.energiadesc','gestion.estado.grupoDesc', 'gestion.estado.cod_servicio',
   'gestion.estado.region', 'gestion.estado.detalleSitio','gestion.estado.muniGestion', 'gestion.estado.id_Beneficiario',
   'gestion.estado.codDaneInstitucionEdu', 'gestion.estado.location','status', 'gestion.estadoCentro', 'itsm_incident_id', 'fechahora',
   'gestion.fechaControl', 'gestion.fecha', 'gestion.hora', 'gestion.anyo','gestion.mes', 'gestion.dia', 'gestion.totales.cantDev',
   'gestion.totales.cantDevConectados','gestion.totales.cantDevdesConectados'])
    print(e)


# In[159]:


df_dispo.rename(columns={'site_id':'gestion.estado.site_id',
                         'disponibilidad' : 'gestion.estado.disponibilidad',
                         'max_fecha':'gestion.estado.max_fecha'},
                inplace=True)

idx=completo[completo['gestion.estado.matricula']=='No aplica'].index

completo.loc[idx,'gestion.estado.matricula']=0

print(df_dispo.columns)
completo = pd.merge(completo, df_dispo,on=['gestion.estado.site_id','mac'],how='inner')
print(completo)


# In[160]:


print(completo)
if not completo.empty:
    completo["gestion.fechaControl"] =  completo['gestion.estado.max_fecha']
    completo["gestion.fecha"]=  completo["gestion.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    completo["gestion.hora"]=completo["gestion.fechaControl"].str.split(" ", n=1, expand=True)[1].str.split(":", n = 2, expand = True)[0]
    completo["gestion.minuto"]=completo["gestion.fechaControl"].str.split(" ", n=1, expand=True)[1].str.split(":", n = 2, expand = True)[1]
    completo["gestion.anyo"] =  completo["gestion.fecha"].str[0:4]
    completo["gestion.mes"]  =  completo["gestion.fecha"].str[5:7]
    completo["gestion.dia"]  =  completo["gestion.fecha"].str[8:10]


# In[161]:


# insertando los datos en el indice 

use_these_keys = ['gestion.estado.id_Beneficiario'
                  ,'gestion.estadoCentro'
                  ,'gestion.estado.nombreSede'
                  ,'gestion.estado.COD_ISO'
                  ,'gestion.estado.codDanesede'
                  ,'gestion.estado.nombre_centro_pob'
                  ,'gestion.estado.site_id'
                  ,'gestion.estado.dptoGestion'
                  ,'gestion.estado.energiadesc'
                  ,'gestion.estado.muniGestion'
                  ,'gestion.estado.location'  
                  ,'gestion.estado.DDA'
                  ,'gestion.estado.nombreInstitucionEd'
                  ,'gestion.estado.matricula'
                  ,'gestion.estado.municipioPDET'    
                  ,'gestion.fechaControl'
                  ,'gestion.fecha'
                  ,'gestion.anyo'
                  ,'gestion.mes'
                  ,'gestion.dia'
                  ,'gestion.hora'
                  ,'gestion.totales.cantDev'
                  ,'gestion.totales.cantDevConectados'
                  ,'gestion.totales.cantDevdesConectados'
                  ,'gestion.estado.disponibilidad'
                  ,'gestion.estado.max_fecha'
                  ,'gestion.estado.cod_servicio'
                  ,'gestion.estado.codDaneMuni'                  
                  ,'gestion.estado.codCentroPoblado'
                  ,'gestion.estado.codDaneInstitucionEdu'
                  ,'gestion.estado.tipoSitio'
                  ,'gestion.estado.detalleSitio'
                  ,'gestion.estado.energia'
                  ,'gestion.estado.region'
                  ,'gestion.estado.grupoDesc'
                  ,'gestion.estado.estadoInstalacion'
                  ,'@timestamp'
                  ,'mac'
                  ,'ip'
                  ,'ap_group'
                  ,'status.macRed'
                  ,'gestion.minuto']                         


# In[162]:


completo['@timestamp'] = now.isoformat()
def doc_generator2(df,use_these_keys):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
#                 "_id": f"{'Estado-'+str(+document['gestion.estado.id_Beneficiario']) + '-' + str(document['gestion.fechaControl'])}",
                "_id": f"{str(document['mac'])}",
                "_source": filterKeys(document,use_these_keys),
            }

salida = helpers.bulk(es, doc_generator2(completo,use_these_keys))
print(completo)
print("Fecha: ", now,"- Llamadas insertadas en indice principal:",salida[0])


# Actualizando fecha de control de ejecución

# In[163]:


fecha_ejecucion = (datetime.strptime(fecha_ejecucion, "%Y-%m-%d"'T'"%H:%M:%S")+timedelta(minutes=60)).strftime("%Y-%m-%d"'T'"%H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime("%Y-%m-%d"'T'"%H:%M:%S"))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime("%Y-%m-%d"'T'"%H:%M:%S"))[0:15] + '0:00'

response = es.index(
        index = indice_control,
        id = 'jerarquia_disponibilidad1',
        body = { 'jerarquia_disponibilidad1': 'jerarquia_disponibilidad1','gestion.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)


# In[ ]:




