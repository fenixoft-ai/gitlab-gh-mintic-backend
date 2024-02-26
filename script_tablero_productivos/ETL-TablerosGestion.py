#!/usr/bin/env python
# coding: utf-8

# In[1]:


from elasticsearch import Elasticsearch
from elasticsearch import helpers
from ssl import create_default_context
import requests
from getpass import getpass
import pandas as pd
import numpy as np
import json
from datetime import datetime
from datetime import timedelta
import parametros
import random
import re


# In[2]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=60, max_retries=3, retry_on_timeout=True
) 


# In[3]:


now = datetime.now()
format_ES = "%Y.%m.%d"
fecha_hoy = str(now.strftime(format_ES))
ahora_format = "%Y-%m-%d"'T'"%H:%M:%S"
ahora = str(now.strftime(ahora_format))


# In[4]:


indice = parametros.gestion_tableros_gestion_index
indice_control = parametros.tableros_mintic_control


# In[5]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# In[6]:


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


# In[7]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['gestion.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['gestion.location']=datos_semilla['gestion.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# In[8]:


datos_semilla = datos_semilla.rename(columns={'nombreSede':'gestion.nombreSede'
                                               , 'DDA':'gestion.DDA'
                                               , 'estadoInstalacion':'gestion.estadoInstalacion' 
                                               , 'COD_ISO':'gestion.COD_ISO'
                                               , 'energia':'gestion.energia'
                                               , 'nombre_departamento':'gestion.departamento'
                                               , 'codCentroPoblado':'gestion.codCentroPoblado'
                                               , 'nombreInstitucionEd':'gestion.nombreInstitucionEd'
                                               , 'codDanesede':'gestion.codDanesede'
                                               , 'tipoSitio':'gestion.tipoSitio'
                                               , 'codDaneMuni':'gestion.codDaneMuni'
                                               , 'nombre_centro_pob':'gestion.nombre_centro_pob'
                                               , 'site_id':'gestion.site_id'
                                               , 'matricula':'gestion.matricula'
                                               , 'energiadesc':'gestion.energiadesc'
                                               , 'grupoDesc':'gestion.grupoDesc'
                                               , 'cod_servicio':'gestion.cod_servicio'
                                               , 'region':'gestion.region'
                                               , 'detalleSitio':'gestion.detalleSitio'
                                               , 'nombre_municipio':'gestion.municipio'
                                               , 'id_Beneficiario':'gestion.id_Beneficiario'
                                               , 'codDaneInstitucionEdu':'gestion.codDaneInstitucionEdu'
                                           })        


# In[9]:


def traeSMInteracciones(fecha_max,fecha_tope):
    #print(fecha_max)
    #print(fecha_tope)
    try:
        total_docs = 10000
        response = es.search(
            index= "servicemanager-interacciones-tmp",
            body={
                "_source": ['callback_type',
    'problem_status',
    'status',
    'initial_impact',
    'close_time',
    'sysmodtime',
    'incident_id',
    'from_source',
    'opened_by',
    'source',
    'key_char',
    'total',
    'close_date',
    'resolution_code',
    'name',
    'variable2',
    'variable3',
                            'motivo_mintic',
                            'just_mintic',
                            'paradas',
                            'total_caso',
                            'total_claro'
                  , 'clr_bmcdatevent'
                  , 'severity'
                  , 'subcategory'
                  , 'clr_bmc_host'
                  , 'clr_txt_idbneficmtc'
                  , 'assignment'
                  , 'category'
                  , 'contact_name'
                  , 'clr_bmc_location'
                  , 'number'
                  , '@version'
                  , 'clr_txt_company_code'
                  , 'open_time'
                  , 'product_type'
                  , 'resolution'
                  , 'resolved_time'
                  , '@timestamp'
                            ]
                ,"query": {
                  "range": {
                    "open_time": {
                        "gte": fecha_max,
                        "lt": fecha_tope
                    }
                  }
              }
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

        datos_SM_interactions = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
        
        return datos_SM_interactions
    
    except Exception as e:
        print("Error")
        print(e)
        return pd.DataFrame()


# In[10]:


def traeSMIncidentes(fecha_max,fecha_tope):
    try:
        
        response = es.search(
            index= "servicemanager-incidentes-tmp",
            body={
                "_source": ['callback_type',
    'problem_status',
    'status',
    'initial_impact',
    'close_time',
    'sysmodtime',
    'incident_id',
    'from_source',
    'opened_by',
    'source',
    'key_char',
    'total',
    'close_date',
    'resolution_code',
    'name',
    'variable2',
    'variable3',
                            'motivo_mintic',
                            'just_mintic',
                            'paradas',
                            'total_caso',
                            'total_claro'
                  , 'clr_bmcdatevent'
                  , 'severity'
                  , 'subcategory'
                  , 'clr_bmc_host'
                  , 'clr_txt_idbneficmtc'
                  , 'assignment'
                  , 'category'
                  , 'contact_name'
                  , 'clr_bmc_location'
                  , 'number'
                  , '@version'
                  , 'clr_txt_company_code'
                  , 'open_time'
                  , 'product_type'
                  , 'resolution'
                  , 'resolved_time'
                  , '@timestamp']
                ,"query": {
                    "range": {
                      "open_time": {
                        "gte": fecha_max,
                        "lt": fecha_tope
                      }
                    }
                }
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
                    
        datos_SM_incidents = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
        return datos_SM_incidents
        
    except Exception as e:
        print("Error")
        print(e)
        return pd.DataFrame()


# In[11]:


datos_SM_inci = traeSMIncidentes("2021-04-01T00:00:00",ahora)


# In[ ]:





# In[12]:


datos_SM_inte = traeSMInteracciones("2021-04-01T00:00:00",ahora)


# In[ ]:





# In[47]:


try:
    if (not datos_SM_inte.empty and not datos_SM_inci.empty):
        datos_SM = datos_SM_inte.append(datos_SM_inci)
    elif (not datos_SM_inte.empty and datos_SM_inci.empty):
        datos_SM = datos_SM_inte
    elif (datos_SM_inte.empty and not datos_SM_inci.empty):
        datos_SM = datos_SM_inci
    else:
        datos_SM = pd.DataFrame()
    
    if not datos_SM.empty:
        datos_SM['resolved_time'] = datos_SM.apply(lambda row: row.resolved_time if( row.resolved_time != 0 and row.resolved_time != '0'  and row.resolved_time != '') else None, axis=1)
        datos_SM['gestion.id'] = datos_SM.apply(lambda row: row.number if( row.number != None and row.number != 'null' and row.number != '') else row.incident_id, axis=1)
        #datos_SM['incident_id'] = datos_SM.apply(lambda row: row.incident_id if( row.incident_id is not None and row.incident_id != "") else row.key_char if( row.key_char is not None and row.key_char != "") else row.number, axis=1)
        datos_SM['gestion.tiempoRespuesta'] = datos_SM.apply(lambda row: (datetime.strptime(row.resolved_time, "%Y-%m-%d"'T'"%H:%M:%S") - datetime.strptime(row.open_time, "%Y-%m-%d"'T'"%H:%M:%S")).total_seconds() % 3600 if( row.resolved_time is not None) else 0, axis=1)    
        datos_SM['gestion.gravedad'] = datos_SM['severity'].replace(['1','2','3'],['Alto','Medio','Bajo'])
        datos_SM['close_time'] = datos_SM.apply(lambda row: row.close_time if( row.close_time != 0 and row.close_time != '0' and row.close_time != '') else None, axis=1)
        datos_SM['initial_impact'] = datos_SM.apply(lambda row: row.initial_impact if( row.initial_impact != None and row.initial_impact != 'null' and row.initial_impact != '') else 0, axis=1)
        #datos_SM['gestion.duracion'] = datos_SM.apply(lambda row: (datetime.strptime(row.resolved_time, "%Y-%m-%d"'T'"%H:%M:%S") - datetime.strptime(row.open_time, "%Y-%m-%d"'T'"%H:%M:%S")).total_seconds() % 3600 if( row.resolved_time is not None) else 0, axis=1)
        
        datos_SM = datos_SM.rename(columns={'category':'gestion.categoria'
                                            ,'problem_status':'gestion.problem_status'
                                            ,'status':'gestion.status'
                                                #, 'severity':'gestion.gravedad'
                                                ,'product_type' : 'gestion.detallesTicket'
                                                ,'contact_name':'gestion.usuarioTicket'
                                                ,'assignment':'gestion.responsable'
                                                ,'clr_bmc_location':'gestion.site_id'
                                                ,'clr_bmc_host':'gestion.IP'
                                                                      , 'opened_by':'gestion.opened_by',
                                            'motivo_mintic':'gestion.motivo_mintic',
                            'just_mintic':'gestion.just_mintic'
                                            , 'total_caso':'gestion.total_caso'
                                            , 'total_claro':'gestion.duracion'
                                            , 'paradas':'gestion.paradas'
                                            , 'number':'gestion.number'
                                            ,'sysmodtime':'gestion.sysmodtime'
                                                                      , 'subcategory':'gestion.subcategoria'
                                                                      , '@timestamp':'gestion.@timestamp'
                                                                      , 'open_time':'gestion.fechaApertura'
                                                                      , 'clr_txt_idbneficmtc':'gestion.id_Beneficiario'
                                                                      , 'category':'gestion.categoria'
                                                                      , 'close_time':'gestion.fechaCierre'
                                                                      , 'source':'gestion.canal'
                                                                      , 'incident_id':'gestion.numeroTicket'
                                                                      , 'total':'gestion.total'
                                                                      , 'close_date':'gestion.close_date'
                                                                      , 'resolution_code':'gestion.coderesolucion'
                                                                      , 'resolved_time':'gestion.timeresolution'
                                                                      , 'callback_type':'gestion.callback_type'
                                                                      , 'initial_impact':'gestion.initial_impact'
                                                                      , 'name':'gestion.name'
                                                                      , 'variable2':'gestion.variable2'
                                                                      , '@version':'gestion.version'
                                                                      #, 'contact_name':'gestion.contact_name'                                                                  
                                                                      , 'variable3':'gestion.variable3'})         
        datos_SM = datos_SM.dropna(subset=['gestion.id_Beneficiario'])
        datos_SM['gestion.id_Beneficiario'] = datos_SM['gestion.id_Beneficiario'].str.strip()
        
        #PARA VERIFICAR NO CRUCES BENFICIARIO
        print("VERIFICAR NO CRUCES BENFICIARIO")
        sub_null = pd.DataFrame()
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"] == ""]
        if (not sub_null.empty):
            print("Caso 1")
            print(sub_null.shape[0])
        
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"] == None]
        if (not sub_null.empty):
            print("Caso 2")
            print(sub_null.shape[0])
        
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"] == "null"]
        if (not sub_null.empty):
            print("Caso 3")
            print(sub_null.shape[0])
            
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.contains(',')]
        if (not sub_null.empty):
            print("Caso 4")
            print(sub_null.shape[0])
        
        #sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.isnumeric() == False]
        #if (not sub_null.empty):
        #    print("Caso 7")
        #    print(sub_null.shape[0])
        #    sub_null = sub_null[sub_null["gestion.id_Beneficiario"] != "null"]
        #    sub_null = sub_null[sub_null["gestion.id_Beneficiario"] != ""]
        #    sub_null = sub_null[sub_null['gestion.id_Beneficiario'].str.contains(' ') == False]
            #print(sub_null_1.shape[0])
            
        #    sub_null = sub_null[sub_null['gestion.id_Beneficiario'].str.contains('\t') == False]
        #    sub_null = sub_null[sub_null['gestion.id_Beneficiario'].str.contains('Z') == False]
        #    sub_null = sub_null[sub_null['gestion.id_Beneficiario'].str.contains('E') == False]
             
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.contains('E')]
        if (not sub_null.empty):
            print("Caso 5")
            print(sub_null.shape[0])
            
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.contains('Z')]
        if (not sub_null.empty):
            print("Caso 6")
            print(sub_null.shape[0])
            
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.contains('\t')]
        if (not sub_null.empty):
            print("Caso 8")
            print(sub_null.shape[0])
            
        sub_null = datos_SM[datos_SM["gestion.id_Beneficiario"].str.contains(' ')]
        if (not sub_null.empty):
            print("Caso 9")
            print(sub_null.shape[0])
            
        ###################################
        
        #datos_SM = datos_SM[datos_SM["gestion.id_Beneficiario"] != ""]
        #datos_SM = datos_SM[datos_SM['gestion.id_Beneficiario'].str.contains(',') == False]
        #datos_SM = datos_SM[datos_SM['gestion.id_Beneficiario'].str.contains('E') == False]
        #datos_SM = datos_SM[datos_SM['gestion.id_Beneficiario'].str.contains('Z') == False]
        #datos_SM = datos_SM[datos_SM['gestion.id_Beneficiario'].str.contains(' ') == False]
        #datos_SM = datos_SM[datos_SM['gestion.id_Beneficiario'].str.contains('\t') == False]
        #datos_SM = datos_SM[datos_SM["gestion.id_Beneficiario"] != "null"]
        datos_SM = datos_SM[datos_SM["gestion.id_Beneficiario"].str.isnumeric() == True]
        
        datos_SM['gestion.id_Beneficiario'] = datos_SM['gestion.id_Beneficiario'].astype(int)
        datos_SM['gestion.canal'] = datos_SM['gestion.canal'].replace(['5','6'],['Correo Electrónico','Teléfono'])

except Exception as e:
    print("Error 2 Aqui")
    print(e)


# In[ ]:





# In[19]:


use_these_keys = [ 'gestion.nombreSede'
                    , 'gestion.DDA'
                    , 'gestion.estadoInstalacion' 
                    , 'gestion.COD_ISO'
                    , 'gestion.energia'
                    , 'gestion.departamento'
                    , 'gestion.codCentroPoblado'
                    , 'gestion.nombreInstitucionEd'
                    , 'gestion.codDanesede'
                    , 'gestion.tipoSitio'
                    , 'gestion.codDaneMuni'
                    , 'gestion.nombre_centro_pob'
                    , 'gestion.site_id'
                    , 'gestion.matricula'
                    , 'gestion.energiadesc'
                    , 'gestion.grupoDesc'
                    , 'gestion.cod_servicio'
                    , 'gestion.region'
                    , 'gestion.detalleSitio'
                    , 'gestion.municipio'
                    , 'gestion.id_Beneficiario'
                    , 'gestion.location'
                    , 'gestion.codDaneInstitucionEdu'
                    , 'gestion.categoria'
                    , 'gestion.gravedad'
                    , 'gestion.detallesTicket'
                    , 'gestion.usuarioTicket'
                    , 'gestion.responsable'
                  ,'gestion.just_mintic'
                  ,'gestion.motivo_mintic'
                  , 'gestion.IP'
                  , 'gestion.duracion'
                  , 'gestion.opened_by'
                  , 'gestion.tiempoRespuesta'
                  ,'gestion.problem_status'
                  ,'gestion.status'
                  ,'gestion.number'
                  #, 'gestion.category'
                    #, 'gestion.opened_by'
                    , 'gestion.subcategoria'
                    , 'gestion.@timestamp'
                    , 'gestion.fechaApertura'
                    , 'gestion.id_Beneficiario'
                    , 'gestion.fechaCierre'
                    , 'gestion.canal'
                    , 'gestion.numeroTicket'
                    , 'gestion.total'
                    , 'gestion.close_date'
                    , 'gestion.coderesolucion'
                    , 'gestion.timeresolution'                                                                                       
                    , 'gestion.callback_type'
                    , 'gestion.initial_impact'
                    , 'gestion.name'
                    , 'gestion.variable2'
                    , 'gestion.version'
                    #, 'gestion.contact_name'                                                                  
                    , 'gestion.variable3'
                    , 'gestion.fecha'
                    , 'gestion.anyo'
                    , 'gestion.mes'
                    , 'gestion.dia'
                  ,'gestion.total_caso'
                  ,'gestion.paradas'
                  ,'gestion.sysmodtime'
                  ]
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": "prod-gestion-tableros-gestion", 
                #"_id": f"{str(document['gestion.id_Beneficiario']) + '-' + str(document['gestion.fechaApertura'])+ str(random.randrange(1000))}",
                "_id": f"{str(document['gestion.id'])}",
                "_source": filterKeys(document),
            }


# In[20]:


def doc_ordenamiento(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": 1,
                "_source": filterKeys(document),
            }


# In[21]:


concat = pd.DataFrame()
try:
    if not datos_SM.empty:
        
        cant_1 = len(datos_SM)
        concat = pd.merge(datos_SM,datos_semilla, on=['gestion.id_Beneficiario'],how='inner')
        cant_2 = len(concat)
        
        concat_outa = pd.DataFrame()
        if (cant_1 != cant_2):
            concat_outa = pd.merge(datos_SM,datos_semilla, on=['gestion.id_Beneficiario'],how='left')
            
            #PARA VERIFICAR NO CRUCES SEMILLA
            print("PARA VERIFICAR NO CRUCES SEMILLA")
            if (not concat_outa.empty):
                concat_outa = concat_outa.fillna('')
                
                sub_null = pd.DataFrame()
                sub_null = concat_outa[concat_outa["gestion.codDaneMuni"] == ""]
                if (not sub_null.empty):
                    print("Caso SEMILLA")
                    print(sub_null.shape[0])
            ############################
            
        concat['gestion.fechaApertura'] = concat['gestion.fechaApertura'].str.replace("T"," ")
        concat['gestion.fechaApertura'] = concat['gestion.fechaApertura'].str.slice(stop=19)
        #concat["gestion.fecha"] = concat["gestion.fechaApertura"].str[0:10]
        concat["gestion.fecha"] = concat["gestion.fechaApertura"]
        concat["gestion.anyo"] = concat["gestion.fechaApertura"].str[0:4]
        concat["gestion.mes"] = concat["gestion.fechaApertura"].str[5:7]
        concat["gestion.dia"] = concat["gestion.fechaApertura"].str[8:10]
        concat['gestion.fechaApertura'] = concat['gestion.fechaApertura'].str[0:10] + " 00:00:00"
        concat['@timestamp'] = now.isoformat()
        concat = concat.fillna('null')
        
        concat = concat.rename(columns={'gestion.site_id_x':'gestion.site_id'})
        concat = concat.rename(columns={'gestion.category':'gestion.categoria'})
        salida = helpers.bulk(es, doc_generator(concat))
        print("Fecha: ", now,"- Interacciones insertadas en indice principal:",salida[0])
except Exception as e:
    print(e, "\nNinguna Interacción insertada en indice principal:")

    


# In[ ]:




