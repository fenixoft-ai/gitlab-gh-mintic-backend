#!/usr/bin/env python
# coding: utf-8

# ## ¿Qué hace este script?
# 
# #### Para cada AP:
# * usuarios.usuariosConectados(corresponde al total de conexiones), usuarios.sesiones_Usuarios (Corresponde al total de usuarios)
# * Información detallada de los dispositivos conectados en el rango de fecha procesado: usuarios.dispositivo.mac, usuarios.dispositivo.tipo, usuarios.dispositivo.marca, usuarios.dispositivo.tecnologia, usuarios.dispositivo.sisOperativo.
# 
# #### Para cada sitio: 
# * Totales por características de dispositivos: usuarios.sistemaOperativoUsuarios, usuarios.tipoDispositivoUsuarios, usuarios.marcaTerminal, usuarios.tecnologiaTerminal, usuarios.totales.dispositivos (Este ultimo guarda el total)
# * usuarios.usoServicioInternetSitio es la suma en Gb consumido discriminado por usuarios.detallesTecnologiasTerminales (Bandas 2.4 o 5 GHz)
# * usuarios.conteoDispositivos el cual indica el total de dispositivos conectados (Es lo mismo que total de conexiones)
# * usuarios.usuariosNuevos (Esta tenía antes Nuevos y Recurrentes, pero ahora solo tiene Nuevos por solicitud de BI)
# * usuarios.totales.usuariosNuevos, tiene el total de usuarios nuevos (el script por el momento solo totaliza nuevos siempre y cuando no se corra historicos)

# In[103]:


from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import parametros
import random
import re
import time


# ## Conectando a ElasticSearch

# La ultima línea se utiliza para garantizar la ejecución de la consulta
# * timeout es el tiempo para cada ejecución
# * max_retries el número de intentos si la conexión falla
# * retry_on_timeout para activar los reitentos

# In[104]:


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

# In[105]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### nombre de indice donde se insertará

# In[106]:


indice = parametros.usuarios_tablero11_index 
indice_control = parametros.tableros_mintic_control


# ### Funcion para construir JSON compatible con ElasticSearch

# In[107]:


def filterKeys(document, use_these_keys):
    return {key: document.get(key) for key in use_these_keys }


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[108]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
           "_source": ["tablero11.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia-tablero11"
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
        fecha_ejecucion = doc["_source"]['tablero11.fechaControl']
except Exception as e:
    print(e)
    response["hits"]["hits"] = []
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-05-14T00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ### leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud,COD_ISO, id_Beneficiario).

# In[109]:


t1=time.time()
total_docs = 10000
try:
    response = es.search(
        index= parametros.semilla_inventario_index,
        body={
               "_source": ['site_id','nombre_municipio', 'nombre_departamento', 'nombre_centro_pob', 'nombreSede' 
                           , 'energiadesc', 'latitud', 'longitud', 'COD_ISO','id_Beneficiario']
        },
        size=total_docs
    )
    #print(es.info())
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])

#     datos_semilla = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) #pd.DataFrame(fields)
    
    datos_semilla = pd.DataFrame([x["_source"] for x in elastic_docs])
    
except:
    exit()
    
t2=time.time()


# In[110]:


datos_semilla['site_id'] = datos_semilla['site_id'].str.strip()


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location
# * Se renombran los campos de semilla

# In[111]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))
datos_semilla['latitud'] = datos_semilla['latitud'].apply(get_location)
datos_semilla['longitud'] = datos_semilla['longitud'].apply(get_location)
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['usuarios.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.location']=datos_semilla['usuarios.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# In[112]:


datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.localidad'
                                            , 'nombreSede' : 'usuarios.nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.codISO'
                                            , 'id_Beneficiario' : 'usuarios.idBeneficiario'})


# Se descartan los registros que tengan la latitud y longitud vacía o no valida

# In[113]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.location"]=='')].index)


# ### leyendo indice cambium-devicedevices

# De esta formas se asocia las MAC de dispositivos de red INDOOR y OUTDOOR
# * site_id para cruzar con las misma llave de semilla.
# * datos del dispositivo: mac, status, ip.
# * ap_group para identificar si la conexión es indoor/outdoor

# In[114]:


t1=time.time()
total_docs = 30000
try:
    response = es.search(
        index= parametros.cambium_d_d_index,
        body={
                    "_source": ["site_id","mac","status","ip","ap_group"]
                  , "query": {
                    "match_all": {}
                  }
        },
        size=total_docs
    )
    #print(es.info())
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])

#     datos_dev = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) #pd.DataFrame(fields)
    
    datos_dev = pd.DataFrame([x["_source"] for x in elastic_docs])
    
    
except:
    exit()
    
t2=time.time()


# In[115]:


datos_dev['site_id'] = datos_dev['site_id'].str.strip()


# Se descartan registros con site_id vacios ya que no cruzarán en el merge y se limpian los NaN del dataframe.

# In[116]:


datos_dev.dropna(subset=['site_id'], inplace=True)
datos_dev.fillna('', inplace=True)
datos_dev = datos_dev.drop(datos_dev[(datos_dev['site_id']=='')].index)


# In[117]:


datos_dev['ap_group'] = datos_dev['ap_group'].str.split("-", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split("_", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split(".", n = 1, expand = True)[0]
datos_dev = datos_dev.drop(datos_dev[(datos_dev['ap_group']=='')].index)


# Se toman solo los datos unicos con mac.

# In[118]:


#datos_dev.drop_duplicates(subset=['site_id','mac'],inplace=True)
datos_dev = datos_dev.drop_duplicates('mac')


# Se cambia el nombre a la mac del dispositivo de red para no confundir con la de dispositivos de usuario 

# In[119]:


datos_dev= datos_dev.rename(columns={'mac' : 'usuarios.macRed','ap_group' : 'usuarios.apGroup'})


# ## Lectura de datos ohmyfi-detalleconexiones

# Los datos que se toman son:
# * fechahora (de cada conexión). fecha_control es lo mismo pero con el intervalo de 10 minutos
# * Información del centro: lugar, lugar_cod.
# * Información del dispositivo de usuario: mac_usuario, dispositivo, sistema_operativo.
# * Información de usuario: tipodoc y documento

# In[120]:


def trae_conexiones(fecha_ini,fecha_fin):
    total_docs = 5000000
    response = es.search(
        index= parametros.ohmyfi_d_c_index,
        body={
                "_source": ["fechahora","fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo"
                            ,"sistema_operativo",'tipodoc','documento']
                , "query": {
                  "range": {
                    "fechahora": {
                      "gte": fecha_ini,
                      "lt": fecha_fin
                    }
                  }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])

#     return pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) 

    return pd.DataFrame([x["_source"] for x in elastic_docs])


# ### Lanzando ejecución de consulta

# * Se calcula rango en base a la fecha de control. Para este caso es de 10 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.
# 

# In[121]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_det_conex = trae_conexiones(fecha_max_mintic,fecha_tope_mintic)

if datos_det_conex is None or datos_det_conex.empty:
    while (datos_det_conex is None or datos_det_conex.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        datos_det_conex = trae_conexiones(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[122]:


datos_det_conex['lugar_cod'] = datos_det_conex['lugar_cod'].str.strip()


# In[123]:


datos_det_conex['fecha'] = datos_det_conex['fecha_control'].str.split(" ", n = 1, expand = True)[0]
datos_det_conex.drop_duplicates(subset=["fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo","sistema_operativo",'tipodoc','documento'],inplace=True)


# In[124]:


datos_det_conex = datos_det_conex.rename(columns={'lugar_cod' : 'site_id'
                                                   ,'fechahora':'usuarios.fechaConexionUsuarios'
                                                   ,'dispositivo': 'usuarios.tipoDispositivoUsuarios'
                                                   , 'sistema_operativo': 'usuarios.sistemaOperativoUsuarios'})


# Se corrigen valores errados de site_id en detalle conexiones

# In[125]:


def traeSesiones(fecha_max,fecha_tope):
    total_docs = 500000
    response = es.search(
        index= parametros.ohmyfi_consumos_index,
        body={
                  "_source": ["lugar_cod", "tiempo_sesion_minutos","mac_ap","fecha_inicio"]
                , "query": {
                      "range": {
                            "fecha_inicio": {
                            "gte": fecha_max,
                            "lt": fecha_tope
                            }
                        }
                  }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])

#     return pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) 

    return pd.DataFrame([x["_source"] for x in elastic_docs])


# In[126]:


def traeRadio(fecha_max,fecha_tope):
    ttotal_docs = 100000
    response = es.search(
        index= 'all-'+parametros.cambium_d_c_index,
        body={
            "_source": ['mac', 'ap_mac', 'radio.band', 'radio.rx_bytes', 'radio.tx_bytes'
                  ,'fecha_control']
              , "query": {
                  "range": {
                    "fecha_control": {
                      "gte": fecha_max,
                      "lt": fecha_tope
                      #"gte": "2021-05-26 15:00:00",
                      #"lt": "2021-05-26 15:10:00"  
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

    return pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))


# ### Se lee la información de cambium device client

# Esta lectura se usa para identificar: 
# * Lecturas de consumo por dispositivo (radios rx y tx)
# * Fabricante del dispositivo
# * la mac del ap que luego se usa para identificar el ap group(Indoor/Outdoor)

# In[127]:


t1=time.time()

total_docs = 30000000
response = es.search(
    index= parametros.cambium_d_c_index,
    body={
            "_source": ["mac","ap_mac", 'manufacturer','name',"radio.band",'radio.rx_bytes','radio.tx_bytes'],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"mac"
                    }
                  }
                  ]
                }
              }
    },
    size=total_docs
)
elastic_docs = response["hits"]["hits"]
# fields = {}
# for num, doc in enumerate(elastic_docs):
#     source_data = doc["_source"]
#     for key, val in source_data.items():
#         try:
#             fields[key] = np.append(fields[key], val)
#         except KeyError:
#             fields[key] = np.array([val])

# datos_dev_clients = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))

datos_dev_clients = pd.DataFrame([x["_source"] for x in elastic_docs])

t2=time.time()


# * se cruza por mac_usuario
# * Los merge usan left para evitar perdida de datos en cruce con cambium-devicedevices. Aquellos datos que no cruzan se les marca como no identificados. En condiciones ideales, no debería presentarse ausencia de información
# * Solo se toma lo que cruza con INNER
# * se cambia el fabricante [Local MAC]
# * se cambian lo nulos

# In[128]:


datos_dev_clients.drop_duplicates(inplace=True)


# In[129]:


try:
    datos_dev_clients.drop_duplicates(inplace=True)
    datos_dev_clients = datos_dev_clients.rename(columns={'ap_mac' : 'usuarios.macRed','mac' : 'mac_usuario'})
    datos_dev_clients['manufacturer'] = datos_dev_clients['manufacturer'].replace("[Local MAC]", "No identificado")
    datos_dev_clients.fillna({'manufacturer':'No identificado'},inplace=True)
    datos_det_conex = pd.merge(datos_det_conex,  datos_dev_clients, on='mac_usuario',how='left')
    datos_det_conex = pd.merge(datos_det_conex,datos_dev[['usuarios.macRed','usuarios.apGroup']],on='usuarios.macRed', how='left')
    datos_det_conex.fillna({'usuarios.tipoDispositivoUsuarios':'No identificado'
                       ,'usuarios.sistemaOperativoUsuarios':'No identificado'
                       ,'manufacturer':'No identificado'
                       ,'radio.band':'No identificado'
                       },inplace=True)
except:
    pass


# ## Calculando totales por dispositivos

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán

# In[130]:



use_these_keys = [ 'usuarios.tablero11.siteID'
                  , 'usuarios.tablero11.nomCentroDigital'
                  , 'usuarios.tablero11.nombreDepartamento'
                  , 'usuarios.tablero11.nombreMunicipio'
                  , 'usuarios.tablero11.idBeneficiario'
                  , 'usuarios.tablero11.location'
                  , 'usuarios.tablero11.fechaControl'
                  , 'usuarios.tablero11.fecha'
                  , 'usuarios.tablero11.anyo'
                  , 'usuarios.tablero11.mes'
                  , '@timestamp']

def doc_generator(df,use_these_keys):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{ 'totalesDispositivos-' + str(document['usuarios.tablero11siteID']) + '-' + str(document['usuarios.fechaControl'])+ '-' + str(document['usuarios.tablero11.fechaControl'])+'-'+str(random.randrange(10000))}",
                "_source": filterKeys(document,use_these_keys),
            }


# Se agrupa por:
# * Sistema operativo
# * Tipo dispositivo
# * Marca del terminal
# * Tecnología terminal
# 
# Se cuentan las mac de usuario para cada site id y fecha control

# In[131]:


def cambioVariable(df):
    df = df.rename(columns={'siteID':'usuarios.tablero11.siteID'
                        ,'usuarios.siteID': 'usuarios.tablero11.siteID'
                        ,'usuarios.tablero11siteID':'usuarios.tablero11.siteID'
                        ,'fechaControl':'usuarios.tablero11.fechaControl'
                        ,'usuarios.fechaControl':'usuarios.tablero11.fechaControl'
                        ,'usuarios.tablero11fechaControl': 'usuarios.tablero11.fechaControl'
                        ,'nomCentroDigital':'usuarios.tablero11.nomCentroDigital'
                        ,'usuarios.nomCentroDigital':'usuarios.tablero11.nomCentroDigital'
                        ,'usuarios.tablero11nomCentroDigital':'usuarios.tablero11.nomCentroDigital'
                        ,'localidad':'usuarios.tablero11.localidad'
                        ,'usuarios.localidad':'usuarios.tablero11.localidad'
                        ,'usuarios.tablero11localidad':'usuarios.tablero11.localidad'
                        ,'nombreDepartamento':'usuarios.tablero11.nombreDepartamento'
                        ,'usuarios.nombreDepartamento':'usuarios.tablero11.nombreDepartamento'
                        ,'usuarios.tablero11nombreDepartamento':'usuarios.tablero11.nombreDepartamento'
                        ,'nombreMunicipio':'usuarios.tablero11.nombreMunicipio'
                        ,'usuarios.nombreMunicipio':'usuarios.tablero11.nombreMunicipio'
                        ,'usuarios.tablero11nombreMunicipio':'usuarios.tablero11.nombreMunicipio'
                        ,'idBeneficiario':'usuarios.tablero11.idBeneficiario'
                        ,'usuarios.idBeneficiario':'usuarios.tablero11.idBeneficiario'
                        ,'usuarios.tablero11idBeneficiario':'usuarios.tablero11.idBeneficiario'
                        ,'apGroup':'usuarios.tablero11.apGroup'
                        ,'usuarios.apGroup':'usuarios.tablero11.apGroup'
                        ,'usuarios.tablero11apGroup':'usuarios.tablero11.apGroup'
                        ,'location':'usuarios.tablero11.location'
                        ,'usuarios.location':'usuarios.tablero11.location'
                        ,'usuarios.tablero11location':'usuarios.tablero11.location'
                        ,'fecha':'usuarios.tablero11.fecha'
                        ,'usuarios.fecha':'usuarios.tablero11.fecha'
                        ,'usuarios.tablero11fecha':'usuarios.tablero11.fecha'
                        ,'anyo':'usuarios.tablero11.anyo'
                        ,'usuarios.anyo':'usuarios.tablero11.anyo'
                        ,'usuarios.tablero11anyo':'usuarios.tablero11.anyo'
                        ,'mes':'usuarios.tablero11.mes'
                        ,'usuarios.mes':'usuarios.tablero11.mes'
                        ,'usuarios.tablero11mes':'usuarios.tablero11.mes'
                        ,'macRed':'usuarios.tablero11.macRed'
                        ,'usuarios.macRed':'usuarios.tablero11.macRed'
                        ,'usuarios.tablero11macRed':'usuarios.tablero11.macRed'
                        ,'usuariosConectados':'usuarios.tablero11.usuariosConectados'
                        ,'usuarios.usuariosConectados':'usuarios.tablero11.usuariosConectados'
                        ,'usuarios.tablero11usuariosConectados':'usuarios.tablero11.usuariosConectados'
                        ,'sesiones_Usuarios':'usuarios.tablero11.sesiones_Usuarios'
                        ,'usuarios.sesiones_Usuarios':'usuarios.tablero11.sesiones_Usuarios'
                        ,'usuarios.tablero11sesiones_Usuarios':'usuarios.tablero11.sesiones_Usuarios'
                        ,'consumoUsuarios':'usuarios.tablero11.consumoUsuarios'
                        ,'usuarios.consumoUsuarios':'usuarios.tablero11.consumoUsuarios'
                        ,'usuarios.tablero11consumoUsuarios':'usuarios.tablero11.consumoUsuarios'
                        ,'consumoUsuariosDescarga_aux':'usuarios.tablero11.consumoUsuariosDescarga_aux'
                        ,'usuarios.consumoUsuariosDescarga_aux':'usuarios.tablero11.consumoUsuariosDescarga_aux'
                        ,'usuarios.tablero11consumoUsuariosDescarga_aux':'usuarios.tablero11.consumoUsuariosDescarga_aux'
                        ,'consumoUsuariosCarga_aux':'usuarios.tablero11.consumoUsuariosCarga_aux'
                        ,'usuarios.consumoUsuariosCarga_aux':'usuarios.tablero11.consumoUsuariosCarga_aux'
                        ,'usuarios.tablero11consumoUsuariosCarga_aux':'usuarios.tablero11.consumoUsuariosCarga_aux'
                        ,'consumoUsuariosDescarga':'usuarios.tablero11.consumoUsuariosDescarga'
                        ,'usuarios.consumoUsuariosDescarga':'usuarios.tablero11.consumoUsuariosDescarga'
                        ,'usuarios.tablero11consumoUsuariosDescarga':'usuarios.tablero11.consumoUsuariosDescarga'
                        ,'consumoUsuariosCarga': 'usuarios.tablero11.consumoUsuariosCarga'
                        ,'usuarios.tablero11consumoUsuariosCarga': 'usuarios.tablero11.consumoUsuariosCarga'
                       }
              )
    
    df_vacio = pd.DataFrame(index=df.index)
    
    col_select= ['usuarios.tablero11.siteID'
                 ,'usuarios.tablero11.nomCentroDigital'
                 , 'usuarios.tablero11.localidad'
                 , 'usuarios.tablero11.nombreDepartamento'
                 , 'usuarios.tablero11.nombreMunicipio'
                 , 'usuarios.tablero11.idBeneficiario'
                 , 'usuarios.tablero11.location'
                 , 'usuarios.tablero11.sesiones_Usuarios'
                 , 'usuarios.tablero11.usuariosConectados'
                 , 'usuarios.tablero11.consumoUsuarios'
                 , 'usuarios.tablero11.consumoUsuariosDescarga_aux'
                 , 'usuarios.tablero11.consumoUsuariosCarga_aux'
                 , 'usuarios.tablero11.consumoUsuariosDescarga'
                 , 'usuarios.tablero11.consumoUsuariosCarga'
                 , 'usuarios.tablero11.apGroup'
                 , 'usuarios.tablero11.fechaControl'
                 , 'usuarios.tablero11.fecha'
                 , 'usuarios.tablero11.anyo'
                 , 'usuarios.tablero11.mes'
                 , 'usuarios.tablero11.macRed'
                 , '@timestamp']
    
    for c in col_select:
        try:
            df_vacio=pd.concat([df_vacio,df[c]],axis=1)
        except:
            pass
    
#     print(df.columns)
    
    df_vacio=df_vacio.iloc[:,~df_vacio.columns.duplicated()]

    return df_vacio
    
#     df= df[['usuarios.tablero11.siteID','usuarios.tablero11.fechaControl','usuarios.tablero11.nomCentroDigital',\
#            'usuarios.tablero11.localidad','usuarios.tablero11.nombreDepartamento','usuarios.tablero11.nombreMunicipio', \
#           'usuarios.tablero11idBeneficiario','usuarios.tablero11.apGroup','usuarios.tablero11.location', \
#           'usuarios.tablero11.fecha','usuarios.tablero11.anyo','usuarios.tablero11.mes','usuarios.tablero11.macRed' \
#           'usuarios.tablero11.usuariosConectados','usuarios.tablero11.sesiones_Usuarios','usuarios.tablero11.consumoUsuarios' \
#           'usuarios.tablero11.consumoUsuariosDescarga_aux','usuarios.tablero11.consumoUsuariosCarga_aux',\
#            'usuarios.tablero11.consumoUsuariosDescarga','usuarios.tablero11.consumoUsuariosCarga','@timestamp']]
    
#     return df


# In[132]:


#No se esta utilizando campo relacionado con los dispositivos en el tablero 11...


try:
    totalesDispositivos = datos_det_conex[["fecha_control","site_id"
                                 ,"usuarios.sistemaOperativoUsuarios"
                                 ,'usuarios.tipoDispositivoUsuarios'
                                 ,'manufacturer'
                                 ,'radio.band'
                                 ,"mac_usuario"]].groupby(["fecha_control","site_id"
                                                     ,"usuarios.sistemaOperativoUsuarios"
                                                     ,'usuarios.tipoDispositivoUsuarios'
                                                     ,'manufacturer'
                                                     ,'radio.band']).agg(['count']).reset_index()
    ##si se quiere contar ocurrencias unicas se debe usar en lugar del agg: ['mac_usuario'].nunique().reset_index()
    totalesDispositivos.columns = totalesDispositivos.columns.droplevel(1)
    totalesDispositivos= totalesDispositivos.rename(columns={'mac_usuario' : 'usuarios.totales.dispositivos'
                                                            ,'fecha_control' : 'usuarios.fechaControl'
                                                            ,'manufacturer' : 'usuarios.marcaTerminal'
                                                            ,'radio.band' : 'usuarios.tecnologiaTerminal'
                                                            })
    totalesDispositivos = pd.merge(totalesDispositivos, datos_semilla, on='site_id',how='inner')
    totalesDispositivos["usuarios.fecha"] = totalesDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    totalesDispositivos["usuarios.anyo"] = totalesDispositivos["usuarios.fecha"].str[0:4]
    totalesDispositivos["usuarios.mes"] = totalesDispositivos["usuarios.fecha"].str[5:7]
    totalesDispositivos["usuarios.dia"] = totalesDispositivos["usuarios.fecha"].str[8:10]
    totalesDispositivos["usuarios.hora"] = totalesDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    totalesDispositivos["usuarios.minuto"] = totalesDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    totalesDispositivos= totalesDispositivos.rename(columns={'site_id' : 'usuarios.siteID'})
    totalesDispositivos['nombreDepartamento'] = totalesDispositivos['usuarios.nombreDepartamento']
    totalesDispositivos['nombreMunicipio'] = totalesDispositivos['usuarios.nombreMunicipio']
    totalesDispositivos['idBeneficiario'] = totalesDispositivos['usuarios.idBeneficiario']
    totalesDispositivos['fecha'] = totalesDispositivos['usuarios.fecha']
    totalesDispositivos['anyo'] = totalesDispositivos['usuarios.anyo']
    totalesDispositivos['mes'] = totalesDispositivos['usuarios.mes']
    totalesDispositivos['dia'] = totalesDispositivos['usuarios.dia']
    totalesDispositivos['@timestamp'] = now.isoformat()
    
    #Se comenta escritura en el indice porque no se necesita esa estructura
    
    totalesDispositivos = cambioVariable(totalesDispositivos)
    
   

    #salida = helpers.bulk(es, doc_generator(totalesDispositivos))

except:
    pass
    


# # Insertando usuarios conectados al indice principal

# * Se calcula cantidad de sesiones por sitio con detalle conexiones, pero contando la ocurrencia unica de documento
# * Se calcula la cantidad de conexiones. Se agrupa por el campo usuarios.macRed(el cual corresponde al AP) y fecha_control. Se cuenta las ocurrencias de mac_usuario. Luego al cruzar con flujo principal, si el dato es nulo para ese momento, se debe colocar en 0.

# In[133]:


fecha_max_mintic = fecha_ejecucion

fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_consumos = traeSesiones(fecha_max_mintic,fecha_tope_mintic)

try:

    if datos_consumos is None or datos_consumos.empty:
        while (datos_consumos is None or datos_consumos.empty) and ((datetime.strptime(fecha_max_mintic[0:50], '%Y-%m-%d %H:%M:%S"').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
            fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            datos_consumos = traeSesiones(fecha_max_mintic,fecha_tope_mintic)
    else:
        pass
except:
    pass


# ### Se lee el indice all-cambium-device-client

# * En este indice se guarda el detalle de los radio por fecha
# * Detalle conexiones cruza con device clients. Con estos se calculan los totales por marca

# In[134]:


total_docs = 100000
response = es.search(
    index= 'all-'+parametros.cambium_d_c_index,
    body={
        "_source": ['mac', 'ap_mac', 'radio.band', 'radio.rx_bytes', 'radio.tx_bytes','fecha_control']
          , "query": {
              "range": {
                "fecha_control": {
                  "gte": fecha_max_mintic,
                  "lt": fecha_tope_mintic
                  #"gte": "2021-05-26 15:00:00",
                  #"lt": "2021-05-26 15:10:00"  
                }
              }
          }
    },
    size=total_docs
)
elastic_docs = response["hits"]["hits"]
# fields = {}
# for num, doc in enumerate(elastic_docs):
#     source_data = doc["_source"]
#     for key, val in source_data.items():
#         try:
#             fields[key] = np.append(fields[key], val)
#         except KeyError:
#             fields[key] = np.array([val])

# datos_radio = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))

datos_radio = pd.DataFrame([x["_source"] for x in elastic_docs])

datos_performance = datos_radio


# In[135]:


if datos_performance is None or datos_performance.empty:
    while (datos_performance is None or datos_performance.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        datos_performance = traeRadio(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[136]:


try:
    datos_logins = datos_det_conex[['fecha_control', 'site_id', 'documento','usuarios.macRed']].groupby(["fecha_control","site_id","usuarios.macRed"])['documento'].nunique().reset_index()
    datos_logins= datos_logins.rename(columns={'documento' : 'usuarios.sesiones_Usuarios'})
    
    ################# datos_performance #####################
    datos_performance['fecha_control'] = datos_performance["fecha_control"].str[0:-4] + '0:00'
    datos_performance.replace('','0',inplace=True)
    datos_performance.fillna({'radio.rx_bytes':0,'radio.tx_bytes':0},inplace=True)
    datos_performance['radio.rx_bytes'] = datos_performance['radio.rx_bytes'].astype(float)
    datos_performance['radio.tx_bytes'] = datos_performance['radio.tx_bytes'].astype(float)
    
    datos_performance = datos_performance.groupby(['ap_mac', 'fecha_control', 'mac', 'radio.band']).agg(['max']).reset_index()
    datos_performance.columns = datos_performance.columns.droplevel(1)
    datos_performance = datos_performance[['ap_mac'
                                         , 'fecha_control'
                                         , 'radio.rx_bytes'
                                         , 'radio.tx_bytes']].groupby(['ap_mac'
                                                                     , 'fecha_control']).agg(['sum']).reset_index()
    datos_performance.columns = datos_performance.columns.droplevel(1)
    datos_performance = datos_performance.rename(columns={'radio.rx_bytes': 'usuarios.consumoUsuariosDescarga_aux'
                                                     , 'radio.tx_bytes':'usuarios.consumoUsuariosCarga_aux'})
    
    datos_performance['usuarios.consumoUsuariosDescarga'] = round((datos_performance['usuarios.consumoUsuariosDescarga_aux']/float(1<<30)),6)
    datos_performance['usuarios.consumoUsuariosCarga'] = round((datos_performance['usuarios.consumoUsuariosCarga_aux']/float(1<<30)),6)
    datos_performance['usuarios.consumoUsuarios'] = datos_performance['usuarios.consumoUsuariosDescarga'] + datos_performance['usuarios.consumoUsuariosCarga']
    datos_performance['usuarios.consumoUsuarios'] = round(datos_performance['usuarios.consumoUsuarios'],6)
    datos_performance = datos_performance.rename(columns={'ap_mac':'usuarios.macRed'})
    
    
    datos_performance = pd.merge(datos_performance,datos_dev[['site_id','usuarios.macRed','usuarios.apGroup']],on='usuarios.macRed', how='left')
    
    aux_performance = pd.merge(datos_performance,  datos_semilla, on='site_id',how='inner')
    aux_performance.fillna({'usuarios.consumoUsuariosDescarga':0,
                              'usuarios.consumoUsuariosCarga':0,
                              'usuarios.consumoUsuarios':0,
                              'usuarios.apGroup':'No identificado'},inplace=True)
    aux_performance = aux_performance.rename(columns={'fecha_control':'usuarios.fechaControl'
                                                           ,'site_id' : 'usuarios.siteID'})
    
    aux_performance["usuarios.fecha"] = aux_performance["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    aux_performance["usuarios.anyo"] = aux_performance["usuarios.fecha"].str[0:4]
    aux_performance["usuarios.mes"] = aux_performance["usuarios.fecha"].str[5:7]
    aux_performance["usuarios.dia"] = aux_performance["usuarios.fecha"].str[8:10]
    aux_performance["usuarios.hora"] = aux_performance["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    aux_performance["usuarios.minuto"] = aux_performance["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    
    aux_performance['nombreDepartamento'] = aux_performance['usuarios.nombreDepartamento']
    aux_performance['nombreMunicipio'] = aux_performance['usuarios.nombreMunicipio']
    aux_performance['idBeneficiario'] = aux_performance['usuarios.idBeneficiario']
    aux_performance['fecha'] = aux_performance['usuarios.fecha']
    aux_performance['anyo'] = aux_performance['usuarios.anyo']
    aux_performance['mes'] = aux_performance['usuarios.mes']
    aux_performance['dia'] = aux_performance['usuarios.dia']
    
    
    ##################
    usuariosConectados = datos_det_conex[["fecha_control","site_id","usuarios.macRed","mac_usuario"]].groupby(["fecha_control","site_id","usuarios.macRed"]).agg(['count']).reset_index()
    usuariosConectados.columns = usuariosConectados.columns.droplevel(1)
    usuariosConectados= usuariosConectados.rename(columns={'mac_usuario' : 'usuarios.tablero11usuariosConectados'})
    
    ###################
    usuariosConectados = pd.merge(usuariosConectados,datos_logins, on=["fecha_control","site_id","usuarios.macRed"], how='inner')
    
    usuariosConectados= usuariosConectados.rename(columns={'usuarios.sesiones_Usuarios' : 'usuarios.tablero11sesiones_Usuarios'})
    
    ###################
    usuariosConectados = pd.merge(datos_semilla,  usuariosConectados, on=['site_id'], how='inner')
    
   
    #Al parecer no esta viniendo consumoUsuario....
    
    usuariosConectados.fillna({'usuarios.consumoUsuarios' : 0
                              ,'usuarios.consumoUsuariosDescarga':0
                              ,'usuarios.consumoUsuariosCarga':0},inplace=True)
    
    ####################
    usuariosConectados = pd.merge(usuariosConectados,datos_dev[['usuarios.apGroup','usuarios.macRed']], on ='usuarios.macRed', how='left')
    
    usuariosConectados.fillna({'usuarios.apGroup':'No identificado'},inplace=True)
    usuariosConectados['usuarios.tablero11usuariosConectados'] = usuariosConectados['usuarios.tablero11usuariosConectados'].astype(int)
    usuariosConectados = usuariosConectados.rename(columns={'fecha_control':'usuarios.fechaControl'
                                                           ,'site_id' : 'usuarios.siteID'})
    
    usuariosConectados["usuarios.fecha"] = usuariosConectados["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    usuariosConectados["usuarios.anyo"] = usuariosConectados["usuarios.fecha"].str[0:4]
    usuariosConectados["usuarios.mes"] = usuariosConectados["usuarios.fecha"].str[5:7]
    usuariosConectados["usuarios.dia"] = usuariosConectados["usuarios.fecha"].str[8:10]
    usuariosConectados["usuarios.hora"] = usuariosConectados["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    usuariosConectados["usuarios.minuto"] = usuariosConectados["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    
    usuariosConectados['nombreDepartamento'] = usuariosConectados['usuarios.nombreDepartamento']
    usuariosConectados['nombreMunicipio'] = usuariosConectados['usuarios.nombreMunicipio']
    usuariosConectados['idBeneficiario'] = usuariosConectados['usuarios.idBeneficiario']
    usuariosConectados['fecha'] = usuariosConectados['usuarios.fecha']
    usuariosConectados['anyo'] = usuariosConectados['usuarios.anyo']
    usuariosConectados['mes'] = usuariosConectados['usuarios.mes']
    usuariosConectados['dia'] = usuariosConectados['usuarios.dia']
    
    #####################

    columnas_aux_performance=[ 'usuarios.fechaControl', 'usuarios.siteID',                               'usuarios.apGroup', 'usuarios.macRed',                               'usuarios.consumoUsuariosCarga_aux', 'usuarios.consumoUsuariosDescarga',                               'usuarios.consumoUsuariosCarga', 'usuarios.consumoUsuarios',                                 'usuarios.consumoUsuariosDescarga_aux']
    columnas_merge = ['usuarios.fechaControl','usuarios.siteID','usuarios.apGroup','usuarios.macRed']
    
    usuariosConectados=pd.merge(usuariosConectados,aux_performance[columnas_aux_performance],on=columnas_merge, how='left')
    

    usuariosConectados = usuariosConectados.rename(columns=
                                                   {'usuarios.consumoUsuariosCarga_aux':'usuarios.tablero11consumoUsuariosCarga_aux'
                                                    ,'usuarios.consumoUsuariosDescarga_aux':'usuarios.tablero11consumoUsuariosDescarga_aux'
                                                    ,'usuarios.consumoUsuariosCarga' : 'usuarios.tablero11consumoUsuariosCarga'
                                                    ,'usuarios.consumoUsuariosDescarga':'usuarios.tablero11consumoUsuariosDescarga'
                                                    ,'usuarios.consumoUsuarios':'usuarios.tablero11consumoUsuarios'
                                                   })

except:
    pass


# In[137]:


try:
    use_these_keys = ['usuarios.tablero11.siteID'
                      ,'usuarios.tablero11.nomCentroDigital'
                      , 'usuarios.tablero11.localidad'
                      , 'usuarios.tablero11.nombreDepartamento'
                      , 'usuarios.tablero11.nombreMunicipio'
                      , 'usuarios.tablero11.idBeneficiario'
                      , 'usuarios.tablero11.location'
                      , 'usuarios.tablero11.sesiones_Usuarios'
                      , 'usuarios.tablero11.usuariosConectados'
                      , 'usuarios.tablero11.consumoUsuarios'
                      , 'usuarios.tablero11.consumoUsuariosDescarga_aux'
                      , 'usuarios.tablero11.consumoUsuariosCarga_aux'
                      , 'usuarios.tablero11.consumoUsuariosDescarga'
                      , 'usuarios.tablero11.consumoUsuariosCarga'
                      , 'usuarios.tablero11.apGroup'
                      , 'usuarios.tablero11.fechaControl'
                      , 'usuarios.tablero11.fecha'
                      , 'usuarios.tablero11.anyo'
                      , 'usuarios.tablero11.mes'
                      , 'usuarios.tablero11.macRed'
                      , '@timestamp']


    usuariosConectados['@timestamp'] = now.isoformat()
    
    #Seleccionar campos del Tablero..
    
    #Cambio de variables en usuariosConectados...
    
    usuariosConectados = cambioVariable(usuariosConectados)
    
    
    def doc_generator_consumo(df,use_these_keys):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'usuariosConectados-'+ str(document['usuarios.tablero11.siteID']) + str(document['usuarios.tablero11.macRed']) + '-' + str(document['usuarios.tablero11.fechaControl']) +'-'+ str(random.randrange(10000))}",
                    "_source": filterKeys(document,use_these_keys),
                }

    #Se comenta escritura en el indice porque no se necesita esa estructura#Se comenta escritura en el indice porque no se necesita esa estructura            }
    
#     usuariosConectados.fillna("",inplace=True)

    usuariosConectados.fillna({'usuarios.tablero11.sesiones_Usuarios':0
                              , 'usuarios.tablero11.usuariosConectados':0
                              , 'usuarios.tablero11.consumoUsuarios':0
                              , 'usuarios.tablero11.consumoUsuariosDescarga_aux':0
                              , 'usuarios.tablero11.consumoUsuariosCarga_aux':0
                              , 'usuarios.tablero11.consumoUsuariosDescarga':0
                              , 'usuarios.tablero11.consumoUsuariosCarga':0},inplace=True)
    
    usuariosConectados.fillna("",inplace=True)
    
    salida = helpers.bulk(es, doc_generator_consumo(usuariosConectados,use_these_keys))
    
    print("Fecha: ", now,"- Usuarios conectados insertados en indice principal:",salida[0])
    
except Exception as e:  
    
    print("Fecha: ", now,"- Ningun usuario conectado para insertar en indice principal:")


# # Uso servicio de internet insertados en indice principal

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán

# In[139]:


# use_these_keys = ['usuarios.siteID'
#                   ,'usuarios.tablero11.nomCentroDigital'
#                   , 'usuarios.tablero11.localidad'
#                   , 'usuarios.tablero11.nombreDepartamento'
#                   , 'usuarios.tablero11.nombreMunicipio'
#                   , 'usuarios.tablero11.idBeneficiario'
#                   , 'usuarios.tablero11.location'
# #                   , 'usuarios.tablero11sesiones_Usuarios'
# #                   , 'usuarios.tablero11consumoUsuarios'
#                   , 'usuarios.tablero11.apGroup'
#                   , 'usuarios.tablero11.fechaControl'
#                   , 'usuarios.tablero11.fecha'
#                   , 'usuarios.tablero11.anyo'
#                   , 'usuarios.tablero11.mes'
#                   , '@timestamp']

# def doc_generator(df,use_these_keys):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#                 "_index": indice, 
#                 "_id": f"{ 'UsoServicio-' + str(document['usuarios.macRed']) + '-' + str(document['usuarios.detallesTecnologiasTerminales']) + '-' + str(document['usuarios.fechaControl'])}",
#                 "_source": filterKeys(document,use_these_keys),
#             }


# * se toma el maximo radio para cada mac de dispositivo final
# * se suma agrupando por ap_mac, banda y fecha
# * El resultado es un total para cada banda, fecha y ap_mac

# In[140]:


#NO se utiliza informacion del uso del servicio de Internet por MAC address en el Tablero 11.


# try:    
#     datos_radio['fecha_control'] = datos_radio['fecha_control'].str[0:15]+'0:00'
#     datos_radio = datos_radio.groupby(['ap_mac', 'fecha_control', 'mac', 'radio.band']).agg(['max']).reset_index()
#     datos_radio.columns = datos_radio.columns.droplevel(1)
#     datos_radio = datos_radio[['ap_mac', 'fecha_control', 'radio.band', 'radio.rx_bytes',
#            'radio.tx_bytes']].groupby(['ap_mac', 'fecha_control', 'radio.band']).agg(['sum']).reset_index()
#     datos_radio.columns = datos_radio.columns.droplevel(1)
#     datos_radio = datos_radio.rename(columns={'ap_mac' : 'usuarios.macRed'})
#     usoServicioInternetSitio = pd.merge(datos_radio,datos_dev[['usuarios.apGroup', 'site_id', 'usuarios.macRed']], on ='usuarios.macRed', how='inner')
#     usoServicioInternetSitio.fillna({'radio.rx_bytes':0,'radio.tx_bytes':0},inplace=True)
#     usoServicioInternetSitio.fillna({'usuarios.macRed':'','radio.band':'No identificado','usuarios.apGroup':'No identificado'},inplace=True)
#     usoServicioInternetSitio['usuarios.usoServicioInternetSitio'] = usoServicioInternetSitio['radio.rx_bytes'].astype(float) + usoServicioInternetSitio['radio.tx_bytes'].astype(float)
#     usoServicioInternetSitio = pd.merge(usoServicioInternetSitio,datos_semilla, on ='site_id', how='inner')
#     usoServicioInternetSitio= usoServicioInternetSitio.rename(columns={'site_id' : 'usuarios.siteID'
#                                                                     ,'radio.band' : 'usuarios.detallesTecnologiasTerminales'
#                                                                     , 'fecha_control' : 'usuarios.fechaControl'})
#     usoServicioInternetSitio['usuarios.usoServicioInternetSitio'] = round((usoServicioInternetSitio['usuarios.usoServicioInternetSitio']/float(1<<30)),6) 
#     usoServicioInternetSitio["usuarios.fecha"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
#     usoServicioInternetSitio["usuarios.anyo"] = usoServicioInternetSitio["usuarios.fecha"].str[0:4]
#     usoServicioInternetSitio["usuarios.mes"] = usoServicioInternetSitio["usuarios.fecha"].str[5:7]
#     usoServicioInternetSitio["usuarios.dia"] = usoServicioInternetSitio["usuarios.fecha"].str[8:10]
#     usoServicioInternetSitio["usuarios.hora"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
#     usoServicioInternetSitio["usuarios.minuto"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    
#     usoServicioInternetSitio['nombreDepartamento'] = usoServicioInternetSitio['usuarios.nombreDepartamento']
#     usoServicioInternetSitio['nombreMunicipio'] = usoServicioInternetSitio['usuarios.nombreMunicipio']
#     usoServicioInternetSitio['idBeneficiario'] = usoServicioInternetSitio['usuarios.idBeneficiario']
#     usoServicioInternetSitio['fecha'] = usoServicioInternetSitio['usuarios.fecha']
#     usoServicioInternetSitio['anyo'] = usoServicioInternetSitio['usuarios.anyo']
#     usoServicioInternetSitio['mes'] = usoServicioInternetSitio['usuarios.mes']
#     usoServicioInternetSitio['dia'] = usoServicioInternetSitio['usuarios.dia']
    
#     usoServicioInternetSitio['@timestamp'] = now.isoformat()
    
#     #Cambio de variables utilizando los campos del Tablero...
    
#     usoServicioInternetSitio=cambioVariable(usoServicioInternetSitio)
    
#     #Se comenta escritura en el indice porque no se necesita esa estructura
#     #salida = helpers.bulk(es, doc_generator(usoServicioInternetSitio))
#     #print("Fecha: ", now,"- Uso servicio y tecnología terminal insertados en indice principal:",salida[0])
# except:
#     print("Fecha: ", now,"- Ningun uservicio/tecnología terminal conectado para insertar en indice principal")


# Se renombra fecha_control para cruzar

# ## Totalizar la cantidad de dispositivos por usuario

# * Para contabilizar los dispositivos por usuario, se debe contar la cantidad de ocurrencias de mac_usuario dentro de detalle de conexiones, agrupando por tipo documento y documento. 
# * Aún cuando la mayoria de las fuentes referencia a mac_usuario, no se debe confundir a la hora de agrupar con los datos reales de usuario que en este caso serían el documento de identidad como campo de identificación

# In[141]:


# NO se utiliza campos de CONTEO DE DISPOSITIVOS..

# try:
#     conteoDispositivos = datos_det_conex[['fecha_control','site_id', 'documento', 'tipodoc','fecha']].groupby(['fecha_control','site_id', 'documento', 'tipodoc']).agg(['count']).reset_index()
#     conteoDispositivos.columns = conteoDispositivos.columns.droplevel(1)
#     conteoDispositivos= conteoDispositivos.rename(columns={'fecha' : 'conteo'})
#     conteoDispositivos = conteoDispositivos[['fecha_control','site_id','conteo']].groupby(['fecha_control','site_id']).agg(['sum']).reset_index()
#     conteoDispositivos.columns = conteoDispositivos.columns.droplevel(1)
#     conteoDispositivos= conteoDispositivos.rename(columns={'conteo' : 'usuarios.conteoDispositivos'})
#     conteoDispositivos = pd.merge(conteoDispositivos, datos_semilla, on='site_id', how='inner')
#     conteoDispositivos = conteoDispositivos.rename(columns={'fecha_control' : 'usuarios.fechaControl'})
#     conteoDispositivos["usuarios.fecha"] = conteoDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
#     conteoDispositivos["usuarios.anyo"] = conteoDispositivos["usuarios.fecha"].str[0:4]
#     conteoDispositivos["usuarios.mes"] = conteoDispositivos["usuarios.fecha"].str[5:7]
#     conteoDispositivos["usuarios.dia"] = conteoDispositivos["usuarios.fecha"].str[8:10]
#     conteoDispositivos["usuarios.hora"] = conteoDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
#     conteoDispositivos["usuarios.minuto"] = conteoDispositivos["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
#     conteoDispositivos= conteoDispositivos.rename(columns={'site_id' : 'usuarios.siteID'})
    
#     conteoDispositivos['nombreDepartamento'] = conteoDispositivos['usuarios.nombreDepartamento']
#     conteoDispositivos['nombreMunicipio'] = conteoDispositivos['usuarios.nombreMunicipio']
#     conteoDispositivos['idBeneficiario'] = conteoDispositivos['usuarios.idBeneficiario']
#     conteoDispositivos['fecha'] = conteoDispositivos['usuarios.fecha']
#     conteoDispositivos['anyo'] = conteoDispositivos['usuarios.anyo']
#     conteoDispositivos['mes'] = conteoDispositivos['usuarios.mes']
#     conteoDispositivos['dia'] = conteoDispositivos['usuarios.dia']
# except:
#     pass


# # 4. Insertando promedio dispositivos en indice principal

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán

# In[142]:


# NO se utiliza campos de CONTEO DE DISPOSITIVOS..


# try:
#     use_these_keys = ['usuarios.tablero11siteID'
#                      , 'usuarios.tablero11nomCentroDigital'
#                      , 'usuarios.tablero11localidad'
#                      , 'usuarios.tablero11nombreDepartamento'
#                      , 'usuarios.tablero11nombreMunicipio'
#                      , 'usuarios.tablero11idBeneficiario'
#                      , 'usuarios.tablero11location'
#                      , 'usuarios.tablero11fechaControl'
#                      , 'usuarios.tablero11fecha'
#                      , 'usuarios.tablero11anyo'
#                      , 'usuarios.tablero11mes'
#                      , '@timestamp']

#     conteoDispositivos['@timestamp'] = now.isoformat()
    
#     #Cambio de variables utilizando los campos del Tablero...
    
#     conteoDispositivos=cambioVariable(conteoDispositivos)
    
#     #2307
    
#     def doc_generator(df):
#         df_iter = df.iterrows()
#         for index, document in df_iter:
#             yield {
#                     "_index": indice, 
#                     "_id": f"{ 'PromedioDispositivos-' + str(document['usuarios.tablero11siteID']) + '-' + str(document['usuarios.fechaControl'])}",
#                     "_source": filterKeys(document),
#                 }
#     #Se comenta escritura en el indice porque no se necesita esa estructura        
#     #salida = helpers.bulk(es, doc_generator(conteoDispositivos))
#     #print("Fecha: ", now,"- Promedios dispositivos Usuarios:",salida[0])
# except:
#     print("Fecha: ", now,"- Ningun Promedios dispositivos Usuarios para insertar en indice principal")
    


# # Marcando si usuario es nuevo o recurrente

# * Se toma del dataframe datos_det_conex y se cuenta las veces que cada mac_usuario aparece
# * Luego se suma con el historico de conexiones por usuario (indice intermedio ohmyfi-total-conexiones-historico)
# * marca como Nuevo(1 conexión). 
# 
# De este proceso se obtiene el campo:
# * usuarios.usuariosNuevos
# * usuarios.totales.usuariosNuevos

# In[143]:


# try:
#     datos_recurrencia = datos_det_conex[["fecha","tipodoc","documento"]].groupby(["tipodoc","documento"]).agg(['count']).reset_index()
#     datos_recurrencia.columns = datos_recurrencia.columns.droplevel(1)
#     datos_recurrencia= datos_recurrencia.rename(columns={'fecha' : 'usuarios_recurrencia_aux'})
#     datos_recurrencia['tipodoc'] = datos_recurrencia['tipodoc'].replace('','No especificado')
#     datos_recurrencia['documento'] = datos_recurrencia['documento'].replace('','No especificado')
# except:
#     pass


# ### Se lee historico

# Se lee ohmyfi-total-conexiones-historico, el cual es un indice creado a partir de los datos crudos. Este guarda la cantidad de conexiones realizadas por ese usuario. Para que la consulta funcione correctamente se debe colocar el filtro que usuarios_recurrencia exista.

# In[144]:


# try:
#     total_docs = 30000000
#     response = es.search(
#         index= parametros.ohmyfi_total_c_index,
#         body={
#               "_source": ['tipodoc', 'documento', 'usuarios_recurrencia'],
#               "query": {
#                 "bool": {
#                   "filter": [
#                   {
#                     "exists": {
#                       "field":"usuarios_recurrencia"
#                     }
#                   }
#                   ]
#                 }
#               }
#         },
#         size=total_docs
#     )
#     elastic_docs = response["hits"]["hits"]
    
#     ya_en_indice = pd.DataFrame([x["_source"] for x in elastic_docs])
    
    
# except:
#     ya_en_indice = pd.DataFrame(columns=['tipodoc', 'documento', 'usuarios_recurrencia'])


# In[145]:


# if ya_en_indice is None or ya_en_indice.empty:
#     datos_recurrencia= datos_recurrencia.rename(columns={'usuarios_recurrencia_aux' : 'usuarios_recurrencia'})
# else:
#     datos_recurrencia = pd.merge(datos_recurrencia, ya_en_indice, on=['tipodoc', 'documento'],how='left')
#     datos_recurrencia.fillna({'usuarios_recurrencia':0},inplace=True)
#     datos_recurrencia['usuarios_recurrencia'] = datos_recurrencia['usuarios_recurrencia'] + datos_recurrencia['usuarios_recurrencia_aux']
#     datos_recurrencia['usuarios_recurrencia'] = [int(x) for x in datos_recurrencia['usuarios_recurrencia']]


# In[146]:


# try:
#     aux_recurrencia = pd.merge(datos_recurrencia,datos_det_conex, on=['tipodoc','documento'],how='inner' )
#     aux_recurrencia = aux_recurrencia[(aux_recurrencia['usuarios_recurrencia']==1)]
#     aux_recurrencia.loc[aux_recurrencia['usuarios_recurrencia']==1,'usuarios.usuariosNuevos']='Nuevo'
#     #aux_recurrencia.loc[aux_recurrencia['usuarios_recurrencia']!=1,'usuarios.usuariosNuevos']='Recurrente'
#     aux_recurrencia = aux_recurrencia.rename(columns={'usuarios_recurrencia' :'usuarios.sesiones_Usuarios'})
#     aux_recurrencia = aux_recurrencia[["fecha_control","site_id","usuarios.usuariosNuevos",'documento']].groupby(["fecha_control","site_id","usuarios.usuariosNuevos"])['documento'].nunique().reset_index()
#     aux_recurrencia= aux_recurrencia.rename(columns={'documento' : 'usuarios.totales.usuariosNuevos'})
# except:
#     pass


# In[147]:


# try:
#     datos_recurrencia = pd.merge(datos_semilla,  aux_recurrencia, on='site_id', how='inner')
#     datos_recurrencia = datos_recurrencia.rename(columns={'fecha_control' : 'usuarios.fechaControl'})
#     datos_recurrencia["usuarios.fecha"] = datos_recurrencia["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
#     datos_recurrencia["usuarios.anyo"] = datos_recurrencia["usuarios.fecha"].str[0:4]
#     datos_recurrencia["usuarios.mes"] = datos_recurrencia["usuarios.fecha"].str[5:7]
#     datos_recurrencia["usuarios.dia"] = datos_recurrencia["usuarios.fecha"].str[8:10]
#     datos_recurrencia["usuarios.hora"] = datos_recurrencia["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
#     datos_recurrencia["usuarios.minuto"] = datos_recurrencia["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
#     datos_recurrencia= datos_recurrencia.rename(columns={'site_id' : 'usuarios.siteID'})
    
#     datos_recurrencia['nombreDepartamento'] = datos_recurrencia['usuarios.nombreDepartamento']
#     datos_recurrencia['nombreMunicipio'] = datos_recurrencia['usuarios.nombreMunicipio']
#     datos_recurrencia['idBeneficiario'] = datos_recurrencia['usuarios.idBeneficiario']
#     datos_recurrencia['fecha'] = datos_recurrencia['usuarios.fecha']
#     datos_recurrencia['anyo'] = datos_recurrencia['usuarios.anyo']
#     datos_recurrencia['mes'] = datos_recurrencia['usuarios.mes']
#     datos_recurrencia['dia'] = datos_recurrencia['usuarios.dia']
# except:
#     pass


# # Insertando recurrencia de usuario en indice principal

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán

# In[148]:


# try:
#     use_these_keys = ['usuarios.tablero11.siteID'
#                       , 'usuarios.tablero11.nomCentroDigital'
#                       , 'usuarios.tablero11.localidad'
#                       , 'usuarios.tablero11.nombreDepartamento'
#                       , 'usuarios.tablero11.nombreMunicipio'
#                       , 'usuarios.tablero11.idBeneficiario'
#                       , 'usuarios.tablero11.location'
#     #                   , 'usuarios.tablero11sesiones_Usuarios'
#     #                   , 'usuarios.tablero11consumoUsuarios'
#     #                   , 'usuarios.tablero11apGroup'
#                       , 'usuarios.tablero11.fechaControl'
#                       , 'usuarios.tablero11.fecha'
#                       , 'usuarios.tablero11.anyo'
#                       , 'usuarios.tablero11.mes'
#                       , '@timestamp']


#     datos_recurrencia['@timestamp'] = now.isoformat()
    
#     datos_recurrencia=cambioVariable(datos_recurrencia)
    
#     datos_recurrencia=cambio
    
#     def doc_generator(df,use_these_keys):
#         df_iter = df.iterrows()
#         for index, document in df_iter:
#             yield {
#                     "_index": indice, 
#                     "_id": f"{ 'Recurrencia-' + str(document['usuarios.tablero11.siteID']) + '-' + str(document['usuarios.tablero11.fechaControl'])}",
#                     "_source": filterKeys(document,use_these_keys),
#                 }
            
#     #Se comenta escritura en el indice porque no se necesita esa estructura        
#     #  salida = helpers.bulk(es, doc_generator(datos_recurrencia))
# #     print("Fecha: ", now,"- recurrencia de usuario a indice:",salida[0])
# except:
#     print("Fecha: ", now,"- Ninguna recurrencia de usuario para insertar en indice principal")


# ## Insertando detalle MACs

# In[149]:


#NO Se utilizan campos MAC en el Tablero 11..

# use_these_keys = ['usuarios.tablero11siteID'
#                   ,'usuarios.tablero11nomCentroDigital'
#                   , 'usuarios.tablero11localidad'
#                   , 'usuarios.tablero11nombreDepartamento'
#                   , 'usuarios.tablero11nombreMunicipio'
#                   , 'usuarios.tablero11idBeneficiario'
#                   , 'usuarios.tablero11location'
#                   , 'usuarios.tablero11apGroup'
#                   , 'usuarios.tablero11fechaControl'
#                   , 'usuarios.tablero11fecha'
#                   , 'usuarios.tablero11anyo'
#                   , 'usuarios.tablero11mes'
#                   , '@timestamp']

# # use_these_keys = ['usuarios.siteID'
# #                   , 'usuarios.nomCentroDigital'
# #                   , 'usuarios.codISO'
# #                   , 'usuarios.localidad'
# #                   , 'usuarios.nombreDepartamento'
# #                   , 'usuarios.sistemaEnergia'
# #                   , 'usuarios.nombreMunicipio'
# #                   , 'usuarios.idBeneficiario'
# #                   , 'usuarios.location'
# #                   , 'usuarios.macRed'
# #                   , 'usuarios.apGroup'
# #                   , 'usuarios.dispositivo.mac'
# #                   , 'usuarios.dispositivo.tipo'
# #                   , 'usuarios.dispositivo.nombre'
# #                   , 'usuarios.dispositivo.marca'
# #                   , 'usuarios.dispositivo.tecnologia'
# #                   , 'usuarios.dispositivo.sisOperativo'
# #                   , 'usuarios.fechaControl'
# #                   , 'usuarios.fecha'
# #                   , 'usuarios.anyo'
# #                   , 'usuarios.mes'
# #                   , 'usuarios.dia'
# #                   , 'usuarios.hora'
# #                   , 'usuarios.minuto'
# #                   , '@timestamp']
# def doc_generator_mac(df,use_these_keys):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#                 "_index": indice, 
#                 "_id": f"{ 'detalleMAC-' + str(document['usuarios.tablero11siteID']) + '-' + str(document['usuarios.fechaControl']) + '-' + str(document['usuarios.dispositivo.mac'])   }",
#                 "_source": filterKeys(document,use_these_keys),
#             }


# ## Insertando campos para tablero11

# In[150]:


# use_these_keys = ['usuarios.tablero11.siteID'
#                   ,'usuarios.tablero11.nomCentroDigital'
#                   , 'usuarios.tablero11.localidad'
#                   , 'usuarios.tablero11.nombreDepartamento'
#                   , 'usuarios.tablero11.nombreMunicipio'
#                   , 'usuarios.tablero11.idBeneficiario'
#                   , 'usuarios.tablero11.location'
#                   , 'usuarios.tablero11.sesiones_Usuarios'
#                   , 'usuarios.tablero11.consumoUsuarios'
#                   , 'usuarios.tablero11.apGroup'
#                   , 'usuarios.tablero11.fechaControl'
#                   , 'usuarios.tablero11.fecha'
#                   , 'usuarios.tablero11.anyo'
#                   , 'usuarios.tablero11.mes'
#                   , '@timestamp']
# def doc_tablero11(df,use_these_keys):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#                 "_index": indice, 
#                 "_id": f"{ 'Tablero11-' + str(document['usuarios.tablero11.idBeneficiario']) + '-' + str(document['usuarios.tablero11.fechaControl'])  }",
#                 "_source": filterKeys(document,use_these_keys),
#             }


# In[151]:


# try:
#     dispositivoUsuarios = datos_det_conex[['site_id','usuarios.macRed','usuarios.apGroup'
#                                 ,'mac_usuario','fecha_control'
#                                 ,'usuarios.tipoDispositivoUsuarios'
#                                 ,'name','manufacturer','radio.band'
#                                 ,'usuarios.sistemaOperativoUsuarios']].drop_duplicates(subset=['mac_usuario'])
#     dispositivoUsuarios.fillna({'usuarios.tipoDispositivoUsuarios':'No identificado'
#                                ,'name':'No identificado'
#                                ,'manufacturer':'No identificado'
#                                ,'radio.band':'No identificado'
#                                ,'usuarios.sistemaOperativoUsuarios':'No identificado'
#                                ,'usuarios.macRed':'No identificado'
#                                ,'usuarios.apGroup':'No identificado'
#                                },inplace=True)

#     dispositivoUsuarios = pd.merge(dispositivoUsuarios, datos_semilla, on='site_id',how='inner')
#     dispositivoUsuarios= dispositivoUsuarios.rename(columns={
#                                                      'site_id' : 'usuarios.siteID'
#                                                     ,'mac_usuario' : 'usuarios.dispositivo.mac'
#                                                     ,'usuarios.tipoDispositivoUsuarios' : 'usuarios.dispositivo.tipo'
#                                                     ,'name' : 'usuarios.dispositivo.nombre'
#                                                     ,'manufacturer' : 'usuarios.dispositivo.marca'
#                                                     ,'radio.band' : 'usuarios.dispositivo.tecnologia'
#                                                     ,'usuarios.sistemaOperativoUsuarios' : 'usuarios.dispositivo.sisOperativo'
#                                                     ,'fecha_control':'usuarios.fechaControl'
#                                                     })
#     dispositivoUsuarios["usuarios.fecha"] = dispositivoUsuarios["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
#     dispositivoUsuarios["usuarios.anyo"] = dispositivoUsuarios["usuarios.fecha"].str[0:4]
#     dispositivoUsuarios["usuarios.mes"] = dispositivoUsuarios["usuarios.fecha"].str[5:7]
#     dispositivoUsuarios["usuarios.dia"] = dispositivoUsuarios["usuarios.fecha"].str[8:10]
#     dispositivoUsuarios["usuarios.hora"] = dispositivoUsuarios["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
#     dispositivoUsuarios["usuarios.minuto"] = dispositivoUsuarios["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
#     dispositivoUsuarios['@timestamp'] = now.isoformat()
    
#    ############# aqui hacer cambio de variable  ################
#     ## El dataframe dispositivos usuarios debe contener los campos de use_these_keys de arriba
    
#     dispositivoUsuarios=cambioVariable(dispositivoUsuarios)
    
    
# #     salida = helpers.bulk(es, doc_tablero11(dataframeresultante))
#     print("Fecha: ", now,"- Detalle MACs a indice:",salida[0])

# except:
#     print("Fecha: ", now,"- Ningún Detalle MACs para insertar en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[152]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia-tablero11',
        body = { 'jerarquia-tablero11': 'jerarquia-tablero11','tablero11.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)


# In[ ]:




