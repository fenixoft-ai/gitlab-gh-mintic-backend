#!/usr/bin/env python
# coding: utf-8

# ### ¿Qué hace este script?
# 
# #### Para cada AP:
# * usuariosConectados(corresponde al total de conexiones), usuarios.sesiones_Usuarios (Corresponde al total de usuarios)
# * Información detallada de los dispositivos conectados en el rango de fecha procesado: usuarios.dispositivo.mac, usuarios.dispositivo.tipo, usuarios.dispositivo.marca, usuarios.dispositivo.tecnologia, usuarios.dispositivo.sisOperativo.
# 
# #### Para cada sitio: 
# * Totales por características de dispositivos: usuarios.sistemaOperativoUsuarios, usuarios.tipoDispositivoUsuarios, usuarios.marcaTerminal, usuarios.tecnologiaTerminal, usuarios.totales.dispositivos (Este ultimo guarda el total)
# * usuarios.usoServicioInternetSitio es la suma en Gb consumido discriminado por usuarios.detallesTecnologiasTerminales (Bandas 2.4 o 5 GHz)
# * usuarios.conteoDispositivos el cual indica el total de dispositivos conectados (Es lo mismo que total de conexiones)
# * usuarios.usuariosNuevos (Esta tenía antes Nuevos y Recurrentes, pero ahora solo tiene Nuevos por solicitud de BI)
# * usuarios.totales.usuariosNuevos, tiene el total de usuarios nuevos (el script por el momento solo totaliza nuevos siempre y cuando no se corra historicos)

# In[1]:



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

# In[2]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=133, max_retries=3, retry_on_timeout=True
)


# Función para realizar consultas cuando la cantidad de registros es mayor a 10.000

# In[3]:


def custom_scan(query, index, total_docs, client):
    
    results = helpers.scan(client, index=index, query=query)
    
    data = []
    for item in results:
        data.append(item['_source'])
        if len(data) >= total_docs:
            break
            
    return pd.DataFrame(data)


# ### Calculando fechas para la ejecución

# * Se calculan las fechas para asociar al nombre del indice
# * fecha_hoy es usada para concatenar al nombre del indice principal previa inserción

# In[4]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### nombre de indice donde se insertará

# In[5]:


indice = parametros.usuarios_tablero09_index 
indice_control = parametros.tableros_mintic_control


# ### Funcion para construir JSON compatible con ElasticSearch

# In[6]:


def filterKeys(document, use_these_keys):
    return {key: document.get(key) for key in use_these_keys }


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[7]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
           "_source": ["tablero09.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia-tablero09"
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
        fecha_ejecucion = doc["_source"]['tablero09.fechaControl']
except Exception as e:
    print(e)
    response["hits"]["hits"] = []
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-05-14 00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ### leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud,COD_ISO, id_Beneficiario).

# In[8]:



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
    elastic_docs = response["hits"]["hits"]  
    datos_semilla = pd.DataFrame([x["_source"] for x in elastic_docs])  
except:
    exit()


# Se eliminan espacios en blanco

# In[9]:


datos_semilla['site_id'] = datos_semilla['site_id'].str.strip()


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location<br>
# * Se renombran los campos de semilla

# In[10]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrÃ³n que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['usuarios.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.location']=datos_semilla['usuarios.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# In[11]:


datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.localidad'
                                            , 'nombreSede' : 'usuarios.nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.codISO'
                                            , 'id_Beneficiario' : 'usuarios.idBeneficiario'})


# Se descartan los registros que tengan la latitud y longitud vacía o no valida

# In[12]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.location"]=='')].index)


# ### leyendo indice cambium-devicedevices

# Se lee la información de los dispositivos de red monitoreados por Cambium. En esta lectura no hay referencia de fechas ya que solo hay una ocurrencia por MAC de dispositivo de red.
# 
# * site_id es la llave para cruzar con cada centro de conexión.
# * mac, status, ip son datos básicos del dispositivo.
# * ap_group para identificar si la conexión es indoor/outdoor

# In[13]:


try:
    
    query = {
        "_source": ["site_id","mac","status","ip","ap_group"], 
        "query": {
            "match_all": {}
        }
    }
    
    datos_dev = custom_scan(
        query, 
        parametros.cambium_d_d_index,
        total_docs=30000, 
        client=es
    )
    
    datos_dev['site_id'] = datos_dev['site_id'].str.strip()
except:
    exit()


# Se descartan registros con site_id vacios ya que no cruzarán en el merge y se limpian los NaN del dataframe.

# In[14]:


datos_dev.dropna(subset=['site_id'], inplace=True)
datos_dev.fillna('', inplace=True)
datos_dev = datos_dev.drop(datos_dev[(datos_dev['site_id']=='')].index)


# In[15]:


datos_dev['ap_group'] = datos_dev['ap_group'].str.split("-", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split("_", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split(".", n = 1, expand = True)[0]
datos_dev = datos_dev.drop(datos_dev[(datos_dev['ap_group']=='')].index)


# Se toman solo los datos unicos con mac.

# In[16]:


datos_dev = datos_dev.drop_duplicates('mac')


# Se cambia el nombre a la mac del dispositivo de red para no confundir con la de dispositivos de usuario 

# In[17]:


datos_dev= datos_dev.rename(columns={'mac' : 'usuarios.macRed','ap_group' : 'usuarios.apGroup'})


# ## Lectura de datos ohmyfi-detalleconexiones

# Los datos que se toman son:
# * fechahora (de cada conexión). fecha_control es lo mismo pero con el intervalo de 10 minutos
# * Información del centro: lugar, lugar_cod.
# * Información del dispositivo de usuario: mac_usuario, dispositivo, sistema_operativo.
# * Información de usuario: tipodoc y documento

# In[18]:


def trae_conexiones(fecha_ini, fecha_fin, client):
    total_docs=5000000
    query = {
        "_source": [
            "fechahora", "fecha_control", "lugar", "lugar_cod", 
            "mac_usuario", "dispositivo", "sistema_operativo",'tipodoc', 
            'documento'
        ], 
        "query": {
            "range": {
                "fechahora": {
                    "gte": fecha_ini,
                    "lt": fecha_fin, 
                }
            }
        }
    }

    return custom_scan(
        query, 
        parametros.ohmyfi_d_c_index,
        total_docs=5000000, 
        client=client
    )


# ### Lanzando ejecución de consulta

# * Se calcula rango en base a la fecha de control. Para este caso es de 10 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.
# 

# In[19]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = datetime.strptime(fecha_max_mintic, "%Y-%m-%d %H:%M:%S")
fecha_tope_mintic += timedelta(minutes=50)
fecha_tope_mintic -= timedelta(seconds=1)
fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")

datos_det_conex = trae_conexiones(fecha_max_mintic, fecha_tope_mintic, es)

if datos_det_conex is None or datos_det_conex.empty:
    
    while (datos_det_conex is None or datos_det_conex.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        
        fecha_max_mintic = datetime.strptime(fecha_max_mintic, "%Y-%m-%d %H:%M:%S") 
        fecha_max_mintic += timedelta(minutes=50)
        fecha_max_mintic = fecha_max_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        fecha_tope_mintic = datetime.strptime(fecha_tope_mintic, "%Y-%m-%d %H:%M:%S") 
        fecha_tope_mintic += timedelta(minutes=50)
        fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        datos_det_conex = trae_conexiones(fecha_max_mintic, fecha_tope_mintic, es)
else:
    pass


# In[20]:


datos_det_conex['fecha'] = datos_det_conex['fecha_control'].str.split(" ", n = 1, expand = True)[0]
datos_det_conex.drop_duplicates(subset=["fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo","sistema_operativo",'tipodoc','documento'],inplace=True)


# In[21]:


datos_det_conex = datos_det_conex.rename(columns={'lugar_cod' : 'site_id'
                                                   ,'fechahora':'usuarios.fechaConexionUsuarios'
                                                   ,'dispositivo': 'usuarios.tipoDispositivoUsuarios'
                                                   , 'sistema_operativo': 'usuarios.sistemaOperativoUsuarios'})


# In[22]:


datos_det_conex['site_id'] = datos_det_conex['site_id'].str.strip()


# ### Se lee información de Ohmyfi consumos

# Apartir de esta lectura se toma el valor del tiempo promedio de sesión en minutos. Se toma de referencia los campos:
# * fecha_inicio a partir de la cual se calcula fecha control
# * tiempo_sesion_minutos
# * mac_ap
# * mac_usuario 
# * lugar_cod

# In[23]:


def traeSesiones(fecha_max, fecha_tope, client):
    query = {
        "_source": ["lugar_cod", "tiempo_sesion_minutos", "mac_ap", "mac_usuario","fecha_inicio"],
        "query": {
            "range": {
                "fecha_inicio": {
                    "gte": fecha_max,
                    "lt": fecha_tope
                }
            }
        }
    }

    return custom_scan(
        query, 
        parametros.ohmyfi_consumos_index,
        total_docs=500000, 
        client=client
    )


# ### Se lee la información de cambium device client

# Esta lectura se usa para identificar: <br>
# * Lecturas de consumo por dispositivo (radios rx y tx)<br>
# * Fabricante del dispositivo<br>
# * la mac del ap que luego se usa para identificar el ap group(Indoor/Outdoor)

# In[24]:


query = {
    "_source": [
        "mac", "ap_mac", 'manufacturer', 'name', 
        "radio.band", 'radio.rx_bytes','radio.tx_bytes'
    ],
    "query": {
        "bool": {
            "filter": [{
                "exists": {
                    "field":"mac"
                }
            }]
        }
    }
}

datos_dev_clients = custom_scan(
    query, 
    parametros.cambium_d_c_index,
    total_docs=30000000, 
    client=es
)


# * se cruza por mac_usuario
# * Los merge usan left para evitar perdida de datos en cruce con cambium-devicedevices. Aquellos datos que no cruzan se les marca como no identificados. En condiciones ideales, no debería presentarse ausencia de información
# * Solo se toma lo que cruza con INNER
# * se cambia el fabricante [Local MAC]
# * se cambian lo nulos

# In[25]:


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

# Se agrupa por:<br>
# * Sistema operativo<br>
# * Tipo dispositivo<br>
# * Marca del terminal<br>
# * Tecnologia terminal<br>
# <br>
# Se cuentan las mac de usuario para cada site id y fecha control

# In[26]:


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
        
    totalesDispositivos = cambioVariable(totalesDispositivos)

except:
    pass
   


# # Calculando usuarios conectados

# * Se calcula cantidad de sesiones por sitio con detalle conexiones, pero contando la ocurrencia unica de documento<br>
# * Se calcula la cantidad de conexiones. Se agrupa por el campo usuarios.macRed(el cual corresponde al AP) y fecha_control. Se cuenta las ocurrencias de mac_usuario. Luego al cruzar con flujo principal, si el dato es nulo para ese momento, se debe colocar en 0.

# In[27]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S') 
fecha_tope_mintic += timedelta(minutes=50) 
fecha_tope_mintic -= timedelta(seconds=1)
fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")

datos_consumos = traeSesiones(fecha_max_mintic, fecha_tope_mintic, es)

try:

    if datos_consumos is None or datos_consumos.empty:
        
        while (datos_consumos is None or datos_consumos.empty) and ((datetime.strptime(fecha_max_mintic[0:50], '%Y-%m-%d %H:%M:%S"').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
            
            fecha_max_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S') 
            fecha_max_mintic += timedelta(minutes=50)
            fecha_max_mintic = fecha_max_mintic.strftime("%Y-%m-%d %H:%M:%S")
            
            fecha_tope_mintic = datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S') 
            fecha_tope_mintic += timedelta(minutes=50)
            fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")
            
            datos_consumos = traeSesiones(fecha_max_mintic, fecha_tope_mintic, es)
    else:
        pass
except:
    pass


# ### Se lee el indice all-cambium-device-client

# * En este indice se guarda el detalle de los radio por fecha<br>
# * Detalle conexiones cruza con device clients. Con estos se calculan los totales por marca

# In[28]:


def traeRadio(fecha_max, fecha_tope, client):
    
    query = {
        "_source": [
            'mac', 'ap_mac', 'radio.band', 'radio.rx_bytes', 
            'radio.tx_bytes','fecha_control'
        ], 
        "query": {
            "range": {
                "fecha_control": {
                    "gte": fecha_max_mintic,
                    "lt": fecha_tope_mintic
                }
            }
        }
    }

    return custom_scan(
        query, 
        'all-'+parametros.cambium_d_c_index,
        total_docs=100000, 
        client=client
    )

query = {
    "_source": [
        'mac', 'ap_mac', 'radio.band', 'radio.rx_bytes', 
        'radio.tx_bytes','fecha_control'
    ], 
    "query": {
          "range": {
              "fecha_control": {
                  "gte": fecha_max_mintic,
                  "lt": fecha_tope_mintic
              }
          }
    }
}

datos_radio = custom_scan(
    query, 
    'all-'+parametros.cambium_d_c_index,
    total_docs=100000, 
    client=es
)

datos_performance = datos_radio


# In[29]:


if datos_performance is None or datos_performance.empty:
    while (datos_performance is None or datos_performance.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        datos_performance = traeRadio(fecha_max_mintic, fecha_tope_mintic, es)
else:
    pass


# In[30]:


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
    columnas_aux_performance=['usuarios.macRed', 'usuarios.fechaControl',                               'usuarios.consumoUsuariosDescarga_aux',
                               'usuarios.consumoUsuariosCarga_aux', 'usuarios.consumoUsuariosDescarga',\
                               'usuarios.consumoUsuariosCarga', 'usuarios.consumoUsuarios',\
                               'usuarios.siteID', 'usuarios.apGroup']
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


# In[31]:


try:
    datos_consumos = datos_consumos.rename(columns={'lugar_cod' :'site_id'})
    datos_consumos['mac_ap'] = datos_consumos['mac_ap'].str.replace('-',':')
    datos_consumos['fecha_control'] = datos_consumos["fecha_inicio"].str[0:-4] + '0:00'
    
    tiempoPromedioSesionSitio=datos_consumos[['site_id','mac_ap','fecha_control','tiempo_sesion_minutos']].groupby(['site_id','mac_ap','fecha_control']).agg(['mean']).reset_index()
    tiempoPromedioSesionSitio.columns = tiempoPromedioSesionSitio.columns.droplevel(1)
    tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'tiempo_sesion_minutos' : 'usuarios.tiempoPromedioSesionSitio'})
    tiempoPromedioSesionSitio['usuarios.tiempoPromedioSesionSitio'] = round(tiempoPromedioSesionSitio['usuarios.tiempoPromedioSesionSitio'],6)
    tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'mac_ap' : 'usuarios.macRed'
                                                                     , 'fecha_control' : 'usuarios.fechaControl'})
    
    
    datos_dev.rename(columns={'usuarios.siteID':'site_id'},inplace=True)
    
    tiempoPromedioSesionSitio = pd.merge(tiempoPromedioSesionSitio,datos_semilla, on ='site_id', how='inner')
    tiempoPromedioSesionSitio = pd.merge(tiempoPromedioSesionSitio, datos_dev, on=['site_id','usuarios.macRed'], how='left')
    tiempoPromedioSesionSitio.fillna({'usuarios.apGroup':'No identificado'},inplace=True)
    tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'site_id' : 'usuarios.siteID'})
    tiempoPromedioSesionSitio["usuarios.fecha"] = tiempoPromedioSesionSitio["usuarios.fechaControl"].str[0:10]
    tiempoPromedioSesionSitio["usuarios.anyo"] = tiempoPromedioSesionSitio["usuarios.fecha"].str[0:4]
    tiempoPromedioSesionSitio["usuarios.mes"] = tiempoPromedioSesionSitio["usuarios.fecha"].str[5:7]
    tiempoPromedioSesionSitio['nombreDepartamento'] = tiempoPromedioSesionSitio['usuarios.nombreDepartamento']
    tiempoPromedioSesionSitio['nombreMunicipio'] = tiempoPromedioSesionSitio['usuarios.nombreMunicipio']
    tiempoPromedioSesionSitio['idBeneficiario'] = tiempoPromedioSesionSitio['usuarios.idBeneficiario']
    tiempoPromedioSesionSitio['fecha'] = tiempoPromedioSesionSitio['usuarios.fecha']    
    tiempoPromedioSesionSitio['@timestamp'] = now.isoformat()
except:
    pass


# # Calculando uso servicio de internet

# ### Asociando datos de Speed test

# Se tiene una lectura diara de velocidad para cada centro. Por tanto se debe cruzar con el fjulo principal, haciendo uso solo del año, mes día, sin incluir la hora.

# In[32]:


def traeVelocidad(fecha_max_mintic,fecha_tope_mintic):
    total_docs = 10000
    response = es.search(
        index= parametros.speed_index+'*',
        body={
                "_source": ["beneficiary_code","locationid", "result_start_date"
                            , "result_download_mbps", "result_upload_mbps"],
                "query": {
                    "range": {
                        "result_start_date": {
                            "gte": fecha_max_mintic.split(' ')[0]+'T00:00:00',
                            "lt": fecha_tope_mintic.split(' ')[0]+'T23:59:59'
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


# * Se genera fecha en yyyy-mm-dd, y cada campo por separado<br>
# * La hora y minuto se toma aparte<br>
# <br>
# Valores que se convierten a cero si son nulos<br>
# * trafico.anchoBandaDescarga<br>
# * trafico.anchoBandaCarga

# In[33]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_speed = traeVelocidad(fecha_max_mintic,fecha_tope_mintic)

if datos_speed is None or datos_speed.empty:
    while (datos_speed is None or datos_speed.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        datos_speed = traeVelocidad(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[34]:


try:
    datos_speed = datos_speed.drop(datos_speed[(datos_speed["result_download_mbps"]<0) | (datos_speed["result_upload_mbps"]<0)].index)
    datos_speed['trafico.totales.fecha'] = datos_speed['result_start_date'].str.split("T", n = 1, expand = True)[0]
    datos_speed['result_download_mbps'] = datos_speed['result_download_mbps'] * 1000
    datos_speed['result_upload_mbps'] = datos_speed['result_upload_mbps'] * 1000
    datos_speed = datos_speed[['beneficiary_code','trafico.totales.fecha','result_download_mbps','result_upload_mbps']].groupby(['beneficiary_code','trafico.totales.fecha']).agg(['max']).reset_index()
    datos_speed.columns = datos_speed.columns.droplevel(1)
    datos_speed.rename(columns={'result_download_mbps': 'trafico.anchoBandaDescarga'
                             ,'result_upload_mbps' :  'trafico.anchoBandaCarga'
                             , 'beneficiary_code' : 'site_id'
                             }, inplace=True)

    datos_speed["trafico.totales.anyo"] = datos_speed["trafico.totales.fecha"].str[0:4]
    datos_speed["trafico.totales.mes"] = datos_speed["trafico.totales.fecha"].str[5:7]
    datos_speed["trafico.totales.dia"] = datos_speed["trafico.totales.fecha"].str[8:10]
    datos_speed = pd.merge(datos_speed,  datos_semilla, on='site_id', how='inner')
    datos_speed = datos_speed.rename(columns={'site_id' : 'trafico.siteID'})
    datos_speed.dropna(subset=['trafico.anchoBandaDescarga','trafico.anchoBandaCarga'])
    datos_speed.fillna({'trafico.anchoBandaDescarga':0
                      , 'trafico.anchoBandaCarga':0
                       },inplace=True)
    datos_speed.fillna('', inplace=True)
    datos_speed['nombreDepartamento'] = datos_speed['trafico.nombreDepartamento']
    datos_speed['nombreMunicipio'] = datos_speed['trafico.nombreMunicipio']
    datos_speed['idBeneficiario'] = datos_speed['trafico.idBeneficiario']
    datos_speed['fecha'] = datos_speed['trafico.totales.fecha']
    datos_speed['anyo'] = datos_speed['trafico.totales.anyo']
    datos_speed['mes'] = datos_speed['trafico.totales.mes']
    datos_speed['dia'] = datos_speed['trafico.totales.dia']
    datos_speed['@timestamp'] = now.isoformat()

    datos_speed.rename(columns={
       'trafico.siteID':'usuarios.siteID',
       'trafico.nombreDepartamento':'usuarios.nombreDepartamento',
       'trafico.codISO':'usuarios.codISO',
       'trafico.sistemaEnergia':'usuarios.sistemaEnergia',
       'trafico.nombreMunicipio':'usuarios.nombreMunicipio',
       'trafico.localidad' : 'usuarios.localidad',
       'trafico.nomCentroDigital' : 'usuarios.nomCentroDigital',
       'trafico.idBeneficiario':'usuarios.idBeneficiario',
       'trafico.location':'usuarios.location',
       'trafico.anchoBandaDescarga':'usuarios.anchoBandaDescarga',
       'trafico.anchoBandaCarga':'usuarios.anchoBandaCarga',
       'trafico.totales.fecha':'usuarios.fecha',
       'trafico.totales.anyo':'usuarios.anyo',
       'trafico.totales.mes':'usuarios.mes',
       'trafico.totales.dia':'usuarios.dia' }, inplace=True)
except:
    pass


# In[35]:


try:    
    datos_radio['fecha_control'] = datos_radio['fecha_control'].str[0:15]+'0:00'
    datos_radio = datos_radio.groupby(['ap_mac', 'fecha_control', 'mac', 'radio.band']).agg(['max']).reset_index()
    datos_radio.columns = datos_radio.columns.droplevel(1)
    datos_radio = datos_radio[['ap_mac', 'fecha_control', 'radio.band', 'radio.rx_bytes',
           'radio.tx_bytes']].groupby(['ap_mac', 'fecha_control', 'radio.band']).agg(['sum']).reset_index()
    datos_radio.columns = datos_radio.columns.droplevel(1)
    datos_radio = datos_radio.rename(columns={'ap_mac' : 'usuarios.macRed'})
    
    usoServicioInternetSitio = pd.merge(datos_radio,datos_dev[['usuarios.apGroup', 'site_id', 'usuarios.macRed']], on ='usuarios.macRed', how='inner')
    usoServicioInternetSitio.fillna({'radio.rx_bytes':0,'radio.tx_bytes':0},inplace=True)
    usoServicioInternetSitio.fillna({'usuarios.macRed':'','radio.band':'No identificado','usuarios.apGroup':'No identificado'},inplace=True)
    usoServicioInternetSitio['usuarios.usoServicioInternetSitio'] = usoServicioInternetSitio['radio.rx_bytes'].astype(float) + usoServicioInternetSitio['radio.tx_bytes'].astype(float)
    usoServicioInternetSitio = pd.merge(usoServicioInternetSitio,datos_semilla, on ='site_id', how='inner')
    usoServicioInternetSitio= usoServicioInternetSitio.rename(columns={'site_id' : 'usuarios.siteID'
                                                                    ,'radio.band' : 'usuarios.detallesTecnologiasTerminales'
                                                                    , 'fecha_control' : 'usuarios.fechaControl'})
    usoServicioInternetSitio['usuarios.usoServicioInternetSitio'] = round((usoServicioInternetSitio['usuarios.usoServicioInternetSitio']/float(1<<30)),6) 
    usoServicioInternetSitio["usuarios.fecha"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    usoServicioInternetSitio["usuarios.anyo"] = usoServicioInternetSitio["usuarios.fecha"].str[0:4]
    usoServicioInternetSitio["usuarios.mes"] = usoServicioInternetSitio["usuarios.fecha"].str[5:7]
    usoServicioInternetSitio["usuarios.dia"] = usoServicioInternetSitio["usuarios.fecha"].str[8:10]
    usoServicioInternetSitio["usuarios.hora"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    usoServicioInternetSitio["usuarios.minuto"] = usoServicioInternetSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    
    usoServicioInternetSitio['nombreDepartamento'] = usoServicioInternetSitio['usuarios.nombreDepartamento']
    usoServicioInternetSitio['nombreMunicipio'] = usoServicioInternetSitio['usuarios.nombreMunicipio']
    usoServicioInternetSitio['idBeneficiario'] = usoServicioInternetSitio['usuarios.idBeneficiario']
    usoServicioInternetSitio['fecha'] = usoServicioInternetSitio['usuarios.fecha']
    usoServicioInternetSitio['anyo'] = usoServicioInternetSitio['usuarios.anyo']
    usoServicioInternetSitio['mes'] = usoServicioInternetSitio['usuarios.mes']
    usoServicioInternetSitio['dia'] = usoServicioInternetSitio['usuarios.dia']
    usoServicioInternetSitio['@timestamp'] = now.isoformat()

except:
    pass


# * se toma el maximo radio para cada mac de dispositivo final<br>
# * se suma agrupando por ap_mac, banda y fecha<br>
# * El resultado es un total para cada banda, fecha y ap_mac

# Se renombra fecha_control para cruzar

# In[36]:


try:
    use_these_keys = [ 'usuarios.tablero09.tiempoPromedioSesionSitio'
                      , 'usuarios.tablero09.siteID'
                      ,'usuarios.tablero09.nomCentroDigital'
                      , 'usuarios.tablero09.localidad'
                      , 'usuarios.tablero09.nombreDepartamento'
                      , 'usuarios.tablero09.nombreMunicipio'
                      , 'usuarios.tablero09.idBeneficiario'
                      , 'usuarios.tablero09.location'
                      , 'usuarios.tablero09.sesiones_Usuarios'
                      , 'usuarios.tablero09.usuariosConectados'
                      , 'usuarios.tablero09.consumoUsuarios'
                      , 'usuarios.tablero09.consumoUsuariosDescarga'
                      , 'usuarios.tablero09.consumoUsuariosCarga'
                      , 'usuarios.tablero09.apGroup'
                      , 'usuarios.tablero09.fechaControl'
                      , 'usuarios.tablero09.fecha'
                      , 'usuarios.tablero09.anyo'
                      , 'usuarios.tablero09.mes'
                      , 'usuarios.tablero09.macRed'
                      , 'usuarios.tablero09.usoServicioInternetSitio'
                      , 'usuarios.tablero09.anchoBandaDescarga'
                      , 'usuarios.tablero09.anchoBandaCarga'
                      , '@timestamp']


    usuariosConectados['@timestamp'] = now.isoformat()

    
    usuariosConectados = usuariosConectados[[  'usuarios.nomCentroDigital',  
                                               'usuarios.localidad',
                                               'usuarios.siteID', 
                                               'usuarios.nombreDepartamento',
                                               'usuarios.nombreMunicipio',
                                               'usuarios.idBeneficiario', 
                                               'usuarios.location', 
                                               'usuarios.fechaControl',
                                               'usuarios.macRed', 
                                               'usuarios.tablero11usuariosConectados',
                                               'usuarios.tablero11sesiones_Usuarios', 
                                               'usuarios.apGroup',
                                               'usuarios.fecha', 
                                               'usuarios.anyo', 
                                               'usuarios.mes',
                                               'usuarios.tablero11consumoUsuariosDescarga',
                                               'usuarios.tablero11consumoUsuariosCarga',
                                               'usuarios.tablero11consumoUsuarios',
                                               '@timestamp']]
    
    usuariosConectados = pd.merge(usuariosConectados,
                                 usoServicioInternetSitio[['usuarios.siteID',
                                                           'usuarios.macRed',
                                                           'usuarios.fechaControl',
                                                           'usuarios.usoServicioInternetSitio']]
                                 ,on=['usuarios.siteID','usuarios.macRed','usuarios.fechaControl'],
                                  how='left')
    

    datos_speed = datos_speed[['trafico.siteID', 
                               'trafico.totales.fecha', 
                               'trafico.anchoBandaDescarga',
                               'trafico.anchoBandaCarga']]
    
    datos_speed.columns = ['usuarios.siteID', 
                           'usuarios.fecha', 
                           'usuarios.anchoBandaDescarga',
                           'usuarios.anchoBandaCarga']
    
    usuariosConectados =  pd.merge(usuariosConectados,datos_speed,on=['usuarios.siteID','usuarios.fecha'],how='left')
    
    usuariosConectados =  pd.merge(usuariosConectados,
                          tiempoPromedioSesionSitio[['usuarios.siteID','usuarios.fecha','usuarios.tiempoPromedioSesionSitio']],
                          on=['usuarios.siteID','usuarios.fecha'],how='left')
    
    usuariosConectados = usuariosConectados.rename(columns={
                                                 'usuarios.nomCentroDigital':'usuarios.tablero09.nomCentroDigital',
                                                 'usuarios.localidad':'usuarios.tablero09.localidad', 
                                                 'usuarios.siteID':'usuarios.tablero09.siteID',
                                                 'usuarios.nombreDepartamento':'usuarios.tablero09.nombreDepartamento',
                                                 'usuarios.nombreMunicipio':'usuarios.tablero09.nombreMunicipio',
                                                 'usuarios.idBeneficiario':'usuarios.tablero09.idBeneficiario',
                                                 'usuarios.location':'usuarios.tablero09.location',
                                                 'usuarios.fechaControl':'usuarios.tablero09.fechaControl',
                                                 'usuarios.macRed':'usuarios.tablero09.macRed',
                                                 'usuarios.tablero11usuariosConectados':'usuarios.tablero09.usuariosConectados',
                                                 'usuarios.tablero11sesiones_Usuarios':'usuarios.tablero09.sesiones_Usuarios',
                                                 'usuarios.apGroup':'usuarios.tablero09.apGroup',
                                                 'usuarios.fecha':'usuarios.tablero09.fecha', 
                                                 'usuarios.anyo':'usuarios.tablero09.anyo', 
                                                 'usuarios.mes':'usuarios.tablero09.mes',
                                                 'usuarios.tablero11consumoUsuariosDescarga':'usuarios.tablero09.consumoUsuariosDescarga',
                                                 'usuarios.tablero11consumoUsuariosCarga':'usuarios.tablero09.consumoUsuariosCarga',
                                                 'usuarios.tablero11consumoUsuarios':'usuarios.tablero09.consumoUsuarios',
                                                 'usuarios.usoServicioInternetSitio':'usuarios.tablero09.usoServicioInternetSitio',
                                                 'usuarios.anchoBandaDescarga':'usuarios.tablero09.anchoBandaDescarga',
                                                 'usuarios.anchoBandaCarga':'usuarios.tablero09.anchoBandaCarga',
                                                 'usuarios.tiempoPromedioSesionSitio':'usuarios.tablero09.tiempoPromedioSesionSitio'})
    
    
    def doc_generator_consumo(df,use_these_keys):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'usuariosConectados-'+ str(document['usuarios.tablero09.siteID']) + str(document['usuarios.tablero09.macRed']) + '-' + str(document['usuarios.tablero09.fechaControl'])+'-'+str(random.randrange(10000))}",
                    "_source": filterKeys(document,use_these_keys),
                }

    usuariosConectados.fillna({'usuarios.tablero09.anchoBandaDescarga':0
                              ,'usuarios.tablero09.anchoBandaCarga':0
                              ,'usuarios.tablero09.usoServicioInternetSitio':0
                              ,'usuarios.tablero09.consumoUsuarios':0
                              ,'usuarios.tablero09.consumoUsuariosCarga':0
                              ,'usuarios.tablero09.consumoUsuariosDescarga':0
                              ,"usuarios.tablero09.tiempoPromedioSesionSitio":0
                              },inplace=True)
    
    usuariosConectados.fillna("",inplace=True)
    
    salida = helpers.bulk(es, doc_generator_consumo(usuariosConectados,use_these_keys))
    
    print("Fecha: ", now,"- Usuarios conectados insertados en indice principal:",salida[0])
    
except Exception as e:  
    
    print("Fecha: ", now,"- Ningun usuario conectado para insertar en indice principal:")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[37]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia-tablero09',
        body = { 'jerarquia-tablero09': 'jerarquia-tablero09','tablero09.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)

