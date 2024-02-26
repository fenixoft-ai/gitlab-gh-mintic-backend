#!/usr/bin/env python
# coding: utf-8

# **¿Qué hace este script?**
# 
# Integracion, calculos y carga en indice de los sigientes campos.
# 
# **Para cada AP:**
# - Información detallada de los dispositivos conectados en el rango de fecha procesado: usuarios.dispositivo.mac, usuarios.dispositivo.tipo, usuarios.dispositivo.marca, usuarios.dispositivo.tecnologia, usuarios.dispositivo.sisOperativo.
# 
# **Para cada sitio:**
# 
# - Totales por características de dispositivos: usuarios.sistemaOperativoUsuarios, usuarios.tipoDispositivoUsuarios, usuarios.marcaTerminal, usuarios.tecnologiaTerminal, usuarios.totales.dispositivos (Este ultimo guarda el total)
# - usuarios.usoServicioInternetSitio es la suma en Gb consumido discriminado por usuarios.detallesTecnologiasTerminales (Bandas 2.4 o 5 GHz)
# - usuarios.usuariosNuevos (Esta tenía antes Nuevos y Recurrentes, pero ahora solo tiene Nuevos por solicitud de BI)
# - usuarios.totales.usuariosNuevos, tiene el total de usuarios nuevos (el script por el momento solo totaliza nuevos siempre y cuando no se corra historicos)
# 

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
    timeout=120, max_retries=3, retry_on_timeout=True
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

# Se calculan las fechas para asociar al nombre del indice.
# fecha_hoy es usada para concatenar al nombre del indice principal previa inserción

# In[4]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### Índice para inserción de datos

# Se parametriza el indice donde seran insertados los datos luego de la transformación de los datos, asi como el indice de contro.

# In[5]:


indice = parametros.usuarios_tablero08_index
indice_control = parametros.tableros_mintic_control


# ### Función para construir JSON compatible con ElasticSearch

# In[6]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### Trae la última fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, la fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[7]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
               "_source": ["tablero08.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia-tablero08"
                    }
                  }
                  ]
                }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
    fields = {}
    for num, doc in enumerate(elastic_docs):
        fecha_ejecucion = doc["_source"]['tablero08.fechaControl']
except:
    fecha_ejecucion = '2021-06-01 00:00:00'
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-06-01 00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ### leyendo Índice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. 
# 
# Para el indice principal se requiere:
# 
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud, COD_ISO, entre otros).

# In[8]:


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
    elastic_docs = response["hits"]["hits"]    
    datos_semilla = pd.DataFrame([x["_source"] for x in elastic_docs])
    
except:
    exit()
    
t2=time.time()


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location<br>
# * Se renombran los campos de semilla

# In[9]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') 
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['usuarios.tablero08.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.tablero08.location']=datos_semilla['usuarios.tablero08.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# Se renombran los campos en el DataFrame Datos Semillas 

# In[10]:


datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.tablero08.centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.tablero08.nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.tablero08.nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.tablero08.localidad'
                                            , 'nombreSede' : 'usuarios.tablero08.nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.tablero08.sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.tablero08.codISO'
                                            , 'id_Beneficiario' : 'usuarios.tablero08.idBeneficiario'})


# Se descartan los registros que tengan la latitud y longitud vacia o no valida

# In[11]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.tablero08.location"]=='')].index)


# ### leyendo indice cambium-devicedevices

# De esta formas se asocia las MAC de dispositivos de red INDOOR y OUTDOOR
# * site_id para cruzar con las misma llave de semilla.
# * datos del dispositivo: mac, status, ip.
# * ap_group para identificar si la conexión es indoor/outdoor

# In[12]:


query = {
    "_source": ["site_id","mac","status","ip","ap_group"],
    "query": {
        "match_all": {}
    },
}

datos_dev = custom_scan(
    query, 
    parametros.cambium_d_d_index, 
    total_docs=30000, 
    client=es
)


# Se descartan registros con site_id vacios ya que no cruzarán en el merge y se limpian los NaN del dataframe.

# In[13]:


datos_dev.dropna(subset=['site_id'], inplace=True)
datos_dev.fillna('', inplace=True)
datos_dev = datos_dev.drop(datos_dev[(datos_dev['site_id']=='')].index)


# Se realiza limpieza de los registros correspondientes al campo ap_gruop eliminando los guiones, puntos y espacios vacíos.

# In[14]:


datos_dev['ap_group'] = datos_dev['ap_group'].str.split("-", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split("_", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split(".", n = 1, expand = True)[0]
datos_dev = datos_dev.drop(datos_dev[(datos_dev['ap_group']=='')].index)


# Se seleccionan solo los datos únicos con  registros en el campo mac.

# In[15]:


datos_dev = datos_dev.drop_duplicates('mac')


# Se cambia el nombre a la mac del dispositivo de red para no confundir con la de dispositivos de usuario 

# In[16]:


datos_dev= datos_dev.rename(columns={'mac' : 'usuarios.tablero08.macRed','ap_group' : 'usuarios.tablero08.apGroup'})


# ## Lectura de datos ohmyfi-detalleconexiones

# Los datos que se toman son:
# * fechahora (de cada conexión): fecha_control es lo mismo pero con el intervalo de 10 minutos
# * Información del centro: lugar, lugar_cod.
# * Información del dispositivo de usuario: mac_usuario, dispositivo, sistema_operativo.
# * Información de usuario: tipodoc y documento

# In[17]:


def trae_conexiones(fecha_ini, fecha_fin, client):
    
    query = {
        "_source": [
            "fechahora", "fecha_control", "lugar", "lugar_cod", 
            "mac_usuario", "dispositivo", "sistema_operativo", 
            'tipodoc', 'documento'
        ], 
        "query": {
            "range": {
                "fechahora": {
                    "gte": fecha_ini,
                    "lt": fecha_fin
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

# In[18]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S') 
fecha_tope_mintic += timedelta(minutes=10) 
fecha_tope_mintic -= timedelta(seconds=1)
fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")

datos_det_conex = trae_conexiones(fecha_max_mintic, fecha_tope_mintic, es)


# In[19]:


if datos_det_conex is None or datos_det_conex.empty:
    
    while (datos_det_conex is None or datos_det_conex.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        
        fecha_max_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S') 
        fecha_max_mintic += timedelta(minutes=10)
        fecha_max_mintic = fecha_max_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        fecha_tope_mintic = datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')
        fecha_tope_mintic += timedelta(minutes=10)
        fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        datos_det_conex = trae_conexiones(fecha_max_mintic, fecha_tope_mintic, es)
else:
    pass


# Se realiza limpiezan en los campos y se eliminan duplicados en los registros

# In[20]:


datos_det_conex['fecha'] = datos_det_conex['fecha_control'].str.split(" ", n = 1, expand = True)[0]
datos_det_conex.drop_duplicates(subset=["fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo","sistema_operativo",'tipodoc','documento'],inplace=True)


# Se renombran los siguientes registros:

# In[21]:


datos_det_conex = datos_det_conex.rename(columns={'lugar_cod' : 'site_id'
                                                             ,'fechahora':'usuarios.tablero08.fechaConexionUsuarios'
                                                             ,'dispositivo': 'usuarios.tablero08.tipoDispositivoUsuarios'
                                                             , 'sistema_operativo': 'usuarios.tablero08.sistemaOperativoUsuarios'})


# Se corrigen valores errados de site_id en detalle conexiones

# In[22]:


datos_det_conex['site_id'] = datos_det_conex['site_id'].str.replace("_","-")


# ### Se lee la información de cambium device client

# Esta lectura se usa para identificar: <br>
# * Lecturas de consumo por dispositivo (radios rx y tx)<br>
# * Fabricante del dispositivo<br>
# * la mac del ap que luego se usa para identificar el ap group(Indoor/Outdoor)

# In[23]:


query = {
    "_source": [
        "mac", "ap_mac", 'manufacturer', 'name', 
        "radio.band",'radio.rx_bytes','radio.tx_bytes'
    ],
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
}

datos_dev_clients = custom_scan(
    query, 
    parametros.cambium_d_c_index, 
    total_docs=30000000, 
    client=es
)


# * Se cruza por mac_usuario<br>
# * Los merge usan left para evitar perdida de datos en cruce con cambium-devicedevices. Aquellos datos que no cruzan se les marca como no identificados. En condiciones ideales, no debería presentarse ausencia de información<br>
# * Solo se toma lo que cruza con INNER<br>
# * Se cambia el fabricante [Local MAC]<br>
# * Se cambian lo nulos

# In[24]:


try:
    datos_dev_clients.drop_duplicates(inplace=True)
    datos_dev_clients = datos_dev_clients.rename(columns={'ap_mac' : 'usuarios.tablero08.macRed','mac' : 'mac_usuario'})
    datos_dev_clients['manufacturer'] = datos_dev_clients['manufacturer'].replace("[Local MAC]", "No identificado")
    datos_dev_clients.fillna({'manufacturer':'No identificado'},inplace=True)
    datos_det_conex = pd.merge(datos_det_conex,  datos_dev_clients, on='mac_usuario',how='left')
    datos_det_conex = pd.merge(datos_det_conex,datos_dev[['usuarios.tablero08.macRed','usuarios.tablero08.apGroup']],on='usuarios.tablero08.macRed', how='left')
    datos_det_conex.fillna({'usuarios.tablero08.tipoDispositivoUsuarios':'No identificado'
                       ,'usuarios.tablero08.sistemaOperativoUsuarios':'No identificado'
                       ,'manufacturer':'No identificado'
                       ,'radio.band':'No identificado'
                       },inplace=True)
except:
    pass


# ## Calculando totales por dispositivos

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irÃ¡n al indice final. si los datos no se declaran en este, no se insertaron

# In[25]:


use_these_keys = ['usuarios.tablero08.siteID'
                  , 'usuarios.tablero08.nomCentroDigital'
                  , 'usuarios.tablero08.codISO'
                  , 'usuarios.tablero08.localidad'
                  , 'usuarios.tablero08.nombreDepartamento'
                  , 'usuarios.tablero08.sistemaEnergia'
                  , 'usuarios.tablero08.nombreMunicipio'
                  , 'usuarios.tablero08.idBeneficiario'
                  , 'usuarios.tablero08.location'
                  , 'usuarios.tablero08.sistemaOperativoUsuarios'
                  , 'usuarios.tablero08.tipoDispositivoUsuarios'
                  , 'usuarios.tablero08.marcaTerminal'
                  , 'usuarios.tablero08.tecnologiaTerminal'
                  , 'usuarios.tablero08.totales.dispositivos'
                  , 'usuarios.tablero08.fechaControl'
                  , 'usuarios.tablero08.fecha'
                  , 'usuarios.tablero08.anyo'
                  , 'usuarios.tablero08.mes'
                  , 'usuarios.tablero08.dia'
                  , 'usuarios.tablero08.hora'
                  , 'usuarios.tablero08.minuto'
                  , 'nombreDepartamento'
                  , 'nombreMunicipio'
                  , 'idBeneficiario'
                  , 'fecha'
                  , 'anyo'
                  , 'mes'
                  , 'dia'
                  , '@timestamp']


# In[26]:


def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{ 'totalesDispositivos-' + str(document['usuarios.tablero08.siteID']) + '-' + str(document['usuarios.tablero08.fechaControl']+'-'+str(random.randrange(10000)))}",
                "_source": filterKeys(document),
            }


# Se agrupa por:<br>
# * Sistema operativo<br>
# * Tipo dispositivo<br>
# * Marca del terminal<br>
# * Tecnoloía terminal<br>
# <br>
# Se cuentan las mac de usuario para cada site id y fecha control

# In[27]:


try:
    totalesDispositivos = datos_det_conex[["fecha_control","site_id"
                                 ,"usuarios.tablero08.sistemaOperativoUsuarios"
                                 ,'usuarios.tablero08.tipoDispositivoUsuarios'
                                 ,'manufacturer'
                                 ,'radio.band'
                                 ,"mac_usuario"]].groupby(["fecha_control","site_id", 'location'
                                                     ,"usuarios.tablero08.sistemaOperativoUsuarios"
                                                     ,'usuarios.tablero08.tipoDispositivoUsuarios'
                                                     ,'manufacturer'
                                                     ,'radio.band']).agg['mac_usuario'].nunique().reset_index()
    totalesDispositivos.columns = totalesDispositivos.columns.droplevel(1)
    totalesDispositivos= totalesDispositivos.rename(columns={'mac_usuario' : 'usuarios.tablero08.totales.dispositivos'
                                                            ,'fecha_control' : 'usuarios.tablero08.fechaControl'
                                                            ,'manufacturer' : 'usuarios.tablero08.marcaTerminal'
                                                            ,'radio.band' : 'usuarios.tablero08.tecnologiaTerminal'
                                                            })
    totalesDispositivos = pd.merge(totalesDispositivos, datos_semilla, on='site_id',how='inner')
    totalesDispositivos["usuarios.tablero08.fecha"] = totalesDispositivos["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    totalesDispositivos["usuarios.tablero08.anyo"] = totalesDispositivos["usuarios.tablero08.fecha"].str[0:4]
    totalesDispositivos["usuarios.tablero08.mes"] = totalesDispositivos["usuarios.tablero08.fecha"].str[5:7]
    totalesDispositivos["usuarios.tablero08.dia"] = totalesDispositivos["usuarios.tablero08.fecha"].str[8:10]
    totalesDispositivos["usuarios.tablero08.hora"] = totalesDispositivos["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    totalesDispositivos["usuarios.tablero08.minuto"] = totalesDispositivos["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    totalesDispositivos= totalesDispositivos.rename(columns={'site_id' : 'usuarios.tablero08.siteID'})
    totalesDispositivos['nombreDepartamento'] = totalesDispositivos['usuarios.tablero08.nombreDepartamento']
    totalesDispositivos['nombreMunicipio'] = totalesDispositivos['usuarios.tablero08.nombreMunicipio']
    totalesDispositivos['idBeneficiario'] = totalesDispositivos['usuarios.tablero08.idBeneficiario']
    totalesDispositivos['fecha'] = totalesDispositivos['usuarios.tablero08.fecha']
    totalesDispositivos['anyo'] = totalesDispositivos['usuarios.tablero08.anyo']
    totalesDispositivos['mes'] = totalesDispositivos['usuarios.tablero08.mes']
    totalesDispositivos['dia'] = totalesDispositivos['usuarios.tablero08.dia']
    totalesDispositivos['@timestamp'] = now.isoformat()
except:
    pass 


# ### Se lee el índice all-cambium-device-client

# * En este indice se guarda el detalle de los radio por fecha<br>
# * Detalle conexiones cruza con device clients. Con estos se calculan los totales por marca

# In[28]:


query = {
    "_source": ['mac', 'ap_mac', 'radio.band', 'radio.rx_bytes', 'radio.tx_bytes','fecha_control']
      , "query": {
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


# # Uso servicio de internet insertados en índice principal

# La siguiente lista use_these_keys se usa para referenciar cuales campos del dataframe irán al índice final. si los datos no se declaran en este, no se insertarán

# In[29]:


use_these_keys = ['usuarios.tablero08.macRed'
                  , 'usuarios.tablero08.usoServicioInternetSitio'
                  , 'usuarios.tablero08.detallesTecnologiasTerminales'
                  , 'usuarios.tablero08.nomCentroDigital'
                  , 'usuarios.tablero08.codISO'
                  , 'usuarios.tablero08.idBeneficiario'
                  , 'usuarios.tablero08.localidad'
                  , 'usuarios.tablero08.siteID'
                  , 'usuarios.tablero08.nombreDepartamento'
                  , 'usuarios.tablero08.sistemaEnergia'
                  , 'usuarios.tablero08.nombreMunicipio'
                  , 'usuarios.tablero08.location'
                  , 'usuarios.tablero08.fechaControl'
                  , 'usuarios.tablero08.fecha'
                  , 'usuarios.tablero08.anyo'
                  , 'usuarios.tablero08.mes'
                  , 'usuarios.tablero08.dia'
                  , 'usuarios.tablero08.hora'
                  , 'usuarios.tablero08.minuto'
                  , 'usuarios.tablero08.apGroup'
                  , 'nombreDepartamento'
                    , 'nombreMunicipio'
                    , 'idBeneficiario'
                    , 'fecha'
                    , 'anyo'
                    , 'mes'
                    , 'dia'
                  , '@timestamp']


# Se genera el documento a ser insertado y se define el ID del documento combinando los siguientes campos para generar un ID único:
#     
# - UsoServicio-usuarios.tablero08.macRed-usuarios.tablero08.detallesTecnologiasTerminales-usuarios.tablero08.fechaControl

# In[30]:


def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{ 'UsoServicio-' + str(document['usuarios.tablero08.macRed']) + '-' + str(document['usuarios.tablero08.detallesTecnologiasTerminales']) + '-' + str(document['usuarios.tablero08.fechaControl'])}",
                "_source": filterKeys(document),
            }


# * Se toma el máximo radio para cada mac de dispositivo final
# * Se suma agrupando por ap_mac, banda y fecha
# * El resultado es un total para cada banda, fecha y ap_mac

# In[31]:


try:    
    datos_radio['fecha_control'] = datos_radio['fecha_control'].str[0:15]+'0:00'
    datos_radio = datos_radio.groupby(['ap_mac', 'fecha_control', 'mac', 'radio.band']).agg(['max']).reset_index()
    datos_radio.columns = datos_radio.columns.droplevel(1)
    datos_radio = datos_radio[['ap_mac', 'fecha_control', 'radio.band', 'radio.rx_bytes',
           'radio.tx_bytes']].groupby(['ap_mac', 'fecha_control', 'radio.band']).agg(['sum']).reset_index()
    datos_radio.columns = datos_radio.columns.droplevel(1)
    datos_radio = datos_radio.rename(columns={'ap_mac' : 'usuarios.tablero08.macRed'})
    usoServicioInternetSitio = pd.merge(datos_radio,datos_dev[['usuarios.apGroup', 'site_id', 'usuarios.tablero08.macRed']], on ='usuarios.tablero08.macRed', how='inner')
    usoServicioInternetSitio.fillna({'radio.rx_bytes':0,'radio.tx_bytes':0},inplace=True)
    usoServicioInternetSitio.fillna({'usuarios.tablero08.macRed':'','radio.band':'No identificado','usuarios.tablero08.apGroup':'No identificado'},inplace=True)
    usoServicioInternetSitio['usuarios.tablero08.usoServicioInternetSitio'] = usoServicioInternetSitio['radio.rx_bytes'].astype(float) + usoServicioInternetSitio['radio.tx_bytes'].astype(float)
    usoServicioInternetSitio = pd.merge(usoServicioInternetSitio,datos_semilla, on ='site_id', how='inner')
    usoServicioInternetSitio= usoServicioInternetSitio.rename(columns={'site_id' : 'usuarios.tablero08.siteID'
                                                                    ,'radio.band' : 'usuarios.tablero08.detallesTecnologiasTerminales'
                                                                    , 'fecha_control' : 'usuarios.tablero08.fechaControl'})
    usoServicioInternetSitio['usuarios.tablero08.usoServicioInternetSitio'] = round((usoServicioInternetSitio['usuarios.tablero08.usoServicioInternetSitio']/float(1<<30)),6) 
    usoServicioInternetSitio["usuarios.tablero08.fecha"] = usoServicioInternetSitio["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    usoServicioInternetSitio["usuarios.tablero08.anyo"] = usoServicioInternetSitio["usuarios.tablero08.fecha"].str[0:4]
    usoServicioInternetSitio["usuarios.tablero08.mes"] = usoServicioInternetSitio["usuarios.tablero08.fecha"].str[5:7]
    usoServicioInternetSitio["usuarios.tablero08.dia"] = usoServicioInternetSitio["usuarios.tablero08.fecha"].str[8:10]
    usoServicioInternetSitio["usuarios.tablero08.hora"] = usoServicioInternetSitio["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    usoServicioInternetSitio["usuarios.tablero08.minuto"] = usoServicioInternetSitio["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    
    usoServicioInternetSitio['nombreDepartamento'] = usoServicioInternetSitio['usuarios.tablero08.nombreDepartamento']
    usoServicioInternetSitio['nombreMunicipio'] = usoServicioInternetSitio['usuarios.tablero08.nombreMunicipio']
    usoServicioInternetSitio['idBeneficiario'] = usoServicioInternetSitio['usuarios.tablero08.idBeneficiario']
    usoServicioInternetSitio['fecha'] = usoServicioInternetSitio['usuarios.tablero08.fecha']
    usoServicioInternetSitio['anyo'] = usoServicioInternetSitio['usuarios.tablero08.anyo']
    usoServicioInternetSitio['mes'] = usoServicioInternetSitio['usuarios.tablero08.mes']
    usoServicioInternetSitio['dia'] = usoServicioInternetSitio['usuarios.tablero08.dia']
    
    usoServicioInternetSitio['@timestamp'] = now.isoformat()

except:
    pass


# # Marcando si usuario es nuevo o recurrente

# * Se toma del dataframe datos_det_conex y se cuenta las veces que cada mac_usuario aparece<br>
# * Luego se suma con el historico de conexiones por usuario (indice intermedio ohmyfi-total-conexiones-historico)<br>
# * marca como Nuevo(1 conexión). <br>
# <br>
# De este proceso se obtiene el campo:<br>
# * usuarios.usuariosNuevos<br>
# * usuarios.totales.usuariosNuevos

# In[32]:


try:
    datos_recurrencia = datos_det_conex[["fecha","tipodoc","documento"]].groupby(["tipodoc","documento"]).agg(['count']).reset_index()
    datos_recurrencia.columns = datos_recurrencia.columns.droplevel(1)
    datos_recurrencia= datos_recurrencia.rename(columns={'fecha' : 'usuarios_recurrencia_aux'})
    datos_recurrencia['tipodoc'] = datos_recurrencia['tipodoc'].replace('','No especificado')
    datos_recurrencia['documento'] = datos_recurrencia['documento'].replace('','No especificado')
except:
    pass


# ### Se lee histórico 

# Se lee ohmyfi-total-conexiones-histórico, el cual es un índice creado a partir de los datos crudos. Este guarda la cantidad de conexiones realizadas por ese usuario. Para que la consulta funcione correctamente se debe colocar el filtro que usuarios_recurrencia exista.

# In[33]:


try:
    
    query = {
        "_source": ['tipodoc', 'documento', 'usuarios_recurrencia'], 
        "query": {
            "bool": {
                "filter": [{
                    "exists": {
                        "field":"usuarios_recurrencia"
                    }
                }]
            }
        }
    }

    ya_en_indice = custom_scan(
        query, 
        parametros.ohmyfi_total_c_index,
        total_docs=30000000, 
        client=es
    )
except:
    ya_en_indice = pd.DataFrame(columns=['tipodoc', 'documento', 'usuarios_recurrencia'])


# In[34]:


if ya_en_indice is None or ya_en_indice.empty:
    datos_recurrencia= datos_recurrencia.rename(columns={'usuarios_recurrencia_aux' : 'usuarios_recurrencia'})
else:
    datos_recurrencia = pd.merge(datos_recurrencia, ya_en_indice, on=['tipodoc', 'documento'],how='left')
    datos_recurrencia.fillna({'usuarios_recurrencia':0},inplace=True)
    datos_recurrencia['usuarios_recurrencia'] = datos_recurrencia['usuarios_recurrencia'] + datos_recurrencia['usuarios_recurrencia_aux']
    datos_recurrencia['usuarios_recurrencia'] = [int(x) for x in datos_recurrencia['usuarios_recurrencia']]


# In[35]:


try:
    aux_recurrencia = pd.merge(datos_recurrencia,datos_det_conex, on=['tipodoc','documento'],how='inner' )
    aux_recurrencia = aux_recurrencia[(aux_recurrencia['usuarios_recurrencia']==1)]
    aux_recurrencia.loc[aux_recurrencia['usuarios_recurrencia']==1,'usuarios.tablero08.usuariosNuevos']='Nuevo'
    aux_recurrencia = aux_recurrencia.rename(columns={'usuarios_recurrencia' :'usuarios.tablero08.sesiones_Usuarios'})
    aux_recurrencia = aux_recurrencia[["fecha_control","site_id","usuarios.tablero08.usuariosNuevos",'documento']].groupby(["fecha_control","site_id","usuarios.tablero08.usuariosNuevos"])['documento'].nunique().reset_index()
    aux_recurrencia= aux_recurrencia.rename(columns={'documento' : 'usuarios.tablero08.totales.usuariosNuevos'})
except:
    pass


# In[36]:


try:
    datos_recurrencia = pd.merge(datos_semilla,  aux_recurrencia, on='site_id', how='inner')
    datos_recurrencia = datos_recurrencia.rename(columns={'fecha_control' : 'usuarios.tablero08.fechaControl'})
    datos_recurrencia["usuarios.tablero08.fecha"] = datos_recurrencia["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    datos_recurrencia["usuarios.tablero08.anyo"] = datos_recurrencia["usuarios.tablero08.fecha"].str[0:4]
    datos_recurrencia["usuarios.tablero08.mes"] = datos_recurrencia["usuarios.tablero08.fecha"].str[5:7]
    datos_recurrencia["usuarios.tablero08.dia"] = datos_recurrencia["usuarios.tablero08.fecha"].str[8:10]
    datos_recurrencia["usuarios.tablero08.hora"] = datos_recurrencia["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    datos_recurrencia["usuarios.tablero08.minuto"] = datos_recurrencia["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    datos_recurrencia= datos_recurrencia.rename(columns={'site_id' : 'usuarios.tablero08.siteID'})
    
    datos_recurrencia['nombreDepartamento'] = datos_recurrencia['usuarios.tablero08.nombreDepartamento']
    datos_recurrencia['nombreMunicipio'] = datos_recurrencia['usuarios.tablero08.nombreMunicipio']
    datos_recurrencia['idBeneficiario'] = datos_recurrencia['usuarios.tablero08.idBeneficiario']
    datos_recurrencia['fecha'] = datos_recurrencia['usuarios.tablero08.fecha']
    datos_recurrencia['anyo'] = datos_recurrencia['usuarios.tablero08.anyo']
    datos_recurrencia['mes'] = datos_recurrencia['usuarios.tablero08.mes']
    datos_recurrencia['dia'] = datos_recurrencia['usuarios.tablero08.dia']
except:
    pass


# # Insertando recurrencia de usuario en indice principal

# 
# La lista use_these_keys se usa para referenciar cuales campos del dataframe irán al índice final. Si los datos no se declaran en este, no se insertarán
# 

# In[37]:


try:
    use_these_keys = ['usuarios.tablero08.nomCentroDigital'
                      , 'usuarios.tablero08.codISO'
                      , 'usuarios.tablero08.idBeneficiario'
                      , 'usuarios.tablero08.localidad'
                      , 'usuarios.tablero08.siteID'
                      , 'usuarios.tablero08.nombreDepartamento'
                      , 'usuarios.tablero08.sistemaEnergia'
                      , 'usuarios.tablero08.nombreMunicipio'
                      , 'usuarios.tablero08.location'
                      , 'usuarios.tablero08.usuariosNuevos'
                      , 'usuarios.tablero08.totales.usuariosNuevos'
                      , 'usuarios.tablero08.fechaControl'
                      , 'usuarios.tablero08.fecha'
                      , 'usuarios.tablero08.anyo'
                      , 'usuarios.tablero08.mes'
                      , 'usuarios.tablero08.dia'
                      , 'usuarios.tablero08.hora'
                      , 'usuarios.tablero08.minuto'
                        , 'nombreDepartamento'
                        , 'nombreMunicipio'
                        , 'idBeneficiario'
                        , 'fecha'
                        , 'anyo'
                        , 'mes'
                        , 'dia'
                      , '@timestamp']
    datos_recurrencia['@timestamp'] = now.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'Recurrencia-' + str(document['usuarios.tablero08.siteID']) + '-' + str(document['usuarios.tablero08.fechaControl'])}",
                    "_source": filterKeys(document),
                }
except:
    pass


# ## Insertando detalle MACs

# In[38]:


use_these_keys = ['usuarios.tablero08.siteID'
                  , 'usuarios.tablero08.nomCentroDigital'
                  , 'usuarios.tablero08.codISO'
                  , 'usuarios.tablero08.localidad'
                  , 'usuarios.tablero08.nombreDepartamento'
                  , 'usuarios.tablero08.sistemaEnergia'
                  , 'usuarios.tablero08.nombreMunicipio'
                  , 'usuarios.tablero08.idBeneficiario'
                  , 'usuarios.tablero08.location'
                  , 'usuarios.tablero08.macRed'
                  , 'usuarios.tablero08.apGroup'
                  , 'usuarios.tablero08.dispositivo.mac'
                  , 'usuarios.tablero08.dispositivo.tipo'
                  , 'usuarios.tablero08.dispositivo.nombre'
                  , 'usuarios.tablero08.dispositivo.marca'
                  , 'usuarios.tablero08.dispositivo.tecnologia'
                  , 'usuarios.tablero08.dispositivo.sisOperativo'
                  , 'usuarios.tablero08.fechaControl'
                  , 'usuarios.tablero08.fecha'
                  , 'usuarios.tablero08.anyo'
                  , 'usuarios.tablero08.mes'
                  , 'usuarios.tablero08.dia'
                  , 'usuarios.tablero08.hora'
                  , 'usuarios.tablero08.minuto'
                  , '@timestamp']
def doc_generator_mac(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{ 'detalleMAC-' + str(document['usuarios.tablero08.siteID']) + '-' + str(document['usuarios.tablero08.fechaControl']) + '-' + str(document['usuarios.tablero08.dispositivo.mac'])   }",
                "_source": filterKeys(document),
            }


# In[39]:


try:
    dispositivoUsuarios = datos_det_conex[['site_id','usuarios.tablero08.macRed','usuarios.tablero08.apGroup'
                                ,'mac_usuario','fecha_control'
                                ,'usuarios.tablero08.tipoDispositivoUsuarios'
                                ,'name','manufacturer','radio.band'
                                ,'usuarios.tablero08.sistemaOperativoUsuarios']].drop_duplicates(subset=['mac_usuario'])
    dispositivoUsuarios.fillna({'usuarios.tablero08.tipoDispositivoUsuarios':'No identificado'
                               ,'name':'No identificado'
                               ,'manufacturer':'No identificado'
                               ,'radio.band':'No identificado'
                               ,'usuarios.tablero08.sistemaOperativoUsuarios':'No identificado'
                               ,'usuarios.tablero08.macRed':'No identificado'
                               ,'usuarios.tablero08.apGroup':'No identificado'
                               },inplace=True)
    dispositivoUsuarios = pd.merge(dispositivoUsuarios, datos_semilla, on='site_id',how='inner')
    dispositivoUsuarios= dispositivoUsuarios.rename(columns={
                                                     'site_id' : 'usuarios.tablero08.siteID'
                                                    ,'nombreSede' : 'usuarios.tablero08.nomCentroDigital'
                                                    ,'mac_usuario' : 'usuarios.tablero08.dispositivo.mac'
                                                    ,'usuarios.tablero08.tipoDispositivoUsuarios' : 'usuarios.tablero08.dispositivo.tipo'
                                                    ,'name' : 'usuarios.tablero08.dispositivo.nombre'
                                                    ,'manufacturer' : 'usuarios.tablero08.dispositivo.marca'
                                                    ,'radio.band' : 'usuarios.tablero08.dispositivo.tecnologia'            
                                                    ,'usuarios.tablero08.sistemaOperativoUsuarios' : 'usuarios.tablero08.dispositivo.sisOperativo'
                                                    ,'fecha_control':'usuarios.tablero08.fechaControl'
                                                    })
    dispositivoUsuarios["usuarios.tablero08.fecha"] = dispositivoUsuarios["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    dispositivoUsuarios["usuarios.tablero08.anyo"] = dispositivoUsuarios["usuarios.tablero08.fecha"].str[0:4]
    dispositivoUsuarios["usuarios.tablero08.mes"] = dispositivoUsuarios["usuarios.tablero08.fecha"].str[5:7]
    dispositivoUsuarios["usuarios.tablero08.dia"] = dispositivoUsuarios["usuarios.tablero08.fecha"].str[8:10]
    dispositivoUsuarios["usuarios.tablero08.hora"] = dispositivoUsuarios["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    dispositivoUsuarios["usuarios.tablero08.minuto"] = dispositivoUsuarios["usuarios.tablero08.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    dispositivoUsuarios['@timestamp'] = now.isoformat()
    salida = helpers.bulk(es, doc_generator_mac(dispositivoUsuarios))
    print("Fecha: ", now,"- Detalle MACs a indice:",salida[0])
except:
    print("Fecha: ", now,"- Ningun Detalle MACs para insertar en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[40]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia-tablero08',
        body = { 'jerarquia-tablero08': 'jerarquia-tablero08','tablero08.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)

