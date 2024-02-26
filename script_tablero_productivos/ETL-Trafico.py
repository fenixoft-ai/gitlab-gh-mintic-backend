#!/usr/bin/env python
# coding: utf-8

# ¿Que hace este script?
# * Calcula para cada dispositivo de red: trafico.totales.traficoIN, trafico.totales.traficoOUT', trafico.totales.consumoGB'.
# * Calcula para cada site_id: trafico.totales.sitio.traficoIN, trafico.totales.sitio.traficoOUT, trafico.totales.sitio.consumoGB

# In[742]:


from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import parametros
import random
import re


# ## Conectando a ElasticSearch

# La ultima línea se utiliza para garantizar la ejecución de la consulta
# * timeout es el tiempo para cada ejecución
# * max_retries el número de intentos si la conexión falla
# * retry_on_timeout para activar los reitentos

# In[743]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=173, max_retries=3, retry_on_timeout=True
) 


# ### Calculando fechas para la ejecución

# * Se calculan las fechas para asociar al nombre del indice
# * fecha_hoy es usada para concatenar al nombre del indice principal previa inserción

# In[744]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### Definiendo indice principal con fecha de hoy

# Estos valores se deben ajustar según ambiente. No es automático ya que no hay separación de ambientes

# In[745]:


indice = parametros.trafico_tableros_trafico_index
indice_control = parametros.tableros_mintic_control


# ### Función para generar JSON compatible con ES

# In[746]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# 
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud, COD_ISO, entre otros).

# In[747]:


total_docs = 10000
try:
    response = es.search(
        index= parametros.semilla_inventario_index,
        body={
               "_source": ['site_id','nombre_municipio', 'nombre_departamento', 'nombre_centro_pob', 'nombreSede' 
                           , 'energiadesc', 'latitud', 'longitud','COD_ISO','id_Beneficiario']
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
except:
    print("fecha:",now,"- Error en lectura de datos semilla")
    #exit()
    


# In[748]:


datos_semilla['site_id'] = datos_semilla['site_id'].str.strip()


# Se valida latitud y longitud, se genera campo location y se renombran los campos de semilla

# In[749]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))

datos_semilla['trafico.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['trafico.location']=datos_semilla['trafico.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)

datos_semilla = datos_semilla.rename(columns={'nombre_municipio': 'trafico.nombreMunicipio'
                                              , 'nombre_departamento' : 'trafico.nombreDepartamento'
                                              , 'nombre_centro_pob': 'trafico.localidad'
                                              , 'nombreSede' : 'trafico.nomCentroDigital'
                                              , 'energiadesc' : 'trafico.sistemaEnergia'
                                              , 'COD_ISO' : 'trafico.codISO'
                                              , 'id_Beneficiario' : 'trafico.idBeneficiario'})
datos_semilla.fillna('', inplace=True)


# In[750]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["trafico.location"]=='')].index)


# ### leyendo indice cambium-devicedevices

# Se lee la información de los dispositivos de red monitoreados por Cambium. En esta lectura no hay referencia de fechas ya que solo hay una ocurrencia por MAC de dispositivo de red.
# 
# * site_id es la llave para cruzar con cada centro de conexión.
# * mac, IP y name son datos básicos del dispositivo.
# * ap_group identifica los dispositivos como INDOOR u OUTDOOR

# In[751]:


total_docs = 30000
try:
    response = es.search(
        index= parametros.cambium_d_d_index,
        body={
                    "_source": ["site_id","mac","ip","ap_group","name"]  
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


# In[752]:


datos_dev['site_id'] = datos_dev['site_id'].str.strip()


# Se descartan registros con site_id vacios y se limpian los NaN del dataframe

# In[753]:


datos_dev.dropna(subset=['site_id'])
datos_dev.fillna('', inplace=True)
datos_dev = datos_dev.drop(datos_dev[(datos_dev['site_id']=='')].index)
datos_dev.sort_values(['site_id','ap_group'], inplace=True)


# Se limpian datos mal formados de ap_group

# In[754]:


datos_dev['ap_group'] = datos_dev['ap_group'].str.split("-", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split("_", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split(".", n = 1, expand = True)[0]
datos_dev = datos_dev.drop(datos_dev[(datos_dev['ap_group']=='')].index)


# In[755]:


datos_dev = datos_dev.drop_duplicates('mac')


# Se renombran campos según formato del indice final

# In[756]:


datos_dev = datos_dev.rename(columns={'ap_group': 'trafico.apGroup'
                                        , 'ip': 'trafico.IP'
                                        , 'mac' : 'trafico.macRed'
                                        , 'name' : 'trafico.deviceName'})


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[757]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
               "_source": ["trafico.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia_tablero_trafico"
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
        fecha_ejecucion = doc["_source"]['trafico.fechaControl']
except:
    fecha_ejecucion = '2021-05-01 00:00:00'
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-05-01 00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ## Se lee la información de cambium device performance

#  Se toma los valores de dispositivos de red y su desempeño.
#  * mac del dispositivo de red
#  * timestamp es la fecha y hora de la medición
#  * radio.* volumen de datos descargados(r) y cargados(t)

# In[758]:


def traePerformance(fecha_max,fecha_tope):
    total_docs = 5000000
    response = es.search(
        index= parametros.cambium_d_p_index,
        body={
                "_source": ["mac","timestamp","radio.5ghz.rx_bps",
                           "radio.5ghz.tx_bps","radio.24ghz.rx_bps"
                          ,"radio.24ghz.tx_bps"]
              , "query": {
                  "range": {
                    "timestamp": {
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


# ### Lanzando  ejecución de consulta

# * Se calcula rango en base a la fecha de control. Para este caso es de 10 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.

# In[759]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_performance = traePerformance(fecha_max_mintic,fecha_tope_mintic)

if datos_performance is None or datos_performance.empty:
    while (datos_performance is None or datos_performance.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        datos_performance = traePerformance(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# funcion para insertar en indice: 

# In[760]:


use_these_keys = ['trafico.nomCentroDigital',
                  'trafico.codISO',
                  'trafico.localidad',
                  'trafico.siteID',
                  'trafico.nombreDepartamento',
                  'trafico.sistemaEnergia',
                  'trafico.nombreMunicipio',
                  'trafico.idBeneficiario',
                  'trafico.location',
                  'trafico.apGroup',
                  'trafico.IP',
                  'trafico.deviceName',
                  'trafico.macRed',
                  #'trafico.status.macRed',
                  'trafico.totales.fechaControl',
                  'trafico.totales.traficoIN',
                  'trafico.totales.traficoOUT',
                  'trafico.totales.consumoGB',
                  'trafico.totales.promedioIN',
                  'trafico.totales.promedioOUT',
                  'trafico.totales.promedioConsumo',
                  'trafico.totales.fecha',
                  'trafico.totales.anyo',
                  'trafico.totales.mes',
                  'trafico.totales.dia',
                  'trafico.totales.hora',
                  'trafico.totales.minuto',
                  'nombreDepartamento',
                    'nombreMunicipio',
                    'idBeneficiario',
                    'fecha',
                    'anyo',
                    'mes',
                    'dia',
                  '@timestamp']
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{'consumo-' + document['trafico.siteID'] + '-' + document['trafico.macRed'] + '-' +document['trafico.totales.fechaControl']+'-'+str(random.randrange(10000))}",
                "_source": filterKeys(document),
            }


# # Insertando consumo a indice 

# * Calculo de indicadores de consumos para cada AP. 
# * Se toman los valores de rx y tx como descarga y carga
# * de datos_dev se toma el site_id y el ap_group

# In[761]:


try:
    datos_performance.drop_duplicates(inplace=True)
    datos_performance['fecha_control'] = datos_performance["timestamp"].str[0:-4] + '0:00'
    datos_performance.rename(columns={'mac': 'trafico.macRed'}, inplace=True)
    datos_performance.replace('','0',inplace=True)
    datos_performance.fillna({'radio.5ghz.rx_bps':0, 'radio.5ghz.tx_bps':0,
                      'radio.24ghz.rx_bps':0, 'radio.24ghz.tx_bps':0 },inplace=True)
    datos_performance[['radio.5ghz.rx_bps','radio.5ghz.tx_bps','radio.24ghz.rx_bps','radio.24ghz.tx_bps']] = datos_performance[['radio.5ghz.rx_bps','radio.5ghz.tx_bps','radio.24ghz.rx_bps','radio.24ghz.tx_bps']].astype(int)

    aux_performance=datos_performance[['trafico.macRed','fecha_control'
                                       ,'radio.5ghz.rx_bps'
                                       ,'radio.5ghz.tx_bps'
                                       ,'radio.24ghz.rx_bps'
                                       ,'radio.24ghz.tx_bps']].groupby(['trafico.macRed','fecha_control']).agg(['sum']).reset_index()
    aux_performance.columns = aux_performance.columns.droplevel(1)
    aux_performance['trafico.totales.traficoIN_aux'] = aux_performance['radio.5ghz.rx_bps'] + aux_performance['radio.24ghz.rx_bps']
    aux_performance['trafico.totales.traficoOUT_aux'] = aux_performance['radio.5ghz.tx_bps'] + aux_performance['radio.24ghz.tx_bps']
    
    aux_performance = pd.merge(aux_performance, datos_dev, on='trafico.macRed',how='inner')
    mintic_02 = pd.merge(datos_semilla,  aux_performance, on='site_id',how='inner')
    
        ## Calculo del promedio 
    aux_performance['trafico.totales.promedioIN'] = round((aux_performance['trafico.totales.traficoIN_aux']/float(1<<30)),6)
    aux_performance['trafico.totales.promedioOUT'] = round((aux_performance['trafico.totales.traficoOUT_aux']/float(1<<30)),6)
    aux_performance['trafico.totales.promedioConsumo'] = aux_performance['trafico.totales.promedioIN'] + aux_performance['trafico.totales.promedioOUT']
    #Se totaliza para la sede completa
    aux_performance['trafico.totales.promedioConsumo'] = round(aux_performance['trafico.totales.promedioConsumo'],6)
    
    #La información de tráfico se convierte a GigaBytes y se toman 6 decimales
    mintic_02['trafico.totales.traficoIN'] = round((mintic_02['trafico.totales.traficoIN_aux']/float(1<<30)),6)
    mintic_02['trafico.totales.traficoOUT'] = round((mintic_02['trafico.totales.traficoOUT_aux']/float(1<<30)),6)
    mintic_02['trafico.totales.consumoGB'] = mintic_02['trafico.totales.traficoIN'] + mintic_02['trafico.totales.traficoOUT']
    #Se totaliza entrante y saliente
    mintic_02['trafico.totales.consumoGB'] = round(mintic_02['trafico.totales.consumoGB'],6)

    ### Generando columnas con fecha, anyo, mes, dia, hora y minuto por separado
    mintic_02["trafico.totales.fecha"] = mintic_02["fecha_control"].str.split(" ", n = 1, expand = True)[0]
    mintic_02["trafico.totales.hora"] = mintic_02["fecha_control"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    mintic_02["trafico.totales.minuto"] = mintic_02["fecha_control"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    mintic_02["trafico.totales.anyo"] = mintic_02["trafico.totales.fecha"].str[0:4]
    mintic_02["trafico.totales.mes"] = mintic_02["trafico.totales.fecha"].str[5:7]
    mintic_02["trafico.totales.dia"] = mintic_02["trafico.totales.fecha"].str[8:10]
    ### Renombrado de campos
    mintic_02.rename(columns={'site_id': 'trafico.siteID'
                             ,'fecha_control' : 'trafico.totales.fechaControl'}, inplace=True)

    
    aux_performance=aux_performance.rename(columns={'site_id' : 'trafico.siteID',                                                 'fecha_control':'trafico.totales.fechaControl'})


    ##nulos a cero
    mintic_02.fillna({'trafico.totales.consumoGB':0,
                      'trafico.totales.traficoIN':0,
                      'trafico.totales.traficoOUT':0
                      },inplace=True)
    #cambia valores a tipo float
    mintic_02[['trafico.totales.consumoGB','trafico.totales.traficoIN','trafico.totales.traficoOUT']] = mintic_02[['trafico.totales.consumoGB','trafico.totales.traficoIN','trafico.totales.traficoOUT']].astype(float)
    
    mintic_02['nombreDepartamento'] = mintic_02['trafico.nombreDepartamento']
    mintic_02['nombreMunicipio'] = mintic_02['trafico.nombreMunicipio']
    mintic_02['idBeneficiario'] = mintic_02['trafico.idBeneficiario']
    mintic_02['fecha'] = mintic_02['trafico.totales.fecha']
    mintic_02['anyo'] = mintic_02['trafico.totales.anyo']
    mintic_02['mes'] = mintic_02['trafico.totales.mes']
    mintic_02['dia'] = mintic_02['trafico.totales.dia']
    mintic_02['@timestamp'] = now.isoformat()
    
    aux_performance = aux_performance[['trafico.siteID'
                              ,'trafico.macRed'
                              ,'trafico.totales.fechaControl'
                              ,'trafico.totales.promedioIN'
                              ,'trafico.totales.promedioOUT'
                              ,'trafico.totales.promedioConsumo']]
    
    mintic_02 = pd.merge(mintic_02,aux_performance, 
                        on=['trafico.siteID',
                            'trafico.macRed',
                            'trafico.totales.fechaControl'],how='left')

    salida = helpers.bulk(es, doc_generator(mintic_02))
    print("Fecha: ", now,"- Datos Trafico Consumos en indice principal:",salida[0])
except Exception as e:
    print(e)
    print("Fecha: ", now,"- No se insertaron datos de consumos en indice principal")


# ## En otra  jerarquía se escriben los dispositivos conectados

# Se toma el dataframe del proceso anterior para realizar el siguiente proceso:
# * Se cuentan la cantidad de dispositivos WAN/LAN (OUTDOOR/INDOOR), agrupando por fecha y sitio
# * Cantidad dispositivos WAN/LAN conectados se calcula validando cuando son OUTDOOR/INDOOR y tienen trafico(traficoOUT)
# * Cantidad de dispositivos desconectados, restando los dos anteriores
# 
# Datos generados
# * trafico.totales.cantDevWAN
# * trafico.totales.cantDevLAN
# * trafico.totales.cantDevConectadosWAN
# * trafico.totales.cantDevDesconectadosWAN
# * trafico.totales.cantDevConectadosLAN
# * trafico.totales.cantDevDesconectadosLAN
# * trafico.totales.cantDev

# In[762]:


try:
    mintic_03 = mintic_02[['trafico.nomCentroDigital',
                      'trafico.codISO',
                      'trafico.localidad',
                      'trafico.siteID',
                      'trafico.nombreDepartamento',
                      'trafico.sistemaEnergia',
                      'trafico.nombreMunicipio',
                      'trafico.idBeneficiario',
                      'trafico.location',
                      'trafico.apGroup',
                      'trafico.macRed',
                      'trafico.totales.fechaControl',
                      'trafico.totales.fecha',
                      'trafico.totales.anyo',
                      'trafico.totales.mes',
                      'trafico.totales.dia',
                      'trafico.totales.hora',
                      'trafico.totales.minuto',
                      'trafico.totales.traficoIN',
                      'trafico.totales.traficoOUT',
                      'trafico.totales.consumoGB',
                      'trafico.totales.promedioIN',
                      'trafico.totales.promedioOUT',
                      'trafico.totales.promedioConsumo']].groupby(['trafico.nomCentroDigital',
                                                              'trafico.codISO',
                                                              'trafico.localidad',
                                                              'trafico.siteID',
                                                              'trafico.nombreDepartamento',
                                                              'trafico.sistemaEnergia',
                                                              'trafico.nombreMunicipio',
                                                              'trafico.idBeneficiario',
                                                              'trafico.location',
                                                              'trafico.apGroup',
                                                              'trafico.macRed',
                                                              'trafico.totales.fechaControl',
                                                              'trafico.totales.fecha',
                                                              'trafico.totales.anyo',
                                                              'trafico.totales.mes',
                                                              'trafico.totales.dia',
                                                              'trafico.totales.hora',
                                                              'trafico.totales.promedioIN',
                                                              'trafico.totales.promedioOUT',
                                                              'trafico.totales.promedioConsumo',
                                                              'trafico.totales.minuto']).agg(['sum']).reset_index()
    mintic_03.columns = mintic_03.columns.droplevel(1)
    mintic_03.rename(columns={'trafico.totales.traficoIN' : 'trafico.totales.sitio.traficoIN'
                          ,'trafico.totales.traficoOUT' : 'trafico.totales.sitio.traficoOUT'
                          ,'trafico.totales.consumoGB' : 'trafico.totales.sitio.consumoGB'
                         }, inplace=True)
except:
    pass


# Funcion para insertar en indice la cantidad de dispositivos conectados 
# * la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán
# 

# In[763]:


use_these_keys = ['trafico.nomCentroDigital',
                  'trafico.codISO',
                  'trafico.localidad',
                  'trafico.siteID',
                  'trafico.nombreDepartamento',
                  'trafico.sistemaEnergia',
                  'trafico.nombreMunicipio',
                  'trafico.idBeneficiario',
                  'trafico.location',
                  'trafico.apGroup',
                  'trafico.totales.fechaControl',
                  'trafico.totales.cantDevWAN',
                  'trafico.totales.cantDevConectadosWAN',
                  'trafico.totales.cantDevDesconectadosWAN',
                  'trafico.totales.cantDevLAN',
                  'trafico.totales.cantDevConectadosLAN',
                  'trafico.totales.cantDevDesconectadosLAN',
                  'trafico.totales.cantDev',
                  'trafico.totales.cantDevConectados',
                  'trafico.totales.cantDevDesconectados',
                  'trafico.totales.fecha',
                  'trafico.totales.anyo',
                  'trafico.totales.mes',
                  'trafico.totales.dia',
                  'trafico.totales.hora',
                  'trafico.totales.minuto',
                  'trafico.totales.sitio.traficoIN',
                  'trafico.totales.sitio.traficoOUT',
                  'trafico.totales.sitio.consumoGB',
                  'trafico.totales.promedioIN',
                  'trafico.totales.promedioOUT',
                  'trafico.totales.promedioConsumo',
                  '@timestamp']

def doc_generator_dis(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{'Conectados-' + document['trafico.siteID'] + '-' +document['trafico.totales.fechaControl']+'-'+str(random.randrange(10000))}",
                "_source": filterKeys(document),
            }


# # Insertando cantidad de dispositivos conectados

# In[764]:


try:
    cantDevWAN = mintic_02[(mintic_02['trafico.apGroup']=='OUTDOOR')][['trafico.totales.fechaControl','trafico.macRed','trafico.siteID']].groupby(['trafico.siteID','trafico.totales.fechaControl'])['trafico.macRed'].nunique().reset_index()    
    cantDevWAN.rename(columns={'trafico.macRed': 'trafico.totales.cantDevWAN'}, inplace=True)
    mintic_03 = pd.merge(mintic_03,  cantDevWAN, on=['trafico.siteID','trafico.totales.fechaControl'],how='left')

    cantDevConectadosWAN = mintic_02[(mintic_02['trafico.totales.traficoOUT']>0) & (mintic_02['trafico.apGroup']=='OUTDOOR')][['trafico.totales.fechaControl','trafico.macRed','trafico.siteID']].groupby(['trafico.siteID','trafico.totales.fechaControl'])['trafico.macRed'].nunique().reset_index()
    cantDevConectadosWAN.rename(columns={'trafico.macRed': 'trafico.totales.cantDevConectadosWAN'}, inplace=True)
    mintic_03 = pd.merge(mintic_03, cantDevConectadosWAN, on=['trafico.siteID','trafico.totales.fechaControl'],how='left')
    mintic_03.fillna({'trafico.totales.cantDevWAN':0,
                      'trafico.totales.cantDevConectadosWAN':0
                      },inplace=True)
    mintic_03['trafico.totales.cantDevDesconectadosWAN'] = mintic_03['trafico.totales.cantDevWAN'] - mintic_03['trafico.totales.cantDevConectadosWAN']

    #La misma lógica se aplica para calcular las cantidades para dispositivos LAN, pero filtrando los INDOOR
    cantDevLAN = mintic_02[(mintic_02['trafico.apGroup']=='INDOOR')][['trafico.totales.fechaControl','trafico.macRed','trafico.siteID']].groupby(['trafico.siteID','trafico.totales.fechaControl'])['trafico.macRed'].nunique().reset_index()
    cantDevLAN.rename(columns={'trafico.macRed': 'trafico.totales.cantDevLAN'}, inplace=True)
    mintic_03 = pd.merge(mintic_03,  cantDevLAN, on=['trafico.siteID','trafico.totales.fechaControl'],how='left')

    cantDevConectadosLAN = mintic_02[~(mintic_02['trafico.totales.traficoIN']>0) & (mintic_02['trafico.apGroup']=='INDOOR')][['trafico.totales.fechaControl','trafico.macRed','trafico.siteID']].groupby(['trafico.siteID','trafico.totales.fechaControl'])['trafico.macRed'].nunique().reset_index()
    cantDevConectadosLAN.rename(columns={'trafico.macRed': 'trafico.totales.cantDevConectadosLAN'}, inplace=True)

    mintic_03 = pd.merge(mintic_03, cantDevConectadosLAN, on=['trafico.siteID','trafico.totales.fechaControl'],how='left')
    mintic_03.fillna({'trafico.totales.cantDevLAN':0,
                      'trafico.totales.cantDevConectadosLAN':0
                      },inplace=True)
    mintic_03['trafico.totales.cantDevDesconectadosLAN'] = mintic_03['trafico.totales.cantDevLAN'] - mintic_03['trafico.totales.cantDevConectadosLAN']

    mintic_03.fillna({'trafico.totales.cantDevConectadosWAN':0,
                      'trafico.totales.cantDevDesconectadosWAN':0,
                      'trafico.totales.cantDevConectadosLAN':0,
                      'trafico.totales.cantDevDesconectadosLAN':0,
                      'trafico.totales.sitio.traficoIN':0,
                      'trafico.totales.sitio.traficoOUT':0,
                      'trafico.totales.sitio.consumoGB':0
                      },inplace=True)
    #cambia valores a tipo float 
    mintic_03[['trafico.totales.sitio.consumoGB','trafico.totales.sitio.traficoIN','trafico.totales.sitio.traficoOUT']] = mintic_03[['trafico.totales.sitio.consumoGB','trafico.totales.sitio.traficoIN','trafico.totales.sitio.traficoOUT']].astype(float)
    
    mintic_03['trafico.totales.cantDev'] = mintic_03['trafico.totales.cantDevLAN'] + mintic_03['trafico.totales.cantDevWAN']
    mintic_03[['trafico.totales.cantDev','trafico.totales.cantDevConectadosWAN','trafico.totales.cantDevDesconectadosWAN','trafico.totales.cantDevConectadosLAN','trafico.totales.cantDevDesconectadosLAN']] = mintic_03[['trafico.totales.cantDev','trafico.totales.cantDevConectadosWAN','trafico.totales.cantDevDesconectadosWAN','trafico.totales.cantDevConectadosLAN','trafico.totales.cantDevDesconectadosLAN']].astype(int)

    mintic_03['trafico.totales.cantDevConectados'] = mintic_03['trafico.totales.cantDevConectadosWAN'] + mintic_03['trafico.totales.cantDevConectadosLAN']
    mintic_03['trafico.totales.cantDevDesconectados'] = mintic_03['trafico.totales.cantDevDesconectadosLAN'] + mintic_03['trafico.totales.cantDevDesconectadosWAN']
    mintic_03[['trafico.totales.cantDevConectados'
              ,'trafico.totales.cantDevDesconectados'
              ,'trafico.totales.cantDevWAN'
              ,'trafico.totales.cantDevLAN']] = mintic_03[['trafico.totales.cantDevConectados'
                                                          ,'trafico.totales.cantDevDesconectados'
                                                          ,'trafico.totales.cantDevWAN'
                                                          ,'trafico.totales.cantDevLAN']].astype(int)    
    mintic_03['@timestamp'] = now.isoformat()
    
    if ('mintic_03' in locals() or 'mintic_03' in globals()) and (not mintic_03.empty):
        pass
    salida = helpers.bulk(es, doc_generator_dis(mintic_03))
    
    print("Fecha: ", now,"- Datos Dispositivos Conectados en indice principal:",salida[0])
except Exception as e:
    print(e)
    print("Fecha: ", now,"- No se insertaron datos de dispositivos conectados en indice principal")


# In[765]:


use_these_keys = ['trafico.nomCentroDigital',
                  'trafico.codISO',
                  'trafico.localidad',
                  'trafico.siteID',
                  'trafico.nombreDepartamento',
                  'trafico.sistemaEnergia',
                  'trafico.nombreMunicipio',
                  'trafico.idBeneficiario',
                  'trafico.location',
                  'trafico.apGroup',
                  'trafico.totales.fechaControl',
                  #'trafico.totales.cantDevWAN',
                  #'trafico.totales.cantDevConectadosWAN',
                  #'trafico.totales.cantDevDesconectadosWAN',
                  #'trafico.totales.cantDevLAN',
                  #'trafico.totales.cantDevConectadosLAN',
                  #'trafico.totales.cantDevDesconectadosLAN',
                  #'trafico.totales.cantDev',
                  #'trafico.totales.cantDevConectados',
                  #'trafico.totales.cantDevDesconectados',
                  'trafico.totales.fecha',
                  'trafico.totales.anyo',
                  'trafico.totales.mes',
                  'trafico.totales.dia',
                  'trafico.totales.hora',
                  'trafico.totales.minuto',
                  #'trafico.totales.sitio.traficoIN',
                  #'trafico.totales.sitio.traficoOUT',
                  #'trafico.totales.sitio.consumoGB',
                  #'trafico.totales.promedioIN',
                  #'trafico.totales.promedioOUT',
                  #'trafico.totales.promedioConsumo',
                  'trafico.totales.totalConexiones',
                  '@timestamp']

def doc_generator_dis(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{'Conectados-' + document['trafico.siteID'] + '-' +document['trafico.totales.fechaControl']+'-'+str(random.randrange(10000))}",
                "_source": filterKeys(document),
            }


# ### Traer datos de ohmyfi-detalleconexiones

# El rango de fechas será definido tomando de referencia la ultima fechahora del indice mintic-concat.
# 
# Campos extaidos:
# * fechahora de la conexión
# * fecha_control es un campo calculado a partir de fechahora. es lo mismo pero con el valor de minuto redondeado a 0.
# * ugar_cod clave para asociar con semilla
# * mac_usuario asociado al dispositivo que realizó la conexión

# In[766]:


def trae_conexiones(fecha_ini,fecha_fin):
    total_docs = 5000000
#     print(fecha_ini)
#     print(fecha_fin)
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
        #, request_timeout=300
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


# ### Se acotan los rangos de fecha por eficiencia

# * Se calcula rango en base a la fecha de control. Para este caso es de 10 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.

# In[767]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_det_conex_completo = trae_conexiones(fecha_max_mintic,fecha_tope_mintic)

if datos_det_conex_completo is None or datos_det_conex_completo.empty:
    while (datos_det_conex_completo is None or datos_det_conex_completo.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        datos_det_conex_completo = trae_conexiones(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[768]:


datos_det_conex_completo['lugar_cod'] = datos_det_conex_completo['lugar_cod'].str.strip()


# In[769]:


datos_det_conex_completo.drop_duplicates(subset=["fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo","sistema_operativo",'tipodoc','documento'],inplace=True)


# ### contanto conexiones por lugar y fecha

# Para cada centro, y fecha control, se cuenta la cantidad de conexiones. Una vez combinado con semilla, se disponibiliza el filtrado por departamento, municipio y centro de conexión. El campo generado es:
# * trafico.totales.totalConexiones

# In[770]:


datos_det_conex=datos_det_conex_completo[["fechahora","fecha_control","lugar_cod"]].groupby(['lugar_cod','fecha_control']).agg(['count']).reset_index()
datos_det_conex.columns = datos_det_conex.columns.droplevel(1)


# ### renombrado de columnas en detalle conexiones

# In[771]:


datos_det_conex = datos_det_conex.rename(columns={'fechahora' : 'trafico.totales.totalConexiones','lugar_cod':'site_id'})


# ### Combinando detalle de conexiones con dispositivos usuario y red

# In[772]:


total_docs = 30000000
response = es.search(
    index= parametros.cambium_d_c_index,
    body={
            "_source": ["mac","ap_mac", 'manufacturer',"radio.band",'radio.rx_bytes','radio.tx_bytes'],
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

datos_dev_clients= pd.DataFrame([x["_source"] for x in elastic_docs])

datos_dev_clients.drop_duplicates(inplace=True)


# * Se renombran campos de clientes
# * se incorpora informaciones de clientes a detalle conexiones

# In[773]:


datos_dev_clients = datos_dev_clients.rename(columns={'ap_mac' : 'trafico.macRed','mac' : 'mac_usuario'})
datos_det_conex_completo = pd.merge(datos_det_conex_completo,  datos_dev_clients, on='mac_usuario',how='left')
datos_det_conex_completo = pd.merge(datos_det_conex_completo,datos_dev[['trafico.macRed','trafico.apGroup','trafico.IP','trafico.deviceName']],on='trafico.macRed', how='left')


# ## calculando total de conexiones

# tiene la información de semilla, total de conexiones, alarmas, performance de AP y recurrencia de usuarios. Para obtener un registro unico para cada site_id, ap_group y fecha_control, se usa el data frame con fechas unicas (fechas_semilla)

# In[774]:


try:
    total_conexiones =  datos_det_conex_completo[["fechahora","fecha_control","lugar_cod"
                                                  ,"trafico.macRed",'trafico.apGroup'
                                                  ,'trafico.IP', 'trafico.deviceName'
                                                 ]].groupby(['lugar_cod','fecha_control'
                                                        ,"trafico.macRed",'trafico.apGroup'
                                                        ,'trafico.IP', 'trafico.deviceName']).agg(['count']).reset_index()
    total_conexiones.columns = total_conexiones.columns.droplevel(1)
    total_conexiones.rename(columns={'fechahora': 'trafico.totales.totalConexiones'
                                    , 'lugar_cod' : 'site_id'}, inplace=True)
    mintic_01 = pd.merge(datos_semilla,  total_conexiones, on='site_id',how='inner')
    mintic_01.fillna({'trafico.totales.totalConexiones': 0}, inplace=True)
    mintic_01['trafico.totales.totalConexiones'] = mintic_01['trafico.totales.totalConexiones'].astype(int)
    mintic_01.rename(columns={'site_id': 'trafico.siteID'
                             ,'fecha_control' : 'trafico.totales.fechaControl'}, inplace=True)

    try:
        mintic_01["trafico.totales.fecha"] = mintic_01["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    except:
        mintic_01["trafico.totales.fecha"] = ""

    try:
        mintic_01["trafico.totales.hora"] = mintic_01["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    except:
        mintic_01["trafico.totales.hora"] = ""

    try:
        mintic_01["trafico.totales.minuto"] = mintic_01["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    except:
        mintic_01["trafico.totales.minuto"] = ""

    try:
        mintic_01["trafico.totales.anyo"] = mintic_01["trafico.totales.fecha"].str[0:4]
    except:
        mintic_01["trafico.totales.anyo"] = ""

    try:
        mintic_01["trafico.totales.mes"] = mintic_01["trafico.totales.fecha"].str[5:7]
    except:
        mintic_01["trafico.totales.mes"] = ""

    try:
        mintic_01["trafico.totales.dia"] = mintic_01["trafico.totales.fecha"].str[8:10]
    except:
        mintic_01["trafico.totales.dia"] = ""

    mintic_01['nombreDepartamento'] = mintic_01['trafico.nombreDepartamento']
    mintic_01['nombreMunicipio'] = mintic_01['trafico.nombreMunicipio']
    mintic_01['idBeneficiario'] = mintic_01['trafico.idBeneficiario']
    mintic_01['fecha'] = mintic_01['trafico.totales.fecha']
    mintic_01['anyo'] = mintic_01['trafico.totales.anyo']
    mintic_01['mes'] = mintic_01['trafico.totales.mes']
    mintic_01['dia'] = mintic_01['trafico.totales.dia']
    mintic_01['@timestamp'] = now.isoformat()       

    #mintic_01 = mintic_01[['trafico.siteID','trafico.totales.fechaControl','trafico.macRed','trafico.apGroup','trafico.totales.totalConexiones','@timestamp']]

    # mintic_04=pd.merge(mintic_03,mintic_01,on=['trafico.siteID','trafico.apGroup','trafico.totales.fechaControl'],how='left')
    total_conexiones.rename(columns={'site_id': 'trafico.siteID'
                             ,'fecha_control' : 'trafico.totales.fechaControl'}, inplace=True)

    #mintic_04=pd.merge(mintic_03,mintic_01,on=['trafico.siteID','trafico.apGroup','trafico.totales.fechaControl'],how='left')
    salida = helpers.bulk(es, doc_generator_dis(mintic_01))
    print("Fecha: ", now,"- Datos Trafico en indice principal:",salida[0])
except Exception as e:
    print(e)
    print("Fecha: ", now,"- Nada de Trafico para insertar en indice principal:")


# ## Se lee información para usuarios recurrencia

# Se calcula y agrega al indice principal:
# * trafico.concurrenciaConexiones
# 
# Se lee el indice recurrencia de conexiones y se compara con el flujo detalle conexiones para el rango dado. Si cruzan, se suma a la cuenta de recurrentes.

# In[775]:


total_docs = 5000000
response = es.search(
    index= parametros.ohmyfi_r_u_index,
    body={
            "_source": ["ultima_conexion", "lugar_cod", "id_usuario"]
    },
    size=total_docs
)
#print(es.info())
elastic_docs = response["hits"]["hits"]
datos_recurrencia = pd.DataFrame([x["_source"] for x in elastic_docs])


# In[776]:


datos_recurrencia['lugar_cod'] = datos_recurrencia['lugar_cod'].str.strip()


# Se cuenta la cantidad de usuarios con mas de una conexión

# # Concurrencia usuario a indice

# In[777]:


try:
    datos_det_conex_completo.rename(columns={'mac_usuario':'id_usuario'}, inplace=True)
    aux_recurrencia=datos_det_conex_completo[['lugar_cod','id_usuario']].groupby(['id_usuario']).agg(['count']).reset_index()
    aux_recurrencia.columns = aux_recurrencia.columns.droplevel(1)
    aux_recurrencia.rename(columns={'lugar_cod': 'contador'}, inplace=True)
    aux_recurrencia = aux_recurrencia.drop(aux_recurrencia[(aux_recurrencia['contador'] < 2)].index)
    datos_recurrencia = pd.merge(datos_det_conex_completo,  aux_recurrencia, on='id_usuario', how='inner')
    datos_recurrencia = datos_recurrencia[['lugar_cod','fecha_control','id_usuario']].groupby(['lugar_cod','fecha_control']).agg(['count']).reset_index()
    datos_recurrencia.columns = datos_recurrencia.columns.droplevel(1)
    datos_recurrencia.rename(columns={'lugar_cod':'site_id'}, inplace=True)
    datos_recurrencia = pd.merge(datos_semilla,  datos_recurrencia, on='site_id', how='inner')
    datos_recurrencia.rename(columns={'site_id':'trafico.siteID'
                                     ,'id_usuario': 'trafico.concurrenciaConexiones'
                                     ,'fecha_control' : 'trafico.totales.fechaControl'}, inplace=True)
    datos_recurrencia.fillna({'trafico.concurrenciaConexiones':0},inplace=True)
    datos_recurrencia.fillna('', inplace=True)
    #print(datos_recurrencia)
    try:
        datos_recurrencia["trafico.totales.fecha"] = datos_recurrencia["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[0]
    except Exception as e:
        datos_recurrencia["trafico.totales.fecha"] = ""
        
    datos_recurrencia["trafico.totales.anyo"] = datos_recurrencia["trafico.totales.fecha"].str[0:4]
    try:
        datos_recurrencia["trafico.totales.mes"] = datos_recurrencia["trafico.totales.fecha"].str[5:7]
    except Exception as e:
        datos_recurrencia["trafico.totales.mes"] = ""
    
    try:
        datos_recurrencia["trafico.totales.dia"] = datos_recurrencia["trafico.totales.fecha"].str[8:10]
    except:
        datos_recurrencia["trafico.totales.dia"] = ""
    
    try:
        datos_recurrencia["trafico.totales.hora"] = datos_recurrencia["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    except:
        datos_recurrencia["trafico.totales.hora"] = ""
    
    try:
        datos_recurrencia["trafico.totales.minuto"] = datos_recurrencia["trafico.totales.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    except:
        datos_recurrencia["trafico.totales.minuto"] = ''
    
    datos_recurrencia['nombreDepartamento'] = datos_recurrencia['trafico.nombreDepartamento']
    datos_recurrencia['nombreMunicipio'] = datos_recurrencia['trafico.nombreMunicipio']
    datos_recurrencia['idBeneficiario'] = datos_recurrencia['trafico.idBeneficiario']
    datos_recurrencia['fecha'] = datos_recurrencia['trafico.totales.fecha']
    datos_recurrencia['anyo'] = datos_recurrencia['trafico.totales.anyo']
    datos_recurrencia['mes'] = datos_recurrencia['trafico.totales.mes']
    datos_recurrencia['dia'] = datos_recurrencia['trafico.totales.dia']
    #print("eje111")
    datos_recurrencia['@timestamp'] = now.isoformat() 
    
    #datos_recurrencia=datos_recurrencia[['trafico.siteID','trafico.totales.fechaControl','trafico.concurrenciaConexiones']]
    
    #mintic_03=pd.merge(mintic_04,datos_recurrencia,on=['trafico.siteID','trafico.totales.fechaControl'],how='left')
    datos_recurrencia=pd.merge(mintic_01,datos_recurrencia,on=['trafico.siteID','trafico.totales.fechaControl'],how='left')
    
    #datos_recurrencia=datos_recurrencia[['trafico.siteID','trafico.totales.fechaControl','trafico.concurrenciaConexiones']]
    #print(mintic_03)
    
    datos_recurrencia.fillna({'trafico.concurrenciaConexiones':0,
                    },inplace=True)
    datos_recurrencia[['trafico.concurrenciaConexiones']] = datos_recurrencia[['trafico.concurrenciaConexiones']].astype(int)
    
    #salida = helpers.bulk(es, doc_generator(datos_recurrencia))
    #print("Fecha: ", now,"- recurrencia de usuario a indice:",salida[0])
except Exception as e:
    #print("Fecha: ", now,"- Ninguna recurrencia de usuario para insertar en indice principal")
    pass


# In[778]:


datos_recurrencia.rename(columns={'trafico.nomCentroDigital_x':'trafico.nomCentroDigital',
                                  'trafico.idBeneficiario_x':'trafico.idBeneficiario',
                                  'trafico.location_x':'trafico.location',
                                  'trafico.totales.fecha_x':'trafico.totales.fecha',
                                  'trafico.totales.hora_x':'trafico.totales.hora',
                                  'trafico.totales.minuto_x':'trafico.totales.minuto',
                                  'trafico.totales.anyo_x': 'trafico.totales.anyo',
                                  'trafico.totales.mes_x' :'trafico.totales.mes',
                                  'trafico.totales.dia_x' : 'trafico.totales.dia',
                                  'fecha_x' : 'fecha',
                                  'anyo_x' : 'anyo',
                                  'mes_x' : 'mes',
                                  'dia_x': 'dia'}, inplace=True)


# In[779]:


datos_recurrencia=datos_recurrencia[['trafico.siteID',
                                     'trafico.nomCentroDigital',
                                     'trafico.idBeneficiario',
                                     'trafico.location',
                                     'trafico.totales.fechaControl',
                                     'trafico.totales.fecha',
                                     'trafico.totales.hora',
                                     'trafico.totales.minuto',
                                     'trafico.totales.anyo',
                                     'trafico.totales.mes',
                                     'trafico.totales.dia',
                                     'fecha',
                                     'anyo',
                                     'mes',
                                     'dia',
                                     'trafico.concurrenciaConexiones'
                                    ]]


# # Insertando recurrencia de usuario en indice principal

# la lista use_these_keys se usa para referenciar cuales campos del dataframe irán al indice final. si los datos no se declaran en este, no se insertarán

# In[780]:


try:
    use_these_keys = ['trafico.siteID',
                                     'trafico.nomCentroDigital',
                                     'trafico.idBeneficiario',
                                     'trafico.location',
                                     'trafico.totales.fechaControl',
                                     'trafico.totales.fecha',
                                     'trafico.totales.hora',
                                     'trafico.totales.minuto',
                                     'trafico.totales.anyo',
                                     'trafico.totales.mes',
                                     'trafico.totales.dia',
                                     'fecha',
                                     'anyo',
                                     'mes',
                                     'dia',
                                     'trafico.concurrenciaConexiones',
                                     '@timestamp']

    datos_recurrencia['@timestamp'] = now.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'Recurrencia-' + str(document['trafico.siteID']) + '-' + str(document['trafico.totales.fechaControl'])+ str(random.randrange(1000))}",
                    "_source": filterKeys(document),
                }
    salida = helpers.bulk(es, doc_generator(datos_recurrencia))
    print("Fecha: ", now,"- recurrencia de usuario a indice:",salida[0])
except Exception as e:
    print(e)
    print("Fecha: ", now,"- Ninguna recurrencia de usuario para insertar en indice principal")


# ### Asociando datos de Speed test

# Se tiene una lectura diara de velocidad para cada centro. Por tanto se debe cruzar con el fjulo principal, haciendo uso solo del año, mes día, sin incluir la hora.

# ### Función para generar JSON compatible con ES

# In[781]:


def traeVelocidad(fecha_max_mintic,fecha_tope_mintic):
    total_docs = 10000
    print(fecha_max_mintic)
    print(fecha_tope_mintic)
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

    return pd.DataFrame([x["_source"] for x in elastic_docs])


# * Se genera fecha en yyyy-mm-dd, y cada campo por separado
# * La hora y minuto se toma aparte
# 
# Valores que se convierten a cero si son nulos
# * trafico.anchoBandaDescarga
# * trafico.anchoBandaCarga

# In[782]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_speed = traeVelocidad(fecha_max_mintic,fecha_tope_mintic)

if datos_speed is None or datos_speed.empty:
    while (datos_speed is None or datos_speed.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")
        datos_speed = traeVelocidad(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[783]:


datos_speed['beneficiary_code'] = datos_speed['beneficiary_code'].str.strip()


# # 7. Speed test a indice

# In[784]:


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
        
    #salida = helpers.bulk(es, doc_generator(datos_speed))
    #print("Fecha: ", now,"- Datos Velocidad carga y descarga (Trafico) en indice principal:",salida[0])
except Exception as e:
    print(e)
    #print("Fecha: ", now,"- Ningun dato velocidad para insertar en indice principal")


# In[785]:


try:
    use_these_keys = ['trafico.nomCentroDigital', 
                  'trafico.localidad',
                  'trafico.siteID',
                  'trafico.nombreDepartamento', 
                  'trafico.codISO', 
                  'trafico.sistemaEnergia',
                  'trafico.nombreMunicipio', 
                  'trafico.idBeneficiario',
                  'trafico.location', 
                  'trafico.anchoBandaDescarga',
                  'trafico.anchoBandaCarga',
                  'trafico.totales.fecha',
                  'trafico.totales.anyo',
                  'trafico.totales.mes',
                  'trafico.totales.dia',
                  'nombreDepartamento',
                    'nombreMunicipio',
                    'idBeneficiario',
                    'fecha',
                    'anyo',
                    'mes',
                    'dia',
                  '@timestamp']

    datos_speed['@timestamp'] = now.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'Velocidad-' + document['trafico.siteID'] + '-' + document['trafico.totales.fecha']+ '-'+str(random.randrange(1000))}",
                    "_source": filterKeys(document),
                }
    salida = helpers.bulk(es, doc_generator(datos_speed))
    print("Fecha: ", now,"- Datos Velocidad carga y descarga (Trafico) en indice principal:",salida[0])
except Exception as e:
    print(e)
    print("Fecha: ", now,"- Ningun dato velocidad para insertar en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[786]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia_tablero_trafico',
        body = { 'jerarquia_tablero_trafico': 'jerarquia_tablero_trafico','trafico.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)


# In[ ]:




