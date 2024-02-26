#!/usr/bin/env python
# coding: utf-8

# In[836]:


from elasticsearch import Elasticsearch, helpers, exceptions
#from elasticsearch import helpers
import pandas as pd
import numpy as np
import json
from ssl import create_default_context
from datetime import datetime, timedelta
import time
#para request
#import requests
#from getpass import getpass
import math
import ipaddress # convertir IP
import parametros #parametros propios del desarrollo
from tld import get_tld #para manipulación de URLs
from tld.utils import update_tld_names
update_tld_names()


# ### Función para gestión de URLs

# In[837]:


def maneja_url(url):
    try:
        res = get_tld(url, fix_protocol=True, as_object=True)
        # opciones de retorno: res,res.subdomain,res.domain,res.tld,res.fld
        return res.fld
    except:
        return url


# Se realizan 3 lecturas al mismo indice crudo (paramtros.sonicwall_raw) con los siguientes criterios:
# 1. Traer los URL de las páginas:
# * el criterio de filtrado es cuando el campo netflow.url-flow tiene datos
# * de esta lectura se obtienen los datos netflow.url-time(fecha de visita) y netflow.url-name(URL visidata), de la cual solo se tomará el dominio.
# * Este flujo cruza con el 2 y el 3.
# 
# 2. Traer la IP del acces point desde donde se consulta la URL:
# * netflow.url-flow-identify traiga algo (esta es la llave de cruce)
# * el criterio de filtrado es cuando el campo netflow.url-flow-ip-origen tiene datos
# * este flujo cruza con el 1 por medio del campo netflow.url-flow.
# 
# 3. Categorias de las URL
# * el criterio de filtrado es cuando el campo netflow.url-flow-categoria tiene datos.
# * de esta lectura se obtiene el campo netflow.url-categoria-name2, el cual es la llave para cruzar con con el archivo "Mapeo-categorias-url.tsv"
# * Esta lectura cruza con la lectura 1, usando el campo netflow.url-name.
# * De la URL solo se debe tomar el dominio. Las jerarquías inferiores se descartan
# * En cada lectura, si la categoría(netflow.url-categoria-name2) no se consigue en el archivo de mapeos, se agrega. Este proceso se hará inicialmente cargando el archivo al indice, después solo se usará el indice sonicwall_categorias
# * este flujop cruza con el 1

# #Definiendo fechas para rangos de consultas

# In[838]:


now = datetime.now()
new_date = now - timedelta(days=6)
format_ES = "%Y.%m.%d"
ahora = str(now.strftime("%Y-%m-%dT%H:%M:%S"))
fecha_6 = str(new_date.strftime("%Y-%m-%dT%H:%M:%S"))
fecha_hoy = str(now.strftime(format_ES))


# * Limite inferior del rango es la maxima fecha en el indice curado de sonicwall. En caso de no existir. Se calcula del indice crudo filebeat restandole 5 minutos a la maxima fecha en este.
# * Limite superior del rango es la maxima fecha de actualización en el indice crudo filebeat. 
# * Las ejecuciones se hacen sobre el indice crudo de la fecha actual. No se toman históricos.

# ### Conexión a Elastic Search

# In[839]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=300, max_retries=1, retry_on_timeout=True
)


# ### Indice crudo filebeat

# In[840]:


index_raw = parametros.sonicwall_raw


# ### Funcion para construcción de JSON ES

# In[841]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### Fecha maxima de indice sonicwall

# Este se usa para descartar de los indices crudos los registros menores a esa fecha

# In[842]:


total_docs = 0
try:
    response = es.search(
        index=parametros.sonicwall_index,
        body={"aggs" : {
                   "max_date": {"max": {"field": "fecha_control", "format": "yyyy-MM-dd HH:mm:ss"}}
                }
             },
        size=total_docs
    )
#     elastic_docs = response["aggregations"]
    fecha_max = response["aggregations"]["max_date"]['value_as_string']
    h_inicio = response["aggregations"]["max_date"]['value_as_string']
    #h_fin = (datetime.strptime(h_inicio, '%Y-%m-%dT%H:%M:%S')+timedelta(hours=2)).strftime("%Y/%m/%dT%H:%M:%S")
except:
    fecha_max = (new_date).strftime("%Y-%m-%d") + ' 00:00:00'
    h_inicio = fecha_max
    
    #h_fin = (datetime.strptime(h_inicio, '%Y-%m-%dT%H:%M:%S') + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
print("Fecha maxima en sonic curado:", fecha_max,"| Fecha inicio:", h_inicio)


# In[788]:


#h_inicio = "2021-10-14 00:00:00"


# ### Fecha maxima del indice crudo filebeat

# In[789]:


total_docs = 0
try:
    response = es.search(
        index= index_raw,
        body={
                "aggs" : {
                   "max_date": {"max": {"field": "@timestamp", "format": "yyyy-MM-dd HH:mm:ss"}}
                },
            "query": {
                "bool": {
                  "filter": [
                
                  {
                    "exists": {
                      "field":"netflow.url-flow"
                    }
                  }
                  ]
                }
              }
             },
        size=total_docs
    )
    elastic_docs = response["aggregations"]
    fecha_max_crudo = response["aggregations"]["max_date"]['value_as_string']
    #h_fin = (datetime.strptime(h_inicio, '%Y-%m-%dT%H:%M:%S')+timedelta(hours=2)).strftime("%Y/%m/%dT%H:%M:%S")
    print("Fecha maxima en indice crudo:", fecha_max_crudo)
except:
    print("Error leyendo indice crudo filebeat")
    #exit()


# * h_inicio viene del indice curado
# * h_fin viene de el indice crudo
# * En caso de que la fechas inicio y fin sean de dias diferentes, se igualan al actual. Se asume un rango de 10 minutos por corrida.
# * Cuando el rango de fechas es mayor a 10 minutos se ajusta.

# ### 1. Traer los URL de las páginas

# Se ajusta el maximo de resultados por consulta en el indice

# In[790]:


es.indices.put_settings(index=index_raw,
                        body= {"index" : {
                                "max_result_window" : 500000
                        }})


# In[791]:


# def cargarJSON(elastic_docs):
#     return pd.DataFrame([x["_source"]['netflow'] for x in elastic_docs])


# In[792]:


def trae_datos(desde, hasta, total_docs):
    response = es.search(
        index= index_raw,
        body={
              "_source": ["netflow.url*","@timestamp"],
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "@timestamp": {
                            "gte": desde.replace(" ","T"),
                            "lt": hasta.replace(" ","T")
                        }
                      }
                    }
                  ,
                  {
                    "exists": {
                      "field":"netflow.url-flow"
                    }
                  }
                  ]
                }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
    
    datos_api = pd.DataFrame([x["_source"]['netflow'] for x in elastic_docs])
    
    #datos_api = pd.DataFrame(elastic_docs,['url-time', 'url-name', 'url-flow', 'url-ip'])
    return datos_api


# Se incrementa la fecha de inicio ya que esta es el limite inferior del rango anteriormente procesado

# In[793]:


#h_fin = h_fin[0:15]+'0:00'
h_inicio = (datetime.strptime(h_inicio, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
h_fin = (datetime.strptime(h_inicio, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
if h_inicio.split(" ")[0]<h_fin.split(" ")[0]:
    h_inicio = h_fin[0:10]+' 00:00:01'
    h_fin = h_fin[0:10]+' 00:30:00'


# In[794]:


#h_inicio='2021-06-30 09:00:00'
#h_fin='2021-06-30 09:10:00'


# In[795]:


sonic_paginas = trae_datos(h_inicio, h_fin,100000)


# In[796]:


if sonic_paginas is None or sonic_paginas.empty:
    while (sonic_paginas is None or sonic_paginas.empty) and ((datetime.strptime(h_inicio, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")) < (datetime.strptime(fecha_max_crudo, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S"))):
        h_inicio = (datetime.strptime(h_inicio, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        h_fin = (datetime.strptime(h_fin, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        sonic_paginas = trae_datos(h_inicio, h_fin,100000)
else:
    pass


# In[797]:


#h_inicio='2021-06-30 09:00:00'
#h_fin='2021-06-30 09:10:00'
print(sonic_paginas)


# In[798]:


sonic_paginas.drop_duplicates(subset=['url-flow','url-ip', 'url-time', 'url-name'],inplace=True)
sonic_paginas['url-dominio'] = sonic_paginas['url-name'].apply(maneja_url)
print(sonic_paginas)


# In[799]:


#sonic_paginas.to_excel("sonic_pagina.xlsx")


# ### 2. Traer la IP del access point desde donde se consulta la URL

# In[800]:


try:
    total_docs = 500000
    response = es.search(
        index= index_raw,
        body={
              "_source": ["netflow.url*","@timestamp"],
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "@timestamp": {
                            "gte": h_inicio.replace(" ","T"),
                            "lt": h_fin.replace(" ","T")
                        }
                      }
                    }
                  ,
                  {
                    "exists": {
                      "field":"netflow.url-flow-identify"
                    }
                  }
                  ]
                }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]["netflow"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])

#     datos_ip = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
    
    datos_ip = pd.DataFrame([x["_source"]['netflow'] for x in elastic_docs])
    
except:
    print("Error leyendo indice crudo URL",response)
    exit()


# In[801]:


datos_ip.drop_duplicates(subset=['url-ip-destino', 'url-ip-origen', 'url-flow-identify'],inplace=True)
datos_ip = datos_ip.rename(columns={'url-flow-identify' : 'url-flow'})


# In[802]:


datos_ip


# In[803]:


#datos_ip.to_excel("datosip.xlsx")


# ### Combinando URL con Names

# In[804]:


datos_ip.to_excel("datos_ip.xlsx")
datos_ip.to_excel("sonic_paginas.xlsx")
print(datos_ip)
print("///////")
print(sonic_paginas)
sonic_paginas = pd.merge(sonic_paginas,  datos_ip, on=['url-flow'], how='inner')


# In[805]:


sonic_paginas


# In[806]:


#sonic_paginas.to_excel("paginas.xlsx")


# ### 3. Leyendo indice de categorías

# * Se lee indice categorias, si no retorna nada se lee el plano separado por tabulador.
# * La información leída del archivo es agregada al indice de categorías. En lecturas sucesivas ya estaría disponible el indice.

# In[807]:


total_docs = 10000
try:
    response = es.search(
        index= parametros.sonicwall_categorias,
        body={
                "_source": ["ID","Categoria"]
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

    datos_categorias = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
except:
    datos_categorias = pd.read_csv('/home/desarrollo/gh-mintic-backend/archivos/Mapeo-categorias-url.tsv', sep='\t', encoding = 'utf-8')
    datos_categorias.dropna(inplace=True)
    use_these_keys = ['ID', 'Categoria','@timestamp']
    timestamp = datetime.now()
    datos_categorias['@timestamp'] = timestamp.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": sonicwall_categorias, 
                    "_id": f"{document['ID']}",
                    "_source": filterKeys(document),
                }
    salida = helpers.bulk(es, doc_generator(datos_categorias))
    print("Fecha: ", now,"- Datos nuevos en indice sonic-categorias:",salida[0])
    #datos_sonic = datos_sonic.drop(aux_recurrencia[(aux_recurrencia['contador'] < 2)].index)


# ### Se lee del indice crudo las Categorias

# In[808]:


try:
    total_docs = 500000
    response = es.search(
        index= index_raw,
        body={
              "_source": ["netflow.url*","@timestamp"],
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "@timestamp": {
                            "gte": h_inicio.replace(" ","T"),
                            "lt": h_fin.replace(" ","T")
                        }
                      }
                    }
                  ,
                  {
                    "exists": {
                      "field":"netflow.url-categoria-name2"
                    }
                  }
                  ]
                }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
#     fields = {}
#     for num, doc in enumerate(elastic_docs):
#         source_data = doc["_source"]["netflow"]
#         for key, val in source_data.items():
#             try:
#                 fields[key] = np.append(fields[key], val)
#             except KeyError:
#                 fields[key] = np.array([val])
#     sonic_categorias = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))

    sonic_categorias = pd.DataFrame([x["_source"]['netflow'] for x in elastic_docs])
    
except:
    print("Error leyendo indice crudo Categorías")
    #exit()


# In[809]:


#prueba = json.loads(elastic_docs)
#prueba
#df = pd.json_normalize(prueba['results'])


# * Se  identifica el dominio de cada URL
# * Se descartam duplicados por dominio y categoría
# * Se renombre ID para cruce

# In[810]:


sonic_categorias.drop_duplicates(subset=['url-name','url-categoria-name2'],inplace=True)
sonic_categorias = sonic_categorias.rename(columns={'url-categoria-name2' : 'ID'})
print(sonic_categorias)


# ### Se  combina las categorias que vienen del indice con las existentes en el indice de categorías

# In[811]:


sonic_categorias = pd.merge(sonic_categorias[['url-name','ID']],  datos_categorias, on='ID', how='outer')


# In[812]:


sonic_categorias.to_excel("categorias.xlsx")
sonic_paginas.to_excel("sonic_paginas.xlsx")
print(sonic_paginas)


# ### Se combina categorías con indice principal

# In[813]:


sonic_paginas = pd.merge(sonic_paginas,  sonic_categorias, on='url-name', how='inner')
print(sonic_paginas)


# * Si la categoría no existe en el indice de categorías, se agrega ascociando el dominio como nombre de la categoría
# * Se genera un nuevo dataframe con los valores que no existan en indice para luego insertarlos
# * Para incorporar en flujo principal, se sustituye el valor Categorías nulas por el valor del url-dominio

# In[814]:


sonic_categorias['url-dominio'] = sonic_categorias['url-name'].apply(maneja_url)
sonic_categorias['Categoria'].fillna(sonic_categorias['url-dominio'], inplace=True)
categoria_update = sonic_categorias[(sonic_categorias['Categoria'].isnull())][['url-dominio','ID']]


# ### Convirtiendo fechas e IPs

# In[815]:


sonic_paginas['url-time'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)) for x in sonic_paginas['url-time']]


# In[816]:


sonic_paginas['url-ip-destino'] = [str(ipaddress.ip_address(x)) for x in sonic_paginas['url-ip-destino']]
sonic_paginas['url-ip-origen'] = [str(ipaddress.ip_address(x)) for x in sonic_paginas['url-ip-origen']]


# In[817]:


sonic_paginas.drop_duplicates(inplace=True)


# Se extrae solo los 3 primeros octetos de la IP origen

# In[818]:


print(sonic_paginas['url-ip-origen'])


# In[819]:


if (len(sonic_paginas) > 0):
    sonic_paginas['segmento'] = sonic_paginas['url-ip-origen'].str.split(".", n = 1, expand = True)[0] +'.'+ sonic_paginas['url-ip-origen'].str.split(".", n = 2, expand = True)[1] +'.'+ sonic_paginas['url-ip-origen'].str.split(".", n = 3, expand = True)[2]


# In[820]:


sonic_paginas


# ### Se lee semilla

# In[821]:


total_docs = 10000
try:
    response = es.search(
        index= parametros.semilla_inventario_index,
        body={
               "_source": ['site_id','IP PROD_1','IP PROD_2']
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


# Se extrae solo los 3 primeros octetos del IP OUTDOOR1 en semilla para cruzar con Sonic

# In[822]:


df1=datos_semilla
df1['segmento'] = datos_semilla['IP PROD_1'].str.split(".", n = 1, expand = True)[0] +'.'+ datos_semilla['IP PROD_1'].str.split(".", n = 2, expand = True)[1] +'.'+ datos_semilla['IP PROD_1'].str.split(".", n = 3, expand = True)[2]
#df1


# In[823]:


df2 = datos_semilla
df2['segmento'] = datos_semilla['IP PROD_2'].str.split(".", n = 1, expand = True)[0] +'.'+ datos_semilla['IP PROD_2'].str.split(".", n = 2, expand = True)[1] +'.'+ datos_semilla['IP PROD_2'].str.split(".", n = 3, expand = True)[2]
#df2


# In[824]:


# df2.index=df2.index+datos_semilla.shape[0]


# In[825]:


df3=pd.DataFrame(pd.concat([df1,df2]))


# In[826]:


if (len(sonic_paginas) > 0):
    sonic_paginas = pd.merge(sonic_paginas, df3, on = 'segmento', how='inner')


# In[827]:


#sonic_paginas['site_id']


# In[828]:


# sonic_paginas = pd.merge(sonic_paginas, datos_semilla, on = 'segmento', how='inner')


# ### Se totaliza para rango de fecha control

# * Se generan dos dataframe los cuales se insertan de forma independiente en el mismo indice
# * la agrupación de sitios mas visitados, se hacer por el dominio para dar mejor visualización al usuario

# In[829]:


#sonic_paginas['fecha_control'] = sonic_paginas['url-time'].str[0:15]+'0:00'
if (len(sonic_paginas) > 0):
    sonic_paginas['fecha_control'] = h_inicio


# In[830]:


total_categoria = pd.DataFrame()
if (len(sonic_paginas) > 0):
    total_categoria = sonic_paginas[['url-time','site_id','fecha_control','Categoria']].groupby(['site_id','fecha_control','Categoria']).agg(['count']).reset_index()
    total_categoria.columns = total_categoria.columns.droplevel(1)
    total_categoria.rename(columns={'url-time': 'total.categoria'}, inplace=True)


# In[831]:


total_dominio = pd.DataFrame()
if (len(sonic_paginas) > 0):
    total_dominio = sonic_paginas[['url-time','site_id','fecha_control','Categoria','url-dominio']].groupby(['site_id','fecha_control','Categoria','url-dominio']).agg(['count']).reset_index()
    total_dominio.columns = total_dominio.columns.droplevel(1)
    total_dominio.rename(columns={'url-time': 'total.dominio'}, inplace=True)


# ### Se escribe la información al indice sonic curado

# In[832]:


use_these_keys = ['site_id', 'fecha_control', 'Categoria', 'total.categoria','@timestamp']

total_categoria['@timestamp'] = now.isoformat()
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": parametros.sonicwall_index, 
                "_id": f"{str(document['site_id'])+'-'+ str(document['fecha_control'])+'-'+ str(document['Categoria'])}",
                "_source": filterKeys(document),
            }
salida = helpers.bulk(es, doc_generator(total_categoria))
print("Fecha: ", now,"- Datos CATEGORIAS en sonic-curado:",salida[0])


# In[833]:


use_these_keys = ['site_id', 'fecha_control', 'url-dominio','Categoria', 'total.dominio','@timestamp']

total_dominio['@timestamp'] = now.isoformat()
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": parametros.sonicwall_index, 
                "_id": f"{str(document['site_id'])+'-'+ str(document['fecha_control'])+'-'+str(document['Categoria'])+'-'+ str(document['url-dominio'])}",
                "_source": filterKeys(document),
            }
salida = helpers.bulk(es, doc_generator(total_dominio))
print("Fecha: ", now,"- Datos DOMINIOS en sonic-curado:",salida[0])


# ### Se actualiza indice categorías

# In[834]:


categoria_update = categoria_update.rename(columns={'url-dominio' : 'Categoria'})


# In[835]:


use_these_keys = ['ID', 'Categoria','@timestamp']
categoria_update['@timestamp'] = now.isoformat()
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": sonicwall_categorias, 
                "_id": f"{document['ID']}",
                "_source": filterKeys(document),
            }
salida = helpers.bulk(es, doc_generator(categoria_update))
print("Fecha: ", now,"- Insertados en indice sonic-categorias:",salida[0])


# In[ ]:





# In[ ]:





# In[ ]:




