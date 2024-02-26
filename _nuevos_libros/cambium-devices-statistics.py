#!/usr/bin/env python
# coding: utf-8

# In[2]:


from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import numpy as np
import json
from ssl import create_default_context
from datetime import datetime
from datetime import timedelta
import requests
import math
import parametros


# ## Se define conexión a Elastic

# In[3]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
)


# In[4]:


now = datetime.now()
new_date = now - timedelta(days=6)
fecha_url = str(new_date.strftime("%Y%m%d"))
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ## Leyendo la APi cambium-devices-statistics

# Para cada elemento en la lista de servidores Cambium, se ejecuta un conjunto de request con paginados de 100

# In[5]:


# Definimos la cabecera y el diccionario con los datos
datos_api = pd.DataFrame(columns=['name','network','status','type','last_sync','lan_mode_status'
                                                     ,'lan_speed_status','lan_status','tower','ip_wan','mac','mode'
                                                     ,'parent_mac','managed_account','ip','cpu','memory'])
for k in range(0,len(parametros.url_cambium)):
    token_aux = 'Bearer ' + parametros.cambium_token_aux[k]
    cabecera1 = { 'Content-Type': 'application/json', 'Authorization' : token_aux, 'accept' : '*/*' } 
    url = parametros.url_cambium[k] + 'devices/statistics' 
    r = requests.get(url, headers = cabecera1, verify=False)
    if r.status_code == 200:
        res = json.loads(r.text)
        dato_param = res['paging']
        ciclos = int(math.ceil(dato_param['total']/100))
        i = 0
        while i <= ciclos:
            offset = str(i*100)
            i+=1
            url = parametros.url_cambium[k] + 'devices/statistics' + '?offset=' + offset
            if r.status_code == 200:
                res_api = json.loads(r.text)
                datos_api = datos_api.append(res_api['data'], ignore_index=True)
            else:
                print("Fecha: ",now," - Error en request del bucle: ",r.status_code)
                exit()
    else:
                print("Fecha: ",now," - Error en request inicial: ",r.status_code)
                exit()


# In[6]:


url


# Se eliminan los registros duplicados por mac y fecha de sincronización

# In[46]:


datos_api.drop_duplicates(subset=['mac','last_sync'], inplace = True)


# Se descartan fechas nulas

# In[47]:


datos_api = datos_api.drop(datos_api[(datos_api['last_sync'].isnull())].index)


# ### Descartando datos de la API que ya están en el indice

# Se formatea la fecha last_sync y se genera fecha control con intervalo de 10 minutos. Para evitar la duplicación de registros para una mac en un rango de tiempo igual, se define como indice de cada documento, la concatenación de la mac con la fecha control

# In[48]:


try:
    datos_api['last_sync'] = (datos_api["last_sync"].str.split("T", n = 2, expand = True)[0])+' '+(datos_api["last_sync"].str.split("T", n = 2, expand = True)[1]).str.split("-", n = 1, expand = True)[0]    
    datos_api['fecha_control'] = datos_api["last_sync"].str[0:-4] + '0'
except:
    print("Fecha: ",now," - Error leyendo dataframe")
    exit()


# In[49]:


datos_api.fillna('', inplace=True)


# ### Definiendo indice con fecha e insertando en ES

# In[50]:


indice = parametros.cambium_d_sta_index # +'-'+ fecha_hoy


# In[51]:


use_these_keys = ['name', 'network', 'status', 'type', 'ip_wan', 'mac', 'mode',
                   'ip', 'site_id', 'last_sync', 'fecha_control','@timestamp']
def filterKeys(document):
    return {key: document[key] for key in use_these_keys }

timestamp = datetime.now()
datos_api['@timestamp'] = timestamp.isoformat()
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{document['mac']+'-'+ document['fecha_control']}",
                "_source": filterKeys(document),
            }
salida = helpers.bulk(es, doc_generator(datos_api))
print("Fecha:",now," - Documentos insertados:",salida[0])

