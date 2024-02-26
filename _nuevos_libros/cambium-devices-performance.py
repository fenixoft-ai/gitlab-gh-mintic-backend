#!/usr/bin/env python
# coding: utf-8

# In[1]:


from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import numpy as np
import json
from ssl import create_default_context
from datetime import datetime
from datetime import timedelta
import requests
#from getpass import getpass
import parametros


# ## Leyendo datos del indice cambium-devices-performance

# In[2]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
)


# ## tomando fecha mas reciente del indice

# In[3]:


now = datetime.now()
new_date = now - timedelta(days=3)
ayer = now - timedelta(days=1)
fecha_hoy = str(now.strftime("%Y.%m.%d"))
ahora_cdd = str(now.strftime("%Y-%m-%d"' '"%H:%M:%S"))

ahora_cdd


# In[4]:


total_docs = 0
try:
    response = es.search(
        index= parametros.cambium_d_p_index,
        body={"aggs" : {
                   "max_date": {"max": {"field": "timestamp", "format": "yyyy-MM-dd HH:mm:ss"}}
                }
             },
        size=total_docs
    )
    #print(es.info())
    elastic_docs = response["aggregations"]
    fecha_max=response["aggregations"]["max_date"]['value_as_string']
except:
    fecha_max = (new_date).strftime("%Y-%m-%d %H:%M:%S")
print("Fecha maxima en indice:",fecha_max)


# In[5]:


new_date_f = datetime.strptime(fecha_max, '%Y-%m-%d  %H:%M:%S')
aaa = str(new_date_f.strftime("%Y%m%d%H%M"))


#aaa = str(now.strftime("%Y%m%d%H"))+'00'
fecha_ini = (datetime.strptime(aaa, '%Y%m%d%H%M')-timedelta(minutes=0)).strftime("%Y%m%d%H%M")
fecha_fin = (datetime.strptime(fecha_ini, '%Y%m%d%H%M')+timedelta(minutes=360)).strftime("%Y%m%d%H%M")


# In[6]:


#fecha_ini = "20210730000"
#fecha_fin = "202107302359"


# In[7]:


# sacar las MAC de devices
"""
def traerDDevices(fecha_max,fecha_tope):
    total_docs = 10000
    try:
        response = es.search(
            index= parametros.cambium_d_d_index,
            body={
                "_source": ["registration_date","mac","ip","ap_group","site_id","status"]
                ,"query":{
                    "bool": {
                      "filter": [
                        {
                          "range": {
                            "registration_date": {
                              "gte": fecha_max,
                              "lt": fecha_tope
                            }
                          }
                        }
                      ],

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

        datos_DDevices  = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
        datos_DDevices  = datos_DDevices.rename(columns={'ap_group':'gestion.ptos_acceso'})
        datos_DDevices  = datos_DDevices.dropna(subset=['site_id'])
        return datos_DDevices 
    except:
        return pd.DataFrame()
    

fec1="2021-04-01 00:00:00"
datos_mac  = traerDDevices(fec1 , ahora_cdd)

"""





indice = parametros.cambium_d_d_index # + '-' + fecha_hoy
total_docs = 10000
try:
    response = es.search(
        index= indice, 
        body={
               "_source": ['*']
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

    datos_mac = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ])) #pd.DataFrame(fields)
except:
    print("fecha:",now,"- Error en lectura de datos_mac: ", parametros.cambium_d_d_index, " servidor: ", parametros.servidor)
    

if datos_mac is None or datos_mac.empty:
    print (" No se sacaron datos de Cambium-DeviceDevices. por lo que el proceso no continua. ", now, parametros.cambium_d_d_index)       
    pass

datos_mac = datos_mac[["mac","site_id"]].groupby(["mac"]).agg(['count']).reset_index()
datos_mac.columns = datos_mac.columns.droplevel(1)  
datos_mac =datos_mac.dropna(subset=['mac'])

#datos_mac

#datos_mac["mac"].size


# ## Leyendo la APi cambium-devices-performance

# In[8]:


# Definimos la cabecera y el diccionario con los datos
arranca = datetime.now()

#datos_api = pd.DataFrame(columns=["name","network","type","timestamp","radio","tower","mac","mode"
#                                                      ,"sm_drops","managed_account","online_duration","uptime"])


datos_api = pd.DataFrame()
#url_cambium2 = ["http://100.123.26.252/api/v2/","http://100.123.26.224/api/v2/"]
mac ="BC:E6:7C:5F:07:11"
for i in datos_mac.index: 
    mac = datos_mac["mac"][i]         
    for k in range(0,len(parametros.url_cambium)):
        token_aux = 'Bearer ' + parametros.cambium_token_aux[k]
        cabecera1 = { 'Content-Type': 'application/json', 'Authorization' : token_aux, 'accept' : '*/*' } 
        url = parametros.url_cambium[k] + "devices/"+mac+"/performance" + '?start_time='+ fecha_ini + '&stop_time=' + fecha_fin    
        try:
             r = requests.get(url, headers = cabecera1, verify=False)
        except KeyError:
            print ("Error URL, ", url, " tipo: ", KeyError)
            r.status_code = 400
        #print(" url: ", url)
        #print(" code: " , r.status_code)
        #break    
        if r.status_code == 200:
            res = json.loads(r.text)
            dato_param = res['paging']        
            #se calcula los ciclos de la consulta paginada
            ciclos = int(round(dato_param['total']/100,0))
            #print(res['paging'])
            if dato_param['total']>0:
                 ciclos = ciclos + 1
            print(" dato_param['total'] ", dato_param['total'], ciclos)

            i = 0
            while i < ciclos:                
                offset = str(i*100)  # aqui modifique deberia de comenzar en 1 y luego de 100 en 100 GM
                url2 = parametros.url_cambium[k] + "devices/"+mac+"/performance" + '?offset=' + offset  + '&start_time=' + fecha_ini + '&stop_time=' + fecha_fin 
                i+=1
                print(url2)

                try:         
                    r = requests.get(url2, headers = cabecera1, verify=False)            
                except KeyError:
                    print ("Error URL, ", url, " tipo: ", KeyError)
                    r.status_code = 400
                
                if r.status_code == 200:
                    #print("respuesta a200: ",r.text)
                    res = json.loads(r.text)
                    #print("respuesta a200  - res['data'] = ",res['data'])    
                    datos_api = datos_api.append(res['data'], ignore_index=True)
                    #print(res['paging'])
                else:
                    print("2.-Fecha:",now,"- Error bucle interno: ",r.status_code , " no trajo datos: ", url2)
                    break                

        else:
            print("1.-Fecha:",now,"- Error bucle externo: ",r.status_code, " no trajo datos: ", url)
            break
        
termina = datetime.now()         

print("fin tiempos 1.-a:",arranca,"- termina: ",termina)
#datos_api


# In[ ]:


#print (datos_api)


# ### Se descompone el diccionario radio

# In[ ]:


#try:
#    datos_api['radio.dl_kbits'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_kbits'])
#except:
#    datos_api['radio.dl_kbits'] = ''
#try:
#    datos_api['radio.dl_mcs'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_mcs'])
#except:
#    datos_api['radio.dl_mcs'] = ''
#try:
#    datos_api['radio.dl_pkts'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_pkts'])
#except:
#    datos_api['radio.dl_pkts'] = ''
#try:
#    datos_api['radio.dl_rssi'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_rssi'])
#except:
#    datos_api['radio.dl_rssi'] = ''
#try:
#    datos_api['radio.dl_snr'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_snr'])
#except:
#    datos_api['radio.dl_snr'] = ''
#try:
#    datos_api['radio.dl_throughput'] = datos_api.loc[:, 'radio'].apply(lambda x: x['dl_throughput'])
#except:
#    datos_api['radio.dl_throughput'] = ''
#try:
#    datos_api['radio.ul_kbits'] = datos_api.loc[:, 'radio'].apply(lambda x: x['ul_kbits'])
#except:
#    datos_api['radio.ul_kbits'] = ''
#try:
#    datos_api['radio.ul_mcs'] = datos_api.loc[:, 'radio'].apply(lambda x: x['ul_mcs'])
#except:
#    datos_api['radio.ul_mcs'] = ''
#try:
#    datos_api['radio.ul_pkts'] = datos_api.loc[:, 'radio'].apply(lambda x: x['ul_pkts'])
#except:
#    datos_api['radio.ul_pkts'] = ''
#try:
#    datos_api['radio.ul_retransmits_pct'] = datos_api.loc[:, 'radio'].apply(lambda x: x['ul_retransmits_pct'])
#except:
#    datos_api['radio.ul_retransmits_pct'] = ''
#try:
#    datos_api['radio.ul_throughput'] = datos_api.loc[:, 'radio'].apply(lambda x: x['ul_throughput'])
#except:
#    datos_api['radio.ul_throughput'] = ''


# In[9]:


#d = pd.DataFrame(columns=["radio.5ghz.rx_bps","radio.5ghz.tx_bps"
#                         ,"radio.24ghz.rx_bps","radio.24ghz.tx_bps"])
datos_api['tower'] = ''
datos_api['sm_drops'] = ''
datos_api['online_duration'] = 0
datos_api['uptime'] = 0
#datos_api['radio.dl_snr'] = ''
#datos_api['radio.dl_throughput'] = ''
#datos_api['radio.ul_kbits'] = ''
#datos_api['radio.ul_mcs'] = ''
#datos_api['radio.ul_pkts'] = ''
#datos_api['radio.ul_rssi'] = ''
#datos_api['radio.ul_snr'] = ''
#datos_api['radio.5ghz.rx_bps'] = datos_api['radios'][1]
#datos_api['radio.5ghz.tx_bps'] = datos_api['radios'][1]['tx_bps']
#datos_api['radio.24ghz.rx_bps'] = datos_api['radios'][0]['rx_bps']
#datos_api['radio.24ghz.tx_bps'] = datos_api['radios'][0]['tx_bps']

array_datos = datos_api.to_numpy()
print (len(array_datos))
list_datos = datos_api.T.to_dict().values()

for key, item in enumerate(list_datos):
    if 'radios' in item.keys():
    #if item.has_key('radios'):
        if (isinstance(item['radios'], float) == False):
            radios_arr = item['radios']
            item['radio.dl_kbits'] = ''
            item['radio.dl_mcs'] = ''
            item['radio.dl_pkts'] = ''
            item['radio.dl_rssi'] = ''
            item['radio.dl_snr'] = ''
            item['radio.dl_throughput'] = ''
            item['radio.ul_kbits'] = ''
            item['radio.ul_mcs'] = ''
            item['radio.ul_pkts'] = ''
            item['radio.ul_rssi'] = ''
            item['radio.ul_snr'] = ''
            item['radio.ul_retransmits_pct'] = ''
            
            if 'rx_bps' in radios_arr[1].keys():
                item['radio.5ghz.rx_bps'] = radios_arr[1]['rx_bps']
            else:
                item['radio.5ghz.rx_bps'] = ''
            
            if 'tx_bps' in radios_arr[1].keys():
                item['radio.5ghz.tx_bps'] = radios_arr[1]['tx_bps']
            else:
                item['radio.5ghz.tx_bps'] = ''
            
            if 'rx_bps' in radios_arr[0].keys():
                item['radio.24ghz.rx_bps'] = radios_arr[0]['rx_bps']
            else:
                item['radio.24ghz.rx_bps'] = ''
            
            if 'tx_bps' in radios_arr[0].keys():
                item['radio.24ghz.tx_bps'] = radios_arr[0]['tx_bps']
            else:
                item['radio.24ghz.tx_bps'] = ''


print(type(list_datos))
#print(list_datos)
#lista_data = list(list_datos)
datos_api = pd.DataFrame(list_datos)
#print(new_df)

for index,datos in datos_api.items():
    try:
        a = 0
        #d.loc[index] = [datos['radio']['5ghz']['rx_bps'],datos['radio']['5ghz']['tx_bps']
        #                ,datos['radio']['24ghz']['rx_bps'],datos['radio']['24ghz']['tx_bps']]


        #datos_api.iloc[index]['radio.5ghz.rx_bps'] = datos['radio']['5ghz']['rx_bps']
        #print(index,datos_api.iloc[index]['radio']['5ghz']['rx_bps'])#['5ghz']['rx_bps'],datos['5ghz']['tx_bps'])
    except:
        pass
#datos_api


# In[10]:


#datos_api.columns[(datos_api['radio.5ghz']!='')]
#d[~(d['radio.5ghz.rx_bps'].isnull())]
#datos_api = datos_api.merge(d, how='left', left_index=True, right_index=True)


# In[11]:


if len(datos_api) > 0:
    datos_api.drop(columns=['radios'],inplace=True)


# In[12]:


# datos_api
# esto fue agregado de prueba para ver los datos


# In[13]:


#datos_api.drop_duplicates(inplace=True)
# Esta linea la quite porque para eliminar registro hay que tener un campo unique-primarykey, por lo cual da error


# ### Descartando datos de la API que ya están en el indice

# In[14]:


try:
    datos_api['timestamp'] = (datos_api["timestamp"].str.split("T", n = 2, expand = True)[0])+' '+(datos_api["timestamp"].str.split("T", n = 2, expand = True)[1]).str.split("-", n = 1, expand = True)[0]
except:
    pass


# In[15]:


datos_api = datos_api.drop(datos_api[(datos_api['timestamp']<= fecha_max)].index)


# In[16]:


datos_api.fillna('', inplace=True)


# In[22]:


#datos_api.dtypes


# ### Definiendo indice con fecha e insertando en ES

# In[17]:


indice = parametros.cambium_d_p_index #+'-'+ fecha_hoy


# In[23]:


use_these_keys = ['name', 'network', 'type', 'tower', 'mac', 'mode', 'sm_drops',
                   'managed_account', 'online_duration', 'uptime', 'site',
                   'radio.dl_kbits', 'radio.dl_mcs', 'radio.dl_pkts', 'radio.dl_rssi',
                   'radio.dl_snr', 'radio.dl_throughput', 'radio.ul_kbits', 'radio.ul_mcs',
                   'radio.ul_pkts', 'radio.ul_retransmits_pct', 
                   'radio.5ghz.rx_bps', 'radio.5ghz.tx_bps', 'radio.24ghz.rx_bps',
                   'radio.24ghz.tx_bps','timestamp','@timestamp']
def filterKeys(document):
    return {key: document[key] for key in use_these_keys }

timestamp = datetime.now()
datos_api['@timestamp'] = timestamp.isoformat()
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_source": filterKeys(document),
            }
salida = helpers.bulk(es, doc_generator(datos_api))
print("Fecha",now,"- Total documentos insertados:",salida[0])


# In[ ]:




