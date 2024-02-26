#!/usr/bin/env python
# coding: utf-8

# ¿Que hace este script?
# 
# Para cada sitio se calcula los totales de visitas web y la categoría asociada: 
# * usuarios.categoriaPagina
# * usuarios.sitioWeb (Dominio)
# * usuarios.visitas.sitioWeb (Este tiene los conteos)

# In[20]:


from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import parametros
import re


# ## Conectando a ElasticSearch

# La ultima línea se utiliza para garantizar la ejecución de la consulta
# * timeout es el tiempo para cada ejecución
# * max_retries el número de intentos si la conexión falla
# * retry_on_timeout para activar los reitentos

# In[21]:


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

# In[22]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### nombre de indice donde se insertará

# In[23]:


indice = parametros.usuarios_mintic_concat_index_tablero10
indice_control = parametros.usuarios_mintic_control


# ### Funcion para JSON ES

# In[24]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[25]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
               "_source": ["usuarios.tablero10fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia_usuarios_web"
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
        fecha_ejecucion = doc["_source"]['usuarios.tablero10fechaControl']
except:
    fecha_ejecucion = '2021-05-28 17:10:00'
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-05-28 17:10:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ### leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud, entre otros).

# In[26]:


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
    exit()


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location
# * Se renombran los campos de semilla

# In[27]:


def get_location(x):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return 'a'
datos_semilla['latitud'] = datos_semilla['latitud'].apply(get_location)
datos_semilla['longitud'] = datos_semilla['longitud'].apply(get_location)
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['usuarios.tablero10location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.tablero10location']=datos_semilla['usuarios.tablero10location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# In[28]:


datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.tablero10centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.tablero10nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.tablero10nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.tablero10localidad'
                                            , 'nombreSede' : 'usuarios.tablero10nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.tablero10sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.tablero10codISO'
                                            , 'id_Beneficiario' : 'usuarios.tablero10idBeneficiario'})


# * Se limpian espacios
# * Se descartan valores con site_id menores a 13 caracteres
# * Se descartan los registros que tengan la latitud y longitud vacía o no valida

# In[29]:


datos_semilla['site_id'] = datos_semilla['site_id'].apply(lambda x: x.strip())
datos_semilla = datos_semilla[(datos_semilla['site_id'].apply(len)>12)]
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.tablero10location"]=='')].index)


# ## Calculando totales por Sitio Web

# In[31]:


def traeDominios(fecha_max_mintic):
    total_docs = 100000
#     response = es.search(
#         index= parametros.sonicwall_index,
#         body={ 
#             "_source": ["site_id","fecha_control","Categoria","url-dominio","total.dominio"],
#             "query": {
#               "bool": {
#                 "filter": [
#                   {
#                     "bool": {
#                       "must": [
#                           {"term": {"fecha_control": fecha_max_mintic}}
#                       ]
#                     } 
#                   },
#                   {
#                     "exists": {
#                       "field":"url-dominio"
#                     }
#                   }
                    
#                 ]
#               }
#             }
#         },
#         size=total_docs
#     )
    
    # "2021-07-02 00:00:00"
    #Se debe Actualizar el rango de fecha para la extraccion de elastic
    
    fecha_max_mintic_gte = fecha_max_mintic[0:len(fecha_max_mintic)-3]+":00"
    fecha_max_mintic_lte = fecha_max_mintic[0:len(fecha_max_mintic)-6]+":59:59"
    
    response = es.search(
        index= parametros.sonicwall_index,
        body={ 
            "_source": ["site_id","fecha_control","Categoria","url-dominio","total.dominio"],
            "query": {
              "bool": {
                "filter": [    
                            {
                              "range": {
                                "fecha_control": {
                                  "gte": fecha_max_mintic_gte,
                                  "lte": fecha_max_mintic_lte
                                }
                              }
                            },
                            {"exists":{
                              "field":"url-dominio"
                            }
                            } 
                ]
              }
            }
        },
        size=total_docs
    )
    
    elastic_docs = response["hits"]["hits"]
#    fields = {}
#    for num, doc in enumerate(elastic_docs):
#        source_data = doc["_source"]
#        for key, val in source_data.items():
#            try:
#                fields[key] = np.append(fields[key], val)
#            except KeyError:
#                fields[key] = np.array([val])
#
#    return pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
    return pd.DataFrame([x["_source"] for x in elastic_docs])


# ### Lanzando ejecución de consulta

# * Se calcula rango en base a la fecha de control. Para este caso es de 60 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.

# In[32]:


fecha_max_mintic = fecha_ejecucion
#total_categorias = traeCategorias(fecha_max_mintic)
total_dominios = traeDominios(fecha_max_mintic)

if total_dominios is None or total_dominios.empty:
    while (total_dominios is None or total_dominios.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
        #total_categorias = traeCategorias(fecha_max_mintic)
        total_dominios = traeDominios(fecha_max_mintic)
else:
    pass


# # Insertando total visitas sitios Web

# In[33]:


use_these_keys = [ 'usuarios.tablero10siteIDWeb'
                    , 'usuarios.tablero10fechaControl'
                    , 'usuarios.tablero10categoriaPagina'
                    , 'usuarios.tablero10sitioWeb'
                    , 'usuarios.tablero10visitas.sitioWeb'
                    , 'usuarios.tablero10nomCentroDigital'
                    , 'usuarios.tablero10codISO'
                    , 'usuarios.tablero10idBeneficiario'
                    , 'usuarios.tablero10localidad'
                    , 'usuarios.tablero10nombreDepartamento'
                    , 'usuarios.tablero10sistemaEnergia'
                    , 'usuarios.tablero10nombreMunicipio'
                    , 'usuarios.tablero10location'
                    , 'usuarios.tablero10fecha'
                    , 'usuarios.tablero10anyo'
                    , 'usuarios.tablero10mes'
                    , 'usuarios.tablero10dia'
                    , 'usuarios.tablero10hora'
                    , 'usuarios.tablero10minuto'
                    , 'tablero10nombreDepartamento'
                    , 'tablero10nombreMunicipio'
                    , 'tablero10idBeneficiario'
                    , 'tablero10fecha'
                    , 'tablero10anyo'
                    , 'tablero10mes'
                    , 'tablero10dia'
                  , '@timestamp']

def doc_generator_dom(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": indice, 
                "_id": f"{'URL-'+str(document['usuarios.tablero10siteIDWeb']) + '-' + str(document['usuarios.tablero10fechaControl']) + '-' + str(document['usuarios.tablero10categoriaPagina']) + '-' + str(document['usuarios.tablero10sitioWeb'])}",
                "_source": filterKeys(document),
            }


# In[34]:


try:
    
    if (total_dominios is None or total_dominios.empty):
        raise Exception()
        
    total_dominios = total_dominios.drop_duplicates()
    total_dominios.fillna({'Categoria':'Not Rated'},inplace=True)
        
    total_dominios = pd.merge(total_dominios, datos_semilla, on='site_id',how='inner')
    
    total_dominios = total_dominios.rename(columns={'fecha_control' : 'usuarios.tablero10fechaControl'})
    total_dominios["usuarios.tablero10fecha"] = total_dominios["usuarios.tablero10fechaControl"].str.split(" ", n = 1, expand = True)[0]
    total_dominios["usuarios.tablero10anyo"] = total_dominios["usuarios.tablero10fecha"].str[0:4]
    total_dominios["usuarios.tablero10mes"] = total_dominios["usuarios.tablero10fecha"].str[5:7]
    total_dominios["usuarios.tablero10dia"] = total_dominios["usuarios.tablero10fecha"].str[8:10]
    total_dominios["usuarios.tablero10hora"] = total_dominios["usuarios.tablero10fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
    total_dominios["usuarios.tablero10minuto"] = total_dominios["usuarios.tablero10fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
    total_dominios= total_dominios.rename(columns={'site_id' : 'usuarios.tablero10siteIDWeb'
                                                  ,'Categoria' : 'usuarios.tablero10categoriaPagina'
                                                  ,'url-dominio' : 'usuarios.tablero10sitioWeb'
                                                  ,'total.dominio': 'usuarios.tablero10visitas.sitioWeb'})
    total_dominios['tablero10nombreDepartamento'] = total_dominios['usuarios.tablero10nombreDepartamento']
    total_dominios['tablero10nombreMunicipio'] = total_dominios['usuarios.tablero10nombreMunicipio']
    total_dominios['tablero10idBeneficiario'] = total_dominios['usuarios.tablero10idBeneficiario']
    total_dominios['tablero10fecha'] = total_dominios['usuarios.tablero10fecha']
    total_dominios['tablero10anyo'] = total_dominios['usuarios.tablero10anyo']
    total_dominios['tablero10mes'] = total_dominios['usuarios.tablero10mes']
    total_dominios['tablero10dia'] = total_dominios['usuarios.tablero10dia']
    total_dominios['@timestamp'] = now.isoformat()
  
    
    salida = helpers.bulk(es, doc_generator_dom(total_dominios))
    print("Fecha: ", now,"- Total Sitios Webs insertados en indice principal:",salida[0])             
except Exception as e:
    print("Fecha: ", now,"- Nada insertado de Sitios Web en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[35]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia_usuarios_web',
        body = { 'jerarquia_usuarios_web': 'usuarios_web','usuarios.tablero10fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)


# In[ ]:




