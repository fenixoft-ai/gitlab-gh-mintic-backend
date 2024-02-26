#!/usr/bin/env python
# coding: utf-8

# ### ¿Qué hace este script?
# Para cada sitio y fecha calcula:
# 
# * usuarios.nivelesSatisfaccionUsuarios: Clasificación de la respuesta (bueno, regular, malo)
# * usuarios.nivelesSatisfaccionUsuariosRespuesta: Total de respuestas para cada opción
# * usuarios.nivelesSatisfaccionUsuariosRespuestaPorCien (porcentaje asociado al total para ese grupo de respuetas)
# * usuarios.cantidadCalificaciones: total de calificaciones (incluye todas las opciones)

# In[1]:


from elasticsearch import Elasticsearch, helpers
import pandas as pd
import numpy as np
from ssl import create_default_context
from datetime import datetime, timedelta
import time
import parametros #parametros propios del desarrollo
import random


# ### Conexión a Elastic Search

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

# In[4]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### Nombre de índice donde se insertará

# Se define tanto el índice principal como el que controla la ejecución

# In[5]:


indice = parametros.usuarios_tableros_usuarios_index
indice_control = parametros.tableros_mintic_control


# ### Funcion que genera JSON compatible con ElasticSearch

# In[6]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### Trae la última fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha máxima en índice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[7]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
               "_source": ["tablero13.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia-tablero13"
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
        fecha_ejecucion = doc["_source"]['tablero13.fechaControl']
except:
    fecha_ejecucion = '2021-06-01 00:00:00'
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-06-01 00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# fecha_ejecucion = '2021-06-16 00:00:00'

# ### Leyendo índice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el índice principal se requiere:<br>
# * site_id como llave del centro de conexión.<br>
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud,  COD_ISO , id_Beneficiario).

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
    print ("Error")


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location<br>
# * Se renombran los campos de semilla

# In[9]:


import re
def get_location(x):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrÃ³n que debe cumplir
    if (not patron.match(x) is None):
        return x.replace(',','.')
    else:
        #CÃ³digo a ejecutar si las coordenadas no son vÃ¡lidas
        return 'a'
datos_semilla['latitud'] = datos_semilla['latitud'].apply(get_location)
datos_semilla['longitud'] = datos_semilla['longitud'].apply(get_location)
datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["longitud"]=='a') | (datos_semilla["latitud"]=='a')].index)
datos_semilla['usuarios.tablero13.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.tablero13.location']=datos_semilla['usuarios.tablero13.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)


# In[10]:


datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.tablero13.centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.tablero13.nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.tablero13.nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.tablero13.localidad'
                                            , 'nombreSede' : 'usuarios.tablero13.nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.tablero13.sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.tablero13.codISO'
                                            , 'id_Beneficiario' : 'idBeneficiario'})


# Se descartan los registros que tengan la latitud y longitud vacía o no valida

# In[11]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.tablero13.location"]=='')].index)


# ## Leyendo índice ohmyfi valoraciones

# Se toman en cuenta todas las valoraciones realizadas dentro del rango de fecha fecha. Campos leídos:<br>
# <br>
# * lugar_cod que es la llave para cruzar con site_id.<br>
# * datos asociados a la valoración: pregunta, respuesta, fechahora (cuando se registrá la valoración)

# In[12]:


def trae_valoraciones(fecha_ini, fecha_fin, client):
    
    query = {
        "_source": ["lugar_cod", "respuesta","fechahora","@timestamp"], 
        "query": {
            "range": {
                "fechahora": {
                    "gte": fecha_ini,
                    "lt":  fecha_fin
                }
            }
        }
    }

    return custom_scan(
        query, 
        parametros.ohmyfi_val_index,
        total_docs=1000000, 
        client=client
    )


# ### Se realiza la consulta de datos

# * Se calcula rango en base a la fecha de control. Para este caso es de un día.<br>
# * Se ejecuta la función de consulta con el rango de fechas.<br>
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.

# In[13]:


fecha_max_mintic = fecha_ejecucion
fecha_tope_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')
fecha_tope_mintic += timedelta(minutes=50)
fecha_tope_mintic -= timedelta(seconds=1)
fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")

datos_valoraciones =  trae_valoraciones(fecha_max_mintic, fecha_tope_mintic, es)

if datos_valoraciones is None or datos_valoraciones.empty:
    while (datos_valoraciones is None or datos_valoraciones.empty) and ((datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        
        fecha_max_mintic = datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')
        fecha_max_mintic += timedelta(minutes=50)
        fecha_max_mintic = fecha_max_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        fecha_tope_mintic = datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S') 
        fecha_tope_mintic += timedelta(minutes=50)
        fecha_tope_mintic = fecha_tope_mintic.strftime("%Y-%m-%d %H:%M:%S")
        
        datos_valoraciones = trae_valoraciones(fecha_max_mintic, fecha_tope_mintic, es)
        
else:
    pass


# 1. Se descartan las respuestas Si y No de las valoraciones, las cuales corresponden a la pregunta: "Te gustaría calificar tu última conexión en". De esta forma solo se dejan las respuestas asociadas a la percepción de calidad por parte del usuario.<br>
# 2. Se estandariza lugar_cod a site_id<br>
# 3. Se estadariza fecha para agrupar (solo se toma yyyy-mm-dd)<br>
# 4. Se Calcula nivel de valoración por pregunta y total de valoraciones<br>
# <br>
# * Para cada centro de conexión, pregunta, se contabilizan las valoraciones. El calculo es diario

# In[14]:


try:
    datos_valoraciones = datos_valoraciones.drop(datos_valoraciones[(datos_valoraciones["respuesta"].isin(['Si','No']))].index)
    #datos_valoraciones['mac_usuario'] = datos_valoraciones['mac_usuario'].str.replace('-',':')
    datos_valoraciones = datos_valoraciones.rename(columns={'lugar_cod': 'site_id'})
    datos_valoraciones['fecha'] = datos_valoraciones["fechahora"].str.split(" ", n = 1, expand = True)[0]
    nivel_valoraciones=datos_valoraciones[['fechahora', 'site_id'
                                         , 'respuesta'
                                         , 'fecha']].groupby(['site_id','respuesta','fecha']).agg(['count']).reset_index()
    nivel_valoraciones.columns = nivel_valoraciones.columns.droplevel(1)
    nivel_valoraciones = nivel_valoraciones.rename(columns={'fechahora' :'usuarios.tablero13.nivelesSatisfaccionUsuarios'
                                                           ,'respuesta' :'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuesta'
                                                           ,'fecha' : 'usuarios.tablero13.fechaCalificacion'})
    
    total_valoraciones = datos_valoraciones[['site_id','fechahora','fecha']].groupby(['site_id','fecha']).agg(['count']).reset_index()
    total_valoraciones.columns = total_valoraciones.columns.droplevel(1)
    total_valoraciones = total_valoraciones.rename(columns={'fechahora' :'usuarios.tablero13.cantidadCalificaciones'
                                                           ,'fecha' : 'usuarios.tablero13.fechaCalificacion'})

    #Con ambos se calcula el porcentaje de cada respuesta
    nivel_valoraciones =  pd.merge(nivel_valoraciones,total_valoraciones, on=['site_id','usuarios.tablero13.fechaCalificacion'],how='inner')
    nivel_valoraciones['usuarios.tablero13.nivelesSatisfaccionUsuariosRespuestaPorCien'] = ((nivel_valoraciones['usuarios.tablero13.nivelesSatisfaccionUsuarios']) / nivel_valoraciones['usuarios.tablero13.cantidadCalificaciones']).round(4)
except:
    total_valoraciones = pd.DataFrame(columns=['site_id','usuarios.tablero13.fechaCalificacion'
                                              ,'usuarios.tablero13.cantidadCalificaciones'])
    nivel_valoraciones = pd.DataFrame(columns=['site_id','usuarios.tablero13.fechaCalificacion'
                                              ,'usuarios.tablero13.nivelesSatisfaccionUsuarios'
                                              ,'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuesta'
                                              ,'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuestaPorCien'])
    


# ### Cruzando con semilla las agregaciones de valoraciones

# In[15]:


mintic_valoraciones = pd.merge(datos_semilla, nivel_valoraciones, on='site_id',how='inner')


# In[16]:


mintic_valoraciones["usuarios.tablero13.nivelesSatisfaccionUsuarios"].sum(axis=0)


# # Escribiendo en indice la información de Valoraciones

# Se convierten los nulos a ceros a nivelesSatisfaccionUsuarios y cantidadCalificaciones

# In[17]:


try:
    mintic_valoraciones.fillna({'usuarios.tablero13.nivelesSatisfaccionUsuarios':0
                               ,'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuestaPorCien':0},inplace=True)
    mintic_valoraciones[['usuarios.tablero13.nivelesSatisfaccionUsuarios']] = mintic_valoraciones[['usuarios.tablero13.nivelesSatisfaccionUsuarios']].astype(int)
    mintic_valoraciones = mintic_valoraciones.rename(columns={'site_id' : 'usuarios.tablero13.siteID'})
    mintic_valoraciones.dropna(subset=['usuarios.tablero13.nivelesSatisfaccionUsuariosRespuesta'], inplace=True)
    mintic_valoraciones["usuarios.tablero13.anyo"] = mintic_valoraciones["usuarios.tablero13.fechaCalificacion"].str[0:4]
    mintic_valoraciones["usuarios.tablero13.mes"] = mintic_valoraciones["usuarios.tablero13.fechaCalificacion"].str[5:7]
    mintic_valoraciones["usuarios.tablero13.dia"] = mintic_valoraciones["usuarios.tablero13.fechaCalificacion"].str[8:10]
    mintic_valoraciones['nombreDepartamento'] = mintic_valoraciones['usuarios.tablero13.nombreDepartamento']
    mintic_valoraciones['nombreMunicipio'] = mintic_valoraciones['usuarios.tablero13.nombreMunicipio']
    mintic_valoraciones['idBeneficiario'] = mintic_valoraciones['idBeneficiario']
    mintic_valoraciones['fecha'] = mintic_valoraciones['usuarios.tablero13.fechaCalificacion']
    mintic_valoraciones['anyo'] = mintic_valoraciones['usuarios.tablero13.anyo']
    mintic_valoraciones['mes'] = mintic_valoraciones['usuarios.tablero13.mes']
    mintic_valoraciones['dia'] = mintic_valoraciones['usuarios.tablero13.dia']
except:
    
    print('Null')
    
    pass


# In[18]:


use_these_keys = ['usuarios.tablero13.nomCentroDigital'
                  , 'usuarios.tablero13.codISO'
                  , 'usuarios.tablero13.localidad'
                  , 'usuarios.tablero13.siteID'
                  , 'usuarios.tablero13.nombreDepartamento'
                  , 'usuarios.tablero13.sistemaEnergia'
                  , 'usuarios.tablero13.nombreMunicipio'
                  , 'idBeneficiario'
                  , 'usuarios.tablero13.location'
                  , 'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuesta'
                  , 'usuarios.tablero13.fechaCalificacion'
                  , 'usuarios.tablero13.nivelesSatisfaccionUsuarios'
                  , 'usuarios.tablero13.nivelesSatisfaccionUsuariosRespuestaPorCien'               
                  , 'usuarios.tablero13.anyo'
                  , 'usuarios.tablero13.mes'
                  , 'usuarios.tablero13.dia'
                    , 'nombreDepartamento'
                    , 'nombreMunicipio'
                    , 'idBeneficiario'
                    , 'fecha'
                    , 'anyo'
                    , 'mes'
                    , 'dia'
                  , '@timestamp']
try:
    mintic_valoraciones['@timestamp'] = now.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{'Valoracion-' + str(document['usuarios.tablero13.siteID']) + '-' + str(document['usuarios.tablero13.fechaCalificacion']) + '-' + str(document['usuarios.tablero13.nivelesSatisfaccionUsuariosRespuesta']) + '-' +str(random.randrange(10000))}",
                    "_source": filterKeys(document),
                }
    salida = helpers.bulk(es, doc_generator(mintic_valoraciones))
    print("Fecha: ", now,"- Valoraciones insertadas en indice principal:",salida[0])
except:
    print("Fecha: ", now,"- No se insertaron valoraciones en indice principal")


# ## Insertando total de calificaciones

# In[19]:


mintic_calificaciones = pd.merge(datos_semilla,  total_valoraciones, on='site_id', how='inner')


# In[20]:


try:
    mintic_calificaciones.fillna({'usuarios.tablero13.cantidadCalificaciones':0},inplace=True)
    mintic_calificaciones[['usuarios.tablero13.cantidadCalificaciones']] = mintic_calificaciones[['usuarios.tablero13.cantidadCalificaciones']].astype(int)
    mintic_calificaciones = mintic_calificaciones.rename(columns={'site_id' : 'usuarios.siteID'})
    mintic_calificaciones.dropna(subset=['usuarios.tablero13.cantidadCalificaciones'], inplace=True)
    mintic_calificaciones["usuarios.tablero13.anyo"] = mintic_calificaciones["usuarios.tablero13.fechaCalificacion"].str[0:4]
    mintic_calificaciones["usuarios.tablero13.mes"] = mintic_calificaciones["usuarios.tablero13.fechaCalificacion"].str[5:7]
    mintic_calificaciones["usuarios.tablero13.dia"] = mintic_calificaciones["usuarios.tablero13.fechaCalificacion"].str[8:10]
    mintic_calificaciones["usuarios.tablero13.fecha"] = mintic_calificaciones["usuarios.tablero13.fechaCalificacion"]
    mintic_calificaciones['nombreDepartamento'] = mintic_calificaciones['usuarios.tablero13.nombreDepartamento']
    mintic_calificaciones['nombreMunicipio'] = mintic_calificaciones['usuarios.tablero13.nombreMunicipio']
    mintic_calificaciones['idBeneficiario'] = mintic_calificaciones['usuarios.idBeneficiario']
    mintic_calificaciones['fecha'] = mintic_calificaciones['usuarios.tablero13.fechaCalificacion']
    mintic_calificaciones['anyo'] = mintic_calificaciones['usuarios.tablero13.anyo']
    mintic_calificaciones['mes'] = mintic_calificaciones['usuarios.tablero13.mes']
    mintic_calificaciones['dia'] = mintic_calificaciones['usuarios.tablero13.dia']
except:
    pass


# In[21]:


use_these_keys = ['usuarios.tablero13.nomCentroDigital'
                  , 'usuarios.tablero13.codISO'
                  , 'idBeneficiario'
                  , 'usuarios.tablero13.localidad'
                  , 'usuarios.siteID'
                  , 'usuarios.tablero13.nombreDepartamento'
                  , 'usuarios.tablero13.sistemaEnergia'
                  , 'usuarios.tablero13.nombreMunicipio'
                  , 'usuarios.tablero13.location'
                  , 'usuarios.tablero13.cantidadCalificaciones'
                  , 'usuarios.tablero13.fechaCalificacion'
                  , 'usuarios.tablero13.fecha'
                  , 'usuarios.tablero13.anyo'
                  , 'usuarios.tablero13.mes'
                  , 'usuarios.tablero13.dia'
                    , 'nombreDepartamento'
                    , 'nombreMunicipio'
                    , '@timestamp']
try:
    mintic_calificaciones['@timestamp'] = now.isoformat()
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{ 'Calificacion-' + str(document['usuarios.siteID']) + '-' + str(document['usuarios.tablero13.fechaCalificacion']) + '-' +str(random.randrange(10000))}",
                    "_source": filterKeys(document),
                }
    salida = helpers.bulk(es, doc_generator(mintic_calificaciones))
    print("Fecha: ", now,"- Total calificaciones insertadas en indice principal:",salida[0])
except:
    print("Fecha: ", now,"- No se insertaron totales de calificaciones en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[22]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia-tablero13',
        body = { 'jerarquia-tablero13': 'jerarquia-tablero13','tablero13.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)

