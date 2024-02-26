#!/usr/bin/env python
# coding: utf-8

# ### ¿Qué hace este script?
# Calcula para cada dispositivo de red(AP) y rango de fecha, el tiempo promedio de sesión (usuarios.tiempoPromedioSesionSitio)
# y usuarios conectados

# In[32]:


from elasticsearch import Elasticsearch, helpers
from ssl import create_default_context
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import parametros
import random
import re
import sys
import time


# ## Conectando a ElasticSearch

# La ultima línea se utiliza para garantizar la ejecución de la consulta
# * timeout es el tiempo para cada ejecución
# * max_retries el número de intentos si la conexión falla
# * retry_on_timeout para activar los reitentos

# In[33]:


context = create_default_context(cafile=parametros.cafile)
es = Elasticsearch(
    parametros.servidor,
    http_auth=(parametros.usuario_EC, parametros.password_EC),
    scheme="https",
    port=parametros.puerto,
    ssl_context=context,
    timeout=120, max_retries=3, retry_on_timeout=True
)


# ### Calculando fechas para la ejecución

# * Se calculan las fechas para asociar al nombre del indice
# * fecha_hoy es usada para concatenar al nombre del indice principal previa inserción

# In[34]:


now = datetime.now()
fecha_hoy = str(now.strftime("%Y.%m.%d"))


# ### nombre de indice donde se insertará e indice para control de ejecución

# In[35]:


indice = parametros.usuarios_tablero07_index
indice_control = parametros.tableros_mintic_control


# ### Funcion para JSON compatible con ElasticSearch

# In[36]:


def filterKeys(document):
    return {key: document[key] for key in use_these_keys }


# ### Trae la ultima fecha para control de ejecución

# Cuando en el rango de tiempo de la ejecución, no se insertan nuevos valores, las fecha maxima en indice mintic no aumenta, por tanto se usa esta fecha de control para garantizar que incremente el bucle de ejecución

# In[37]:


total_docs = 1
try:
    response = es.search(
        index= indice_control,
        body={
               "_source": ["usuarios.Tablero07.fechaControl"],
              "query": {
                "bool": {
                  "filter": [
                  {
                    "exists": {
                      "field":"jerarquia-tablero07"
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
        fecha_ejecucion = doc["_source"]['usuarios.Tablero07.fechaControl']
except Exception as e:
    print("Error:")
    print(e)
    fecha_ejecucion = '2021-05-01 00:00:00'
    pass
if response["hits"]["hits"] == []:
    fecha_ejecucion = '2021-05-01 00:00:00'
print("ultima fecha para control de ejecucion:",fecha_ejecucion)


# ### leyendo indice semilla-inventario

# En el script que ingesta semilla, trae la información de los centros de conexión administrados. Para el indice principal se requiere:
# * site_id como llave del centro de conexión.
# * Datos geográficos (Departamento, municipio, centro poblado, sede, energía, latitud, longitud, COD_ISO, id_Beneficiario).

# In[38]:


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
    datos_semilla['site_id'] = datos_semilla['site_id'].str.strip()
except:
    exit()


# ### Cambiando nombre de campos y generando location

# * Se valida latitud y longitud. Luego se calcula campo location<br>
# * Se renombran los campos de semilla

# In[39]:


def get_location(x,y='lat'):
    patron = re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$') #patrón que debe cumplir
    if (not patron.match(x) is None) and (str(x)!=''):
        return x.replace(',','.')
    else:
        #Código a ejecutar si las coordenadas no son válidas
        return '4.596389' if y=='lat' else '-74.074639'
    
datos_semilla['latitud'] = datos_semilla['latitud'].apply(lambda x:get_location(x,'lat'))
datos_semilla['longitud'] = datos_semilla['longitud'].apply(lambda x:get_location(x,'lon'))

datos_semilla['usuarios.location'] = datos_semilla['latitud'] + ',' + datos_semilla['longitud']
datos_semilla['usuarios.location']=datos_semilla['usuarios.location'].str.replace('a,a','')
datos_semilla.drop(columns=['latitud','longitud'],inplace=True)

datos_semilla = datos_semilla.rename(columns={'lugar_cod' : 'usuarios.centroDigitalUsuarios'
                                            , 'nombre_municipio': 'usuarios.nombreMunicipio'
                                            , 'nombre_departamento' : 'usuarios.nombreDepartamento'
                                            , 'nombre_centro_pob': 'usuarios.localidad'
                                            , 'nombreSede' : 'usuarios.nomCentroDigital'
                                            , 'energiadesc' : 'usuarios.sistemaEnergia'
                                            , 'COD_ISO' : 'usuarios.codISO'
                                            , 'id_Beneficiario' : 'usuarios.idBeneficiario'})
datos_semilla.fillna('', inplace=True)


# Se descartan los registros que tengan la latitud y longitud vacía o no valida

# In[40]:


datos_semilla = datos_semilla.drop(datos_semilla[(datos_semilla["usuarios.location"]=='')].index)


# ### leyendo indice cambium-devicedevices

# Se lee la información de los dispositivos de red monitoreados por Cambium. En esta lectura no hay referencia de fechas ya que solo hay una ocurrencia por MAC de dispositivo de red.
# 
# * site_id es la llave para cruzar con cada centro de conexión.
# * mac, IP son datos básicos del dispositivo.
# * ap_group identifica los dispositivos como INDOOR u OUTDOOR

# In[41]:


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
    elastic_docs = response["hits"]["hits"]
    fields = {}
    for num, doc in enumerate(elastic_docs):
        source_data = doc["_source"]
        for key, val in source_data.items():
            try:
                fields[key] = np.append(fields[key], val)
            except KeyError:
                fields[key] = np.array([val])
    datos_dev = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in fields.items() ]))
    datos_dev['site_id'] = datos_dev['site_id'].str.strip()
except:
    exit()


# In[42]:


datos_dev.dropna(subset=['site_id'], inplace=True)
datos_dev.fillna('', inplace=True)
datos_dev = datos_dev.drop(datos_dev[(datos_dev['site_id']=='')].index)


# Se corrigen datos de ap group con formato no valido

# In[43]:


datos_dev['ap_group'] = datos_dev['ap_group'].str.split("-", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split("_", n = 1, expand = True)[0]
datos_dev['ap_group'] = datos_dev['ap_group'].str.split(".", n = 1, expand = True)[0]
datos_dev = datos_dev.drop(datos_dev[(datos_dev['ap_group']=='')].index)


# Se toman solo los datos con mac unicos.

# In[44]:


datos_dev = datos_dev.drop_duplicates('mac')


# Se cambia el nombre a la mac del dispositivo de red para no confundir con la de dispositivos de usuario 

# In[45]:


datos_dev= datos_dev.rename(columns={'mac' : 'usuarios.macRed','ap_group' : 'usuarios.apGroup'})


# ### Se lee información de Ohmyfi consumos

# Apartir de esta lectura se toma el valor del tiempo promedio de sesión en minutos. Se toma de referencia los campos:
# * fecha_inicio a partir de la cual se calcula fecha control
# * tiempo_sesion_minutos
# * mac_ap
# * lugar_cod

# In[46]:


def traeSesiones(fecha_max,fecha_tope):
    total_docs = 500000
    response = es.search(
        index= parametros.ohmyfi_consumos_index,
        body={
                  "_source": ["lugar_cod", "tiempo_sesion_minutos","mac_ap","fecha_inicio"]
                 ,"query": {
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
    return pd.DataFrame([x["_source"] for x in elastic_docs])


# ### Traer datos de ohmyfi-detalleconexiones

# El rango de fechas será definido tomando de referencia la ultima fechahora del indice mintic-concat.
# 
# Campos extaidos:
# * fechahora de la conexión
# * fecha_control es un campo calculado a partir de fechahora. es lo mismo pero con el valor de minuto redondeado a 0.
# * lugar_cod clave para asociar con semilla
# * mac_usuario asociado al dispositivo que realizó la conexión

# In[47]:


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
    return pd.DataFrame([x["_source"] for x in elastic_docs])


# ### Se ejecuta consulta de datos

# * Se calcula rango en base a la fecha de control. Para este caso es de 50 minutos.
# * Se ejecuta la función de consulta con el rango de fechas.
# * Si no retorna datos se incrementa el rango y se ejecuta nuevamente. Este proceso se repite hasta conseguir datos o hasta que el rango de ejecución alcance la fecha y hora actual.

# In[48]:


fecha_max_mintic = fecha_ejecucion

fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_consumos = traeSesiones(fecha_max_mintic,fecha_tope_mintic)

if datos_consumos is None or datos_consumos.empty:
    while (datos_consumos is None or datos_consumos.empty) and ((datetime.strptime(fecha_max_mintic[0:50], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        datos_consumos = traeSesiones(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# In[49]:


datos_consumos['lugar_cod'] = datos_consumos['lugar_cod'].str.strip()


# In[50]:


datos_det_conex = trae_conexiones(fecha_max_mintic,fecha_tope_mintic)


# In[51]:


datos_det_conex['lugar_cod'] = datos_det_conex['lugar_cod'].str.strip()


# In[52]:


continuar = True
if datos_det_conex.empty:
    print("Dataframe vacio")
    print(fecha_max_mintic)
    print(fecha_tope_mintic)
    
    continuar = False
else:
    datos_det_conex['fecha'] = datos_det_conex['fecha_control'].str.split(" ", n = 1, expand = True)[0]
    datos_det_conex.drop_duplicates(subset=["fecha_control","lugar","lugar_cod","mac_usuario", "dispositivo","sistema_operativo",'tipodoc','documento'],inplace=True)


# In[53]:


if  continuar:
    
    datos_det_conex = datos_det_conex.rename(columns={'lugar_cod' : 'site_id'
                                                             ,'fechahora':'usuarios.fechaConexionUsuarios'
                                                             ,'dispositivo': 'usuarios.tipoDispositivoUsuarios'
                                                             , 'sistema_operativo': 'usuarios.sistemaOperativoUsuarios'})


# ### Se lee el indice all-cambium-device-client

# * En este indice se guarda el detalle de los radio por fecha<br>
# * Detalle conexiones cruza con device clients. Con estos se calculan los totales por marca

# In[54]:


def traeRadio(fecha_max,fecha_tope):
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
                    }
                  }
              }
        },
        size=total_docs
    )
    elastic_docs = response["hits"]["hits"]
    return pd.DataFrame([x["_source"] for x in elastic_docs])    


# In[55]:


fecha_max_mintic = fecha_ejecucion

fecha_tope_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
datos_performance = traeRadio(fecha_max_mintic,fecha_tope_mintic)

if datos_performance is None or datos_performance.empty:
    while (datos_performance is None or datos_performance.empty) and ((datetime.strptime(fecha_max_mintic[0:10], '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")) < str(now.strftime("%Y-%m-%d %H:%M:%S"))):
        fecha_max_mintic = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        fecha_tope_mintic = (datetime.strptime(fecha_tope_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")
        datos_performance = traeRadio(fecha_max_mintic,fecha_tope_mintic)
else:
    pass


# Se cruza all-cambium-device-clients con cambium-devicedevices para obtener el site_id

# In[56]:


datos_performance = datos_performance.rename(columns={'ap_mac':'usuarios.macRed', 'mac':'mac_usuario'})


# In[57]:


usuarios_conectados_cambium = pd.merge(datos_performance,datos_dev, on ='usuarios.macRed', how='inner')


# # Escribiendo en indice la información de tiempo promedio sesión en sitio

# In[58]:


use_these_keys = ['usuarios.fecha'
                  , 'usuarios.siteID'
                  ,'usuarios.tiempoPromedioSesionSitio'
                  , 'usuarios.nomCentroDigital'
                  , 'usuarios.codISO'
                  , 'usuarios.idBeneficiario'
                  , 'usuarios.localidad'
                  , 'usuarios.nombreDepartamento'
                  , 'usuarios.sistemaEnergia'
                  , 'usuarios.nombreMunicipio'
                  , 'usuarios.location'
                  , 'usuarios.macRed'
                  , 'usuarios.apGroup'
                  , 'usuarios.fechaControl'
                  , 'usuarios.anyo'
                  , 'usuarios.mes'
                  , 'usuarios.dia'
                  , 'usuarios.hora'
                  , 'usuarios.minuto'
                    , 'nombreDepartamento'
                    , 'nombreMunicipio'
                    , 'idBeneficiario'
                    , 'fecha'
                    , 'anyo'
                    , 'mes'
                    , 'dia'
                  , '@timestamp']
def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            document = document.rename(index={'usuarios.tiempoPromedioSesionSitio_x': 'usuarios.tiempoPromedioSesionSitio','usuarios.macRed_x':'usuarios.macRed','usuarios.apGroup_x':'usuarios.apGroup','usuarios.fechaControl_x':'usuarios.fechaControl'})
            yield {
                    "_index": indice, 
                    "_id": f"{str(document['usuarios.siteID']) + '-' + str(document['usuarios.fechaControl']) + '-' + str(document['usuarios.macRed'])+'-'+str(random.randrange(10000000))}",
                    "_source": filterKeys(document),
                }
            


# Se agrupa por lugar_cod, fecha_control, mac_ap, mac_usuario y se promedia el tiempo_sesion_minutos. Este genera el campo del indice final:<br>
# * usuarios.tiempoPromedioSesionSitio

# In[59]:


if  continuar:

    try:

        datos_consumos = datos_consumos.rename(columns={'lugar_cod' :'site_id'})
        datos_consumos['mac_ap'] = datos_consumos['mac_ap'].str.replace('-',':')
        datos_consumos['fecha_control'] = datos_consumos["fecha_inicio"].str[0:-4] + '0:00'
        tiempoPromedioSesionSitio=datos_consumos[['site_id','mac_ap','fecha_control','tiempo_sesion_minutos']].groupby(['site_id','mac_ap','fecha_control']).agg(['mean']).reset_index()
        tiempoPromedioSesionSitio.columns = tiempoPromedioSesionSitio.columns.droplevel(1)
        tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'tiempo_sesion_minutos' : 'usuarios.tiempoPromedioSesionSitio'})
        tiempoPromedioSesionSitio['usuarios.tiempoPromedioSesionSitio'] = round(tiempoPromedioSesionSitio['usuarios.tiempoPromedioSesionSitio'],6)
        tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'mac_ap' : 'usuarios.macRed'
                                                                              ,'fecha_control' : 'usuarios.fechaControl'})
        tiempoPromedioSesionSitio = pd.merge(tiempoPromedioSesionSitio,datos_semilla, on ='site_id', how='inner')
        tiempoPromedioSesionSitio = pd.merge(tiempoPromedioSesionSitio, datos_dev, on=['site_id','usuarios.macRed'], how='left')
        tiempoPromedioSesionSitio.fillna({'usuarios.apGroup':'No identificado'},inplace=True)

        ##################
        tiempoPromedioSesionSitio = tiempoPromedioSesionSitio.rename(columns={'site_id' : 'usuarios.siteID'})
        try:
            tiempoPromedioSesionSitio["usuarios.fecha"] = tiempoPromedioSesionSitio["usuarios.fechaControl"].str[0:10]
        except:
            tiempoPromedioSesionSitio["usuarios.fecha"] = ""

        try:
            tiempoPromedioSesionSitio["usuarios.anyo"] = tiempoPromedioSesionSitio["usuarios.fecha"].str[0:4]
        except:
            tiempoPromedioSesionSitio["usuarios.anyo"] = ""

        try:
            tiempoPromedioSesionSitio["usuarios.mes"] = tiempoPromedioSesionSitio["usuarios.fecha"].str[5:7]
        except:
            tiempoPromedioSesionSitio["usuarios.mes"] = ""

        try:
            tiempoPromedioSesionSitio["usuarios.dia"] = tiempoPromedioSesionSitio["usuarios.fecha"].str[8:10]
        except:
            tiempoPromedioSesionSitio["usuarios.dia"] = ""

        try:
            tiempoPromedioSesionSitio["usuarios.hora"] = tiempoPromedioSesionSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]["usuarios.hora"] = tiempoPromedioSesionSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[0]
        except:
            tiempoPromedioSesionSitio["usuarios.hora"] = ""

        try:
            tiempoPromedioSesionSitio["usuarios.minuto"] = tiempoPromedioSesionSitio["usuarios.fechaControl"].str.split(" ", n = 1, expand = True)[1].str.split(":", n = 2, expand = True)[1]
        except:
            tiempoPromedioSesionSitio["usuarios.minuto"] = ""

        tiempoPromedioSesionSitio['nombreDepartamento'] = tiempoPromedioSesionSitio['usuarios.nombreDepartamento']
        tiempoPromedioSesionSitio['nombreMunicipio'] = tiempoPromedioSesionSitio['usuarios.nombreMunicipio']
        tiempoPromedioSesionSitio['idBeneficiario'] = tiempoPromedioSesionSitio['usuarios.idBeneficiario']
        tiempoPromedioSesionSitio['fecha'] = tiempoPromedioSesionSitio['usuarios.fecha']
        tiempoPromedioSesionSitio['anyo'] = tiempoPromedioSesionSitio['usuarios.anyo']
        tiempoPromedioSesionSitio['mes'] = tiempoPromedioSesionSitio['usuarios.mes']
        tiempoPromedioSesionSitio['dia'] = tiempoPromedioSesionSitio['usuarios.dia']
        tiempoPromedioSesionSitio['@timestamp'] = now.isoformat()
        
        salida = helpers.bulk(es, doc_generator(tiempoPromedioSesionSitio))

        print("Fecha: ", now,"- Tiempo promedio sesion en sitio insertado en indice principal:",salida[0])

    except Exception as e:
        print(e)
        print("Fecha: ", now,"- Nada para insertar en indice principal")


# # Escribiendo en indice la información de usuarios conectados

# In[60]:


use_these_keys = ['usuarios.fecha'
                  , 'usuarios.siteID'
                  , 'usuarios.nomCentroDigital'
                  , 'usuarios.codISO'
                  , 'usuarios.idBeneficiario'
                  , 'usuarios.localidad'
                  , 'usuarios.nombreDepartamento'
                  , 'usuarios.sistemaEnergia'
                  , 'usuarios.nombreMunicipio'
                  , 'usuarios.location'
                  , 'usuarios.usuariosConectados'
                  , 'usuarios.sesiones_Usuarios'
                  , 'usuarios.fechaControl'
                  , 'usuarios.anyo'
                  , 'usuarios.mes'
                  , 'usuarios.dia'
                    , 'nombreDepartamento'
                    , 'nombreMunicipio'
                    , 'idBeneficiario'
                    , 'fecha'
                    , 'anyo'
                    , 'mes'
                    , 'dia'
                  , '@timestamp']
def doc_generator_usuconectados(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": indice, 
                    "_id": f"{str(document['usuarios.siteID']) + '-' + str(document['usuarios.fechaControl']) + '-' +str(random.randrange(10000000))}",
                    "_source": filterKeys(document),
                }
            


# In[61]:


if  continuar:

    try:
        datos_logins = datos_det_conex[['fecha_control', 'site_id', 'documento']].groupby(["fecha_control","site_id"])['documento'].nunique().reset_index()
        datos_logins= datos_logins.rename(columns={'documento' : 'usuarios.sesiones_Usuarios'})

        ###################################################################################################


        #Se trae usuarios conectados desde cambium devices clients 
        usuariosConectados = usuarios_conectados_cambium[["fecha_control","site_id","mac_usuario"]].groupby(["fecha_control","site_id"]).agg(['count']).reset_index()
        usuariosConectados.columns = usuariosConectados.columns.droplevel(1)
        usuariosConectados= usuariosConectados.rename(columns={'mac_usuario' : 'usuarios.usuariosConectados'})


        ###################################################################################################

        usuariosConectados = pd.merge(usuariosConectados,datos_logins,  how='outer')
        usuariosConectados.fillna({'usuarios.usuariosConectados': 0
                                   ,'usuarios.sesiones_Usuarios' : 0 },inplace=True)
        usuariosConectados['usuarios.usuariosConectados'] = usuariosConectados['usuarios.usuariosConectados'].astype(int)
        usuariosConectados['usuarios.sesiones_Usuarios'] = usuariosConectados['usuarios.sesiones_Usuarios'].astype(int)
        usuariosConectados = pd.merge(datos_semilla,  usuariosConectados, on=['site_id'], how='inner')
        usuariosConectados = usuariosConectados.rename(columns={'fecha_control':'usuarios.fechaControl'
                                                               ,'site_id' : 'usuarios.siteID'})
            

        try:
            usuariosConectados["usuarios.fecha"] = usuariosConectados["usuarios.fechaControl"].str[0:10]
        except:
            usuariosConectados["usuarios.fecha"] = ""

        try:
            usuariosConectados["usuarios.anyo"] = usuariosConectados["usuarios.fecha"].str[0:4]
        except:
            usuariosConectados["usuarios.anyo"] = ""

        try:
            usuariosConectados["usuarios.mes"] = usuariosConectados["usuarios.fecha"].str[5:7]
        except:
            usuariosConectados["usuarios.mes"] = ""

        try:
            usuariosConectados["usuarios.dia"] = usuariosConectados["usuarios.fecha"].str[8:10]
        except:
            usuariosConectados["usuarios.dia"] = ""

        

        usuariosConectados['nombreDepartamento'] = usuariosConectados['usuarios.nombreDepartamento']
        usuariosConectados['nombreMunicipio'] = usuariosConectados['usuarios.nombreMunicipio']
        usuariosConectados['idBeneficiario'] = usuariosConectados['usuarios.idBeneficiario']
        usuariosConectados['fecha'] = usuariosConectados['usuarios.fecha']
        usuariosConectados['anyo'] = usuariosConectados['usuarios.anyo']
        usuariosConectados['mes'] = usuariosConectados['usuarios.mes']
        usuariosConectados['dia'] = usuariosConectados['usuarios.dia']
        usuariosConectados['@timestamp'] = now.isoformat()


        salida = helpers.bulk(es, doc_generator_usuconectados(usuariosConectados))

        print("Fecha: ", now,"- Usuarios Conectados y dispositivos conectados en sitio insertado en indice principal:",salida[0])

    except Exception as e:
        print(e)
        print("Fecha: ", now,"- Nada para insertar en indice principal")


# ### Guardando fecha para control de ejecución

# * Se actualiza la fecha de control. Si el calculo supera la fecha hora actual, se asocia esta ultima.

# In[62]:


fecha_ejecucion = (datetime.strptime(fecha_max_mintic, '%Y-%m-%d %H:%M:%S')+timedelta(minutes=50)).strftime("%Y-%m-%d %H:%M:%S")[0:15] + '0:00'    

if fecha_ejecucion > str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00':
    fecha_ejecucion = str(now.strftime('%Y-%m-%d %H:%M:%S'))[0:15] + '0:00'
response = es.index(
        index = indice_control,
        id = 'jerarquia-tablero07',
        body = { 'jerarquia-tablero07': 'jerarquia-tablero07','usuarios.Tablero07.fechaControl' : fecha_ejecucion}
)
print("actualizada fecha control de ejecucion:",fecha_ejecucion)

