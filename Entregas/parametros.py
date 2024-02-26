#Globales
#Servidores Elastic Search
servidor = 'hotzone3'
#Certificado par ala conexion ES
cafile="/etc/logstash/certs/ca.crt"
#Datos conexión ES
usuario_EC = 'elastic'
password_EC = 'czUAEagbe5RsP2VGvQ7R'
puerto=9200

####################################
#Globales para cambium
url_cambium = ["https://100.123.26.252/api/v2/","https://100.123.26.224/api/v2/"]
#cambium_token_aux = ['4762523963aec71c3633d3e39e5c3c04532bbcf0','9560bca7108eec506dc48338e2005186ab892797']
cambium_token_aux = ['337f5211e65afed0b46bd67d078f10f723f01217','64e4f49e05266b9dfe06f5adf9ec324bb58db46c']
#Para cambium-alarmhistory
cambium_a_h_index="cambium-alarmhistory"
#cambium_a_h_url= url_cambium + "alarms/history"

#Para cambium-deviceclients
cambium_d_c_index="cambium-deviceclients"
#cambium_d_c_url= url_cambium + "devices/clients"

#Para cambium-devices-performance
cambium_d_p_index = "cambium-devicesperformance"
#cambium_d_p_url= url_cambium + "devices/performance"


#Nuevo cambium performance
#Para cambium-devices-newperformance
cambium_d_p_n_index = "hitst-cambium-deviceperformance"
#cambium_d_p_n_url= url_cambium + "devices/"+mac+"/performance"





#Para cambium-device-devices
cambium_d_d_index="cambium-devicedevices"
#cambium_devices_url= url_cambium + "devices/"


#Para cambium-devicemac
cambium_d_mac_index = "cambium-devicemac"

#Para cambium-devices-statistics
cambium_d_sta_index = "cambium-devstatistics"
####################################
#Globales para Ohmyfi
url_ohmyfi = "https://www.ohmyfi.com/ApiOMF"
ohmyfi_api_key = "okMOpLAkiYpafQKXhXirwUys"

#Para ohmyfi-valoraciones
ohmyfi_val_index = "ohmyfi-valoraciones"
#ohmyfi_val_url = "/valoraciones"

#Para ohmyfi-detalleconexiones
ohmyfi_d_c_index = "ohmyfi-detalleconexiones"
#ohmyfi_d_c_url = "/detalleconexiones"

#Para ohmyfi-estadisticaresumen-dispositivosnuevos
ohmyfi_e_r_d_n_index = 'ohmyfi-estadisticaresumen-dispositivosnuevos'

#Para ohmyfi-estadisticaresumen-dispositivos
ohmyfi_e_r_d_index = 'ohmyfi-estadisticaresumen-dispositivos'

#Para ohmyfi-estadisticaresumen-logins
ohmyfi_e_r_l_index = 'ohmyfi-estadisticaresumen-logins'

#Para ohmyfi-estadisticaresumen-visitas
ohmyfi_e_r_v_index = 'ohmyfi-estadisticaresumen-visita'

#Para ohmyfi-datosusuario
ohmyfi_d_u_index = 'ohmyfi-datosusuarios'

#Para ohmyfi-resumenconexiones
ohmyfi_r_c_index = 'ohmyfi-resumenconexiones'
ohmyfi_total_c_index = 'ohmyfi-total-conexiones-historico'  
#Para ohmyfi-recurrenciausuarios
ohmyfi_r_u_index = 'ohmyfi-recurrenciausuarios'

# Para ohmyfi-consumos
ohmyfi_consumos_index = 'ohmyfi-consumos'
####################################
#Globales para semilla
semilla_inventario_index = "semilla-inventario"

####################################
#Globales BMC
bmc_url = 'https://mintictsim.triara.co/bppmws/api/Event/search?routingId=entuity&routingIdType=CELL_NAME'
bmc_url_token = 'https://mintictsps.triara.co/tsws/api/v10.1/token'
bmc_username= 'minticrepor'
bmc_password = 'Colombia123'
bmc_index_1 = 'bmc-01'

####################################
#Speed test
speed_index = 'speedtest'

####################################
#Para SonycWall
sonicwall_index = 'prod-sonicwall'
sonicwall_raw = 'filebeat-7.10.2'
sonicwall_categorias = 'sonicwall-categorias'
####################################
#Globales Indice principal
mintic_concat_index = "dev-mintic-concat"
usuarios_mintic_concat_index = "usuarios-mintic-concat"
usuarios_mintic_concat_index_tablero10 = "dev_prueba_10"

#control de ejecucion
mintic_control = 'dev-control_ejecucion_mintic'
usuarios_mintic_control = 'usuarios-dev-control_ejecucion_mintic'
usuarios_mintic_control_tablero10 = "dev-trafico-tablero10"
gestion_mintic_concat_index = "gestion-mintic-concat"
trafico_mintic_concat_index = "dev-trafico-mintic-concat"
##########################
#Indices Tableros
trafico_tablero04_index = "dev-trafico-tablero04"
trafico_tablero05_index = "dev-trafico-tablero05"
trafico_tablero06_index = "dev-trafico-tablero06"
usuarios_tablero07_index = "dev-usuarios-tablero07"
usuarios_tablero08_index = "dev-usuarios-tablero08"
usuarios_tablero09_index = "dev-usuarios-tablero09"
usuarios_tablero11_index = "dev-usuarios-tablero11"
usuarios_tablero12_index = "dev-usuarios-tablero12"
usuarios_tablero13_index = "dev-usuarios-tablero13"
gestion_tablero14_index = "dev-gestion-tablero14"
gestion_tableros_gestion_index  = "dev-gestion-tableros-gestion"
trafico_tableros_trafico_index  = "dev-trafico-tableros-trafico"

disponibilidad_tableros_index  = "dev-disponibilidad-tableros"

gestion_tablero16_index = "dev-gestion-tablero16"
conteo_centros_index = "dev-conteo-centros"
usuarios_tableros_usuarios_index="dev-usuarios-tableros-usuarios"
#Indice Control tableros
tableros_mintic_control = "dev-tableros_mintic_control"
####################################
##Service manager
sm_interaciones = 'servicemanager-interacciones-tmp_'
sm_incidentes = 'servicemanager-incidentes-tmp_'
