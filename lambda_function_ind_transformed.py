import pandas as pd
import json
from datetime import date,datetime
import boto3
import numpy as np
import io
import pytz

#pd.options.display.max_columns = None

def func_atentida(df) :
    if (df['agente'] !='' or df['agente'] !=None) and df['estado_llamada'] == 'Atendida':
        return 1
    elif (df['agente'] !='' or df['agente'] !=None) and (df['tipo_de_llamada']=='Manual' and  df['estado_llamada'] == 'Completada'):
        return 1
    else:
        return 0


def func_abandono(df) :
    if  (df['agente'] !='' or df['agente'] !=None) and df['estado_llamada'] =='Abandonada':
        return 1
    else:
        return 0


def lambda_handler(event, context):
    
    # Establecer zona horaria Perú
    tz = pytz.timezone('America/Bogota')
    ct = datetime.now(tz=tz)
    year = str(ct).replace('-','')[0:4]
    period = str(ct).replace('-','')[0:6]
    month = str(ct).replace('-','')[4:6]
    day = str(ct).replace('-','')[6:8]
    print(ct)
    day_now= year+month+day
    #print(year,month,day)
    #print(day_now)
    #print(year, period, day )
    
    
    
    s3_client = boto3.client('s3')
    
    bucket_name = 'bucket-ind-2023'
    #path_key = 'raw/llamadas/2023/202302/reporte_llamadas_20230201.csv'
    path_key  = f'raw/llamadas/{year}/{period}/'
    #path_key  = f'raw/llamadas/2023/202302/'    ##############
    
    result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_key )
    all_files = [key['Key'] for key in result['Contents']]
    
    dfs = []
    for file in all_files:
        #print("POINT")
        #print(file[-12:].replace('.','').replace('csv',''))
        file_now = file[-12:].replace('.csv','')
        if file_now == day_now:
            #print(file)
            resp = s3_client.get_object(Bucket=bucket_name, Key=file)
            dfs.append(pd.read_csv(resp['Body'], sep=';', dtype='unicode'))
    
    
    
    # concatenando todos los dataframe
    df = pd.concat(dfs, ignore_index=True)

     
    # limpiando los nombres de las columnas
    df.columns = [x.lower().replace(" ","_").replace("?","").replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u") \
                    .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
                    .replace(")","").replace(r"(","").replace("?","").replace("$","") for x in df.columns]
     
    # seleccion de columnas necesarias            
    df = df.loc[:, ['modo_de_llamada', 'dia', 'hora','agente','tipo_de_llamada','estado_llamada','fecha_completa_llamada','tiempo_en_cola_llamada', \
                    'tiempo_total_llamada','motivo','submotivo','tipo_de_emisor','tipo_de_cliente']]                 
    
    
    df['tipo_de_cliente'] =  df['tipo_de_cliente'].str[6:20]
    df['motivo'] = np.where( df['motivo']=='-', 'Consultas', df['motivo']) 
    df['submotivo'] = np.where( df['submotivo']=='-', 'Otras Consultas', df['submotivo']) 
    df['campania'] = df['modo_de_llamada'].str[4:7]
    df['rango_hora'] = df['hora'].str[0:2] +":"+  np.where( df['hora'].str[3:5].astype(int) >=30 , "30", "00" ) 
    df['hora_valida'] = np.where(  (df['hora'].str[0:2].astype(int) >=6)  &  (df['hora'].str[0:2].astype(int) <=23) , "si", "no" ) 
    df['direccion'] =  np.where(  df['modo_de_llamada'].str.contains('Entra', na=True) , "entrante", "saliente" ) 
    
    df['nivel_servicio'] = np.where( ( df['estado_llamada'].isin(['Atendida', 'Completada']) ) &  ( df['tiempo_en_cola_llamada'].str[0:2].astype(int)*3600 + df['tiempo_en_cola_llamada'].str[3:5].astype(int)*60 + df['tiempo_en_cola_llamada'].str[6:8].astype(int) < 45  ) , 'NS-Dentro',\
                           np.where( ( df['estado_llamada'].isin(['Atendida', 'Completada']) ) &  ( df['tiempo_en_cola_llamada'].str[0:2].astype(int)*3600 + df['tiempo_en_cola_llamada'].str[3:5].astype(int)*60 + df['tiempo_en_cola_llamada'].str[6:8].astype(int) >= 45 ) , 'NS-Fuera',\
                           'No Aplica' ))
                  
    df['aten_tmo'] = np.where( ( df['estado_llamada'].isin(['Atendida', 'Completada']) ) &  ( df['tiempo_total_llamada'].str[0:2].astype(int)*3600 + df['tiempo_total_llamada'].str[3:5].astype(int)*60 + df['tiempo_total_llamada'].str[6:8].astype(int) <= 251  ) , 'si', 'no' )
    df['wra_aten']  =  df['tiempo_total_llamada'].str[0:2].astype(int)*3600 + df['tiempo_total_llamada'].str[3:5].astype(int)*60 + df['tiempo_total_llamada'].str[6:8].astype(int)
    
    df['entrante'] = 1
    
    df['atendida'] = df.apply(func_atentida, axis=1)
    df['abandono'] = df.apply(func_abandono, axis=1)
    df['tmo'] =   ( df['tiempo_total_llamada'].str[0:2].astype(int)*3600 + df['tiempo_total_llamada'].str[3:5].astype(int)*60 + df['tiempo_total_llamada'].str[6:8].astype(int) )  - ( df['tiempo_en_cola_llamada'].str[0:2].astype(int)*3600 + df['tiempo_en_cola_llamada'].str[3:5].astype(int)*60 + df['tiempo_en_cola_llamada'].str[6:8].astype(int) )  
    df['tme'] =   df['tiempo_en_cola_llamada'].str[0:2].astype(int)*3600 + df['tiempo_en_cola_llamada'].str[3:5].astype(int)*60 + df['tiempo_en_cola_llamada'].str[6:8].astype(int)
    
    #validar fecha de hoy
    now = str(ct)[0:10]
    df = df.loc[df['dia'] == now ]
    
    df = df.loc[:, ['dia','rango_hora' ,'hora_valida', 'campania' ,'direccion','agente' ,'estado_llamada','motivo' ,'submotivo','tipo_de_emisor' ,'tipo_de_cliente' \
                         ,'nivel_servicio', 'wra_aten' ,'aten_tmo','entrante' , 'atendida','abandono' ,'tmo','tme']] 
    
    df.rename(columns = {'dia':'fecha'}, inplace = True)
    #print(df.head(5))
    
    for fecha in df.fecha.unique():
        df_final = df[df['fecha'] == fecha]
        fecha = fecha.replace('-','')
        year  = fecha[0:4]
        period = fecha[0:6]
        with io.StringIO() as csv_buffer:
            df_final.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket=bucket_name, Key=f"transformed/llamadas/{year}/{period}/reporte_llamadas_{fecha}.csv", Body=csv_buffer.getvalue() )
            #response = s3_client.put_object(Bucket=bucket_name, Key=f"transformed/llamadas/2023/202302/reporte_llamadas_{fecha}.csv", Body=csv_buffer.getvalue() )
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('proceso ejecutado correctamente')
        
    }