# Data Pipeline AWS Lambda

Este proyecto es la entrega final del curso de Ingeneria de Datos en AWS. Para el cual se utilizan diferentes servicios de la nube de AWS.

## Arquitectura

[![arquitectura.png](https://i.postimg.cc/DfrBSXDn/arquitectura.png)](https://postimg.cc/t7ghfYpM)

## Escenario

La empresa AZT necesita analizar las llamadas que realizan sus clientes. Para ello se creo una solución en la nube de AWS.

## Tecnologias utilizadas

- Python, VSCODE, AWS CLI, EC2, Boto3, S3, Athena, Lambda, AWS RDS PostgreSQL, CloudWacht, IAM, VPC, Power BI.

## Proceso

1.- Se crean los servicios, credenciales y roles necesarios en AWS:
- IAM, VPC, CloudWacht
- EC2: Instancia de desarrollo
- S3: Datalake (raw, transformed, enriched).
- Athena: Exploración de los archivos.
- Lambda: Procesamiento de los archivos.
- AWS RDS: Almacenamiento
- Power BI: Indicadores

2- Las llamadas se extraen de un CRM el cual exporta los registros en un archivo .csv. Para ello se realiza un proceso de Web Scraping.

3.- En una instacia de EC2 se instala AWS CLI con las credenciales necesarias para conectarse con el servicio de AWS. Luego con la API Boto3 se suben los archivos dinamicamente por periodo (año/mes) al Datalake (zona raw) creado en S3. 

[![instancia-dev-EC2.png](https://i.postimg.cc/rpq8LWvs/instancia-dev-EC2.png)](https://postimg.cc/LJ0dtq4F)

La instancia EC2 se conecta con VSCODE para realizar el script para subir los archivos al servicio de S3.

[![vscode-dev.png](https://i.postimg.cc/YqMsp3d1/vscode-dev.png)](https://postimg.cc/G9gQKkw2)

4.- Los archivos se reciben en S3 en la zona raw, para luego ser distribuida en las diferentes zonas (transformed, enriched). Se crean 3 notificaciones de eventos (Put) para cada zona el cual disparan en los Lambda creados.

#### Bucket
[![4-bucket.png](https://i.postimg.cc/ncVGScVJ/4-bucket.png)](https://postimg.cc/mhq9t4FX)
#### Zonas
[![5-bucket-zonas.png](https://i.postimg.cc/WpxXzPsL/5-bucket-zonas.png)](https://postimg.cc/23F49Mq2)
#### Llamadass por año
[![6-bucket-zonas-a-o.png](https://i.postimg.cc/RhyzNpXC/6-bucket-zonas-a-o.png)](https://postimg.cc/JHNFFqfS)
#### Llamadas por periodo
[![7-bucket-zonas-periodos.png](https://i.postimg.cc/KYSBnZRB/7-bucket-zonas-periodos.png)](https://postimg.cc/jDcDTY6S)
#### Llamadas por archivo
[![8-bucket-zonas-archivos.png](https://i.postimg.cc/3wT93jjs/8-bucket-zonas-archivos.png)](https://postimg.cc/0zV7fwBn)
#### S3 eventos por zonas (raw, transformed y enriched)
[![9-S3-eventos-put-trigger-lambda.png](https://i.postimg.cc/6pJpczN4/9-S3-eventos-put-trigger-lambda.png)](https://postimg.cc/LnDS6t5m)

5.- Se realiza una exploración de los datos con el servicio de Athena.
[![15-athena.png](https://i.postimg.cc/0yFx3w4B/15-athena.png)](https://postimg.cc/PCWBvPTW)

6.- El el servicio de Lambda se crean 3 funciones :

- lambda_ind_transformed: Lee el directorio de la zona 'raw' y se queda con el archivo que tiene la fecha actual, realiza las transformaciónes necesarias y lo envia a la zona 'transformed'
[![11-1-Lambda-ind-transformed.png](https://i.postimg.cc/50hDWKGQ/11-1-Lambda-ind-transformed.png)](https://postimg.cc/S2GgWdmm)

- lambda_ind_enriched: Lee el directorio de la zona 'transformed' y se queda con el archivo que tiene la fecha actual, realiza las agregaciones necesarias y lo envia a la zona 'enriched'.
[![11-2-Lambda-ind-enriched.png](https://i.postimg.cc/HkTN4yX5/11-2-Lambda-ind-enriched.png)](https://postimg.cc/RNbGvqPV)


- lambada_ind_load_postgreSQL: Lee el directorio de la zona 'enriched' y se queda con el archivo que tiene la fecha actual y procede a cargar el archivo al servicio de AWS RDS PostgreSQL. 
[![11-3-Lambda-ind-load-postgre-SQL.png](https://i.postimg.cc/W30Kw6WG/11-3-Lambda-ind-load-postgre-SQL.png)](https://postimg.cc/625Y9RNq)


[![10-Lambda.png](https://i.postimg.cc/vH5nL46h/10-Lambda.png)](https://postimg.cc/nCcMnc29)

#### Nota: Se crean un Layer personalizado para importar las librerias necesarias.
[![10-Lambda-layers.png](https://i.postimg.cc/R067n4wx/10-Lambda-layers.png)](https://postimg.cc/rdT0B6hf)
| Custom Layer | https://drive.google.com/file/d/1Z3u9h3V26_XgeZqnPJooixsaT6pt9XE9/view?usp=sharing |
| ----------------- | ------------------------------------------------------------------ |




7.- Se crea un servicio de PostgreSQL para almacenar la información procesada para su posterior analisis. 

[![12-RDS-Postgre-SQL.png](https://i.postimg.cc/J0NFxdSM/12-RDS-Postgre-SQL.png)](https://postimg.cc/hQGMPMS5)

#### Validación de información con DBeaver
[![13-rdb-dbeaver.png](https://i.postimg.cc/Hn5Xqqt8/13-rdb-dbeaver.png)](https://postimg.cc/p5Wm814P)

8.- Se crea un dashboard en Power BI para crear indicadores y para el equipo del negocio tome las desiciones necesarias.
[![14-BI.png](https://i.postimg.cc/K8P92RDQ/14-BI.png)](https://postimg.cc/nXLKKV5Q)

## Codigo

#### upload_files.py
```python
import logging
import boto3
from botocore.exceptions import ClientError
from decouple import config 
import glob
import re
import sys

def upload_file(path_file: str, bucket: str, key: str):
    try:
        client = boto3.client('s3')
        client.upload_file(path_file, bucket, key)
    except ClientError as e:
        logging.error(e)
        sys.exit(-1)
        

def main(files_local: str, bucket:str, key_bucket:str):
    try:
        i=1
        for file in files_local:
            file_split = file.split('/')
            date = str(re.findall('[0-9]+', file)).replace('[','').replace(']','').replace("'","")
            year   = date[0:4]
            period = date[0:6]
            file_name = file_split[-1]

            upload_file(file, bucket, f'{key_bucket}/{year}/{period}/{file_name}')
            
            print(f'File upload, {file_name} -> {i}/{len(files)}')
            i+=1 
    except Exception as e:
        print(f'failed to load: {e}')

          
if __name__=='__main__':
    BUCKET_NAME = config('BUCKET_NAME')
    key = 'raw/llamadas'
    
    path_file_local = '/home/ubuntu/project/data/calls_periodo'
    files = glob.glob(f'{path_file_local}/*.csv') 

    main(files_local=files, bucket=BUCKET_NAME, key_bucket=key)
```

#### lambda_ind_transformed
```python
import pandas as pd
import json
#import psycopg2
from datetime import date,datetime
#import os
#import csv
#import glob
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

```

#### Lambda_ind_enriched
```python
from datetime import date,datetime
import json
import pandas as pd
import numpy as np
import boto3
import io
import pytz

#pd.options.display.max_columns = None

def lambda_handler(event, context):
    # TODO implement
    tz = pytz.timezone('America/Bogota')
    ct = datetime.now(tz=tz)
    year = str(ct).replace('-','')[0:4]
    period = str(ct).replace('-','')[0:6]
    day = str(ct).replace('-','')[6:8]
    month = str(ct).replace('-','')[4:6]
    day_now= year+month+day
    print(ct)
    #print(year, period )

    s3_client = boto3.client('s3')
    
    bucket_name = 'bucket-ind-2023'
    #path_key = 'transformed/llamadas/2023/202302/reporte_llamadas_20230201.csv'
    path_key  = f'transformed/llamadas/{year}/{period}/'
    #path_key  = f'transformed/llamadas/2023/202302/'
    
    result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_key )
    all_files = [key['Key'] for key in result['Contents']]
    
    """
    for file in all_files:
        print(file)
        break
    """
    
    dfs = []
    for file in all_files:
        file_now = file[-12:].replace('.csv','')
        if file_now == day_now:
            #print(file)
            resp = s3_client.get_object(Bucket=bucket_name, Key=file)
            dfs.append(pd.read_csv(resp['Body'], sep=',' ))
        
    # concatenando todos los dataframe
    df = pd.concat(dfs, ignore_index=True)
    #print(df.head(5))
    #print(df.dtypes)
    #print(df.shape)
    
    # filtar fecha de 
    now = str(ct)[0:10]
    df = df.loc[df['fecha'] == now ]
    
    # aggreacion
    df_agg = df.groupby(['fecha','rango_hora','hora_valida','campania','direccion','agente','estado_llamada','motivo','submotivo','tipo_de_emisor', \
                         'tipo_de_cliente','nivel_servicio','aten_tmo'], as_index=False)['wra_aten','entrante','atendida','abandono','tmo','tme'].agg(sum)
    #print(df_agg.head(5))
    #print(df_agg.shape)    
    
    for fecha in df_agg.fecha.unique():
        df_final = df_agg[df_agg['fecha'] == fecha]
        fecha = fecha.replace('-','')
        year   = fecha[0:4]
        period = fecha[0:6]
        
        with io.StringIO() as csv_buffer:
            df_final.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket=bucket_name, Key=f"enriched/llamadas/{year}/{period}/reporte_llamadas_{fecha}.csv", Body=csv_buffer.getvalue() )
            #response = s3_client.put_object(Bucket=bucket_name, Key=f"enriched/llamadas/2023/202302/reporte_llamadas_{fecha}.csv", Body=csv_buffer.getvalue() )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
```

#### lambada_ind_load_postgreSQL
```py
import json
import psycopg2
import pytz
import boto3
import csv
import pandas as pd
from datetime import date,datetime

def lambda_handler(event, context):
    # TODO implement
    
    tz = pytz.timezone('America/Bogota')
    ct = datetime.now(tz=tz)
    year = str(ct).replace('-','')[0:4]
    day = str(ct).replace('-','')[6:8]
    month = str(ct).replace('-','')[4:6]
    period = str(ct).replace('-','')[0:6]
    day_now= year+month+day
    
    s3_client = boto3.client('s3')
    bucket = 'bucket-ind-2023'
    
    host = 'db-ind.ccothdrjgv9f.us-east-1.rds.amazonaws.com'
    user = 'postgres'
    password = 'postgres'
    dbname = 'postgres'
    port = '5432'
  
    db = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
    cursor = db.cursor()
    
    key= 'enriched/llamadas/2022/202212/'
    bucket_name = 'bucket-ind-2023'
    #path_key = 'enriched/llamadas/2023/202302/reporte_llamadas_20230201.csv'
    path_key  = f'enriched/llamadas/{year}/{period}/'
    #path_key  = f'enriched/llamadas/2023/202302/'
    
    result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_key )
    all_files = [key['Key'] for key in result['Contents']]
    
    dfs = []
    for file in all_files:
        #print(file)
        file_now = file[-12:].replace('.csv','')
        if file_now == day_now:
            resp = s3_client.get_object(Bucket=bucket_name, Key=file)
            dfs.append(pd.read_csv(resp['Body'], sep=','))
    
    #, dtype='unicode'
    
    # concatenando todos los dataframe
    df = pd.concat(dfs, ignore_index=True)
    now = str(ct)[0:10]
    df = df.loc[df['fecha'] == now ]
    
    print(df.shape)
    
    # Eliminar los registros de hoy PostgreSQL
    sql_delete = "DELETE FROM public.llamadas WHERE fecha = CURRENT_DATE;"  
    cursor.execute(sql_delete)
    db.commit()
      
    # Insertando registros  
    sql = "INSERT INTO public.llamadas VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    
    for i, row in df.iterrows():
        #print(tuple(row))
        cursor.execute(sql,tuple(row))
    
    db.commit()
    db.close()
  
    """
    con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
    
    cur = con.cursor()
    query = "select * from public.llamadas;"
    cur.execute(query) 
    records = cur.fetchall()
    print(records)
    # close cursor and con
    cur.close()
    con.close()
    """
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

```


## Contacto
ztejorge@hotmail.com
