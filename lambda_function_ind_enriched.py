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
