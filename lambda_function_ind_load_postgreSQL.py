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
