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
    
    