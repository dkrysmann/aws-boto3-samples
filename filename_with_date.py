from datetime import datetime
import os
import boto3
import time

def upload_delete():
    '''Create S3 path based on date, upload to S3 and delete local parquet
        Resultoing in S3 path like s3://piaware/temp/2021/10/21/16/29_flights.parq
    '''
    print('Upload parquet to S3 and delete local version')
    strings = time.strftime("%Y,%m,%d,%H,%M")
    t = strings.split(',')
    s = "/"
    path = s.join(t)
    file = '/tmp/2029240f6d1128be89ddc32729463129'
    s3name = 'flights.parq'
    prefix = "temp/" + path + "_" + s3name
    print(prefix)
    bucket_name = 'piaware'
    s3 = boto3.client('s3')
    s3.upload_file(file, bucket_name, prefix)
    # os.remove(file)
    # os.rename('/tmp/flights.parq', '/tmp/' + strings.replace(',','') + '.parquet')

def file_with_date():
    '''
        Simple function adding datetime suffix to a file
    '''
    strings = time.strftime("%Y,%m,%d,%H,%M")
    t = strings.split(',')
    s = ""
    path = s.join(t)
    filename = '/tmp/flights'
    prefix = f"{filename}_{path}.parquet"
    print(prefix)

# upload_delete()
# file_with_date()