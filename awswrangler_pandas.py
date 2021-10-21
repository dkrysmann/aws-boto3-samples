
from urllib.parse import urlparse
import awswrangler as wr
import pandas as pd

'''
    Example download data from S3 URI in to Pandas dataframe
'''

s3_path = 's3://aws-athena-query-results-eu-central-1/jupyter/28b31537-873c-4405-90d7-d065a4cbbfc9.csv'

bucket = urlparse(s3_path).netloc
filename = urlparse(s3_path).path.strip('/')

wr.s3.download(path=s3_path, local_file='./key')
df = pd.read_csv('key')
df.to_parquet('flights.parquet', compression='gzip')