
from urllib.parse import urlparse
import awswrangler as wr
import pandas as pd

s3_path = 's3://aws-athena-query-results-000290283155-eu-central-1/jupyter/28b31537-873c-4405-90d7-d065a4cbbfc9.csv'

bucket = urlparse(s3_path).netloc
filename = urlparse(s3_path).path.strip('/')

# s3_client = boto3.client('s3')
# s3_client.download_file(bucket, filename, 'flights.csv')

# print(bucket, filename)

# df = wr.s3.read_csv(path=s3_path, sep=',', na_values=['null', 'none'], skip_blank_lines=True)
wr.s3.download(path=s3_path, local_file='./key')
df = pd.read_csv('key')
df.to_parquet('flights.parquet', compression='gzip')