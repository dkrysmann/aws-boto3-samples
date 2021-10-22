import boto3
from urllib.parse import urlparse

'''
    Read s3 object content for example access s3 object content in lambda function
'''

uri = 's3://aws-athena-query-results-000290283155-eu-central-1/lambda/c5b49b66-c034-4830-a3a0-e5fac5dd4650.csv'
path = urlparse(uri)
print(path[2].strip('/'))

s3 = boto3.client('s3')
original = s3.get_object(
    Bucket=path[1],
    Key=path[2].strip('/'))
print(original['Body'].read().decode('utf-8'))