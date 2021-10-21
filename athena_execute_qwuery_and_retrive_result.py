import boto3
from urllib.parse import urlparse
import time
import pandas as pd

session = boto3.Session()


def athena_query_auto(params):
    client = session.client('athena', region_name=params["region"])
    response1 = client.start_query_execution(
        QueryString=params['query'],
        WorkGroup=params['workgroup'],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': params['OutputLocation'],
        },
    )

    max_execution = 10 ### Retrys waiting for result
    state = 'RUNNING'
    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = response1['QueryExecutionId'])
        
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            print(state)
            if state == 'FAILED':
                return response
            elif state == 'SUCCEEDED':
                # print(rsponse)
                return response
            time.sleep(1)

        return response

params = {
    'region': 'eu-central-1',
    'database': 'default',
    'workgroup': 'primary',
    'OutputLocation': 's3://aws-athena-query-results-000290283155-eu-central-1/jupyter/',
    'path': 'jupyter/envy/',
    'query': 'SELECT * FROM "default"."flights" limit 10;'
}


result = athena_query_auto(params)

print(result)
s3_path = result['QueryExecution']['ResultConfiguration']['OutputLocation']
print(s3_path)
bucket = urlparse(s3_path).netloc
filename = urlparse(s3_path).path.strip("/")


s3_client = boto3.client('s3')
s3_client.download_file(bucket, filename, 'flights.csv')

### Optional print query result
print((open('flights.csv').read()))
### Create Pandas dataframe from result
df = pd.read_csv('flights.csv')
print(df)