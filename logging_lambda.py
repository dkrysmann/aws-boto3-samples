import pprint
import datetime
from dateutil.tz import tzlocal

import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

''' Two examples: 
        - How to use pretty print ins python
        - How to log events to cloudwatch
    Note: the datetime and dateutil helps in interpretting boto3 API response
'''

pp = pprint.PrettyPrinter(indent=4)
p = {'QueryExecution': {'QueryExecutionId': 'a97001d9-4d08-4c32-ba16-620e8ab499f3', 'Query': 'drop table if exists flights_table', 'StatementType': 'DDL', 'ResultConfiguration': {'OutputLocation': 's3://aws-athena-query-results/lambda/a97001d9-4d08-4c32-ba16-620e8ab499f3.txt'}, 'QueryExecutionContext': {'Database': 'default'}, 'Status': {'State': 'FAILED', 'StateChangeReason': 'FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:User: arn:aws:sts::000290283155:assumed-role/create_flights_table-role-adhlbkba/create_flights_table is not authorized to perform: glue:DeletePartition on resource: arn:aws:glue:eu-central-1:000290283155:catalog (Service: AmazonDataCatalog; Status Code: 400; Error Code: AccessDeniedException; Request ID: 76d5177a-05ff-4572-ae34-3d1e3975554b; Proxy: null))', 'SubmissionDateTime': datetime.datetime(2021, 10, 22, 10, 4, 29, 475000, tzinfo=tzlocal()), 'CompletionDateTime': datetime.datetime(2021, 10, 22, 10, 4, 33, 736000, tzinfo=tzlocal())}, 'Statistics': {'EngineExecutionTimeInMillis': 3677, 'DataScannedInBytes': 0, 'TotalExecutionTimeInMillis': 4261, 'QueryQueueTimeInMillis': 574, 'ServiceProcessingTimeInMillis': 10}, 'WorkGroup': 'primary', 'EngineVersion': {'SelectedEngineVersion': 'AUTO', 'EffectiveEngineVersion': 'Athena engine version 2'}}, 'ResponseMetadata': {'RequestId': '3226a306-826c-472b-92ac-1a8537280816', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Fri, 22 Oct 2021 10:04:35 GMT', 'x-amzn-requestid': '3226a306-826c-472b-92ac-1a8537280816', 'content-length': '2408', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}

pp.pprint(p)
logger.info(f"Status element: {p['QueryExecution']['Status']}")