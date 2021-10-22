import boto3

logs_client = boto3.client('logs')

'''
    List CloudWatch log groups starting with a given prefix and set retention to 90 days
'''
def set_retention(group):
    response = logs_client.put_retention_policy(
        logGroupName=group,
        retentionInDays=90
    )
    return response


response = logs_client.describe_log_groups(
    logGroupNamePrefix='/',
    # nextToken='string'
)

for l in response['logGroups']:
    group = l['logGroupName']
    retention = l['retentionInDays']
    print(retention, group)
    print(set_retention(group))