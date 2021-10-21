import boto3
from boto3.dynamodb.conditions import Key

def query_dynamo_db(search_string, table_name, key, dynamodb=None):
    '''Query dyname by primary key'''
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=Key(key).eq(search_string)
    )

    return response['Items']

def put_flight(*args, table_name, dynamodb=None):
    '''Put record to dynamo table'''
    #     print('ARGS:    ', args[0])
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb') 

    table = dynamodb.Table(table_name)
    response = table.put_item(
       Item=args[0]
    )

    return response

result = query_dynamo_db('the search key', 'table_name', 'Partition key name')

result = put_flight('json string having partition_key and sort key names', 'table_name')