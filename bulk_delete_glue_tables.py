import boto3

'''Set client and region'''
glue_client = boto3.client('glue', region_name='eu-west-1')

'''Standard boto3 documentation description'''
# response = glue_client.get_tables(
#     CatalogId='string',
#     DatabaseName='piaware',
#     Expression='string',
#     NextToken='string',
#     MaxResults=100
# )

'''Using NextToken in boto3 output to create list of tables in database piaware'''
results = response["TableList"]
while "NextToken" in response:
    response = glue_client.get_tables(
        NextToken=response["NextToken"], 
        DatabaseName='piaware'
        )
    results.extend(response["TableList"])
    

'''Delete all tables having 'df_parquet_gzip' in name'''
i = 1
cnt = len(results)
for tbl in results:
    table = tbl["Name"]

    if 'df_parquet_gzip' in table:
        print(i, cnt, "Deleting ", table)
        response = glue_client.delete_table(
            # CatalogId='string',
            DatabaseName='piaware',
            Name=table
        )
        i += 1