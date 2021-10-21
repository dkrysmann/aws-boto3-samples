import boto3

glue_client = boto3.client('glue', region_name='eu-west-1')

response = glue_client.get_tables(
    # CatalogId='string',
    DatabaseName='piaware',
    # Expression='string',
    # NextToken='string',
    MaxResults=200
)

results = response["TableList"]
while "NextToken" in response:
    response = glue_client.get_tables(NextToken=response["NextToken"], DatabaseName='piaware')
    results.extend(response["TableList"])
    

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