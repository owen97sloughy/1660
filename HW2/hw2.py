#ofd2 py 1660

import boto3

s3 = boto3.resource('s3', 
    aws_access_key_id='',
    aws_secret_access_key=''
)

try:
    s3.create_bucket(Bucket='datacont-1660cloudslough', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")
    
bucket = s3.Bucket("datacont-1660cloudslough")
bucket.Acl().put(ACL='public-read')


#upload object into bucket
body = open('C:\\Users\\owen1\\OneDrive\\Documents\\cs1660\\HW2\\experiments.csv', 'rb')

o = s3.Object('datacont-1660cloudslough', 'test').put(Body=body)
s3.Object('datacont-1660cloudslough', 'test').Acl().put(ACL='public-read')


#create dynamodb
dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='',
    aws_secret_access_key=''
)

#set up table
try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

#0 because empty table
print(table.item_count)


import csv

#open experiments file to set up dynamoDB
with open('C:\\Users\\owen1\\OneDrive\\Documents\\cs1660\\HW2\\experiments.csv', 'rt', encoding='ascii') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        
        #try to open file and put the item into the table
        try:
            body = open('C:\\Users\\owen1\\OneDrive\\Documents\\cs1660\\HW2\\'+item[4], 'rb')
            s3.Object('datacont-1660cloudslough', item[4]).put(Body=body)
            md = s3.Object('datacont-1660cloudslough', item[4]).Acl().put(ACL='public-read')
            
            url = "https://s3-us-west-2.amazonaws.com/datacont-1660cloudslough/"+item[4]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'comment': item[3],
                            'date': item[2], 'url': url}

            table.put_item(Item=metadata_item)
        except:
            print("item may already exist or another failure")

#query table for key pair
response = table.get_item(
    Key={
        'PartitionKey': 'experiment1',
        'RowKey': 'data1'
    }
)

#spacing in console
print("\n\n\n")

item = response['Item']
print(item)

#spacing in console
print("\n\n\n")

print(response)