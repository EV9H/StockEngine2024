# using AWS boto3, access the existing dynamoDB, get all the related information print out
import boto3
from botocore.exceptions import ClientError

def access_dynamodb(table_name):
    dynamoDB = boto3.client('dynamodb')
    
    try:
        response = dynamoDB.scan(TableName=table_name)
        items = response['Items']
        
        for item in items:
            print(item)
            
    except ClientError as e:
        print(e.response['Error']['Message'])


# Example usage
access_dynamodb('Stocks')
access_dynamodb('Users')

access_dynamodb('Orders')

