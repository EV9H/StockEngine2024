import boto3
from botocore.exceptions import ClientError

# Initialize boto3 clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

# SQS queue URLs
SQS_URLS = {
    "NVIDIA_BUY_SQS": "https://sqs.us-east-1.amazonaws.com/553509088460/NVIDIA_BUY_SQS",
    "NVIDIA_SELL_SQS": "https://sqs.us-east-1.amazonaws.com/553509088460/NVIDIA_SELL_SQS"
}

# DynamoDB table name
ORDERS_TABLE_NAME = 'Orders'

def purge_sqs_queue(queue_url):
    """
    Purges all messages from an SQS queue.
    """
    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print(f"Successfully purged queue: {queue_url}")
    except ClientError as e:
        print(f"Error purging queue {queue_url}: {e}")

def clear_dynamodb_table(table_name):
    """
    Clears all items from a DynamoDB table.
    """
    table = dynamodb.Table(table_name)
    
    try:
        # Scan the table
        response = table.scan()
        
        with table.batch_writer() as batch:
            for item in response['Items']:
                batch.delete_item(Key={
                    'UUID': item['UUID']
                })
        
        print(f"Successfully cleared table: {table_name}")
    except ClientError as e:
        print(f"Error clearing table {table_name}: {e}")

def main():
    # Purge SQS queues
    for queue_name, queue_url in SQS_URLS.items():
        purge_sqs_queue(queue_url)
    
    # Clear DynamoDB table
    clear_dynamodb_table(ORDERS_TABLE_NAME)

if __name__ == "__main__":
    main()