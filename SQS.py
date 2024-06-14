import boto3
from botocore.exceptions import ClientError


stocks = ["NVIDIA", "APPL"]


def create_sqs_buy(name="NVIDIA"):
    sqs = boto3.client('sqs')
    buy_queue_name = name + '_BUY_SQS'
    sell_queue_name = name + '_SELL_SQS'

    # Check if the buy queue exists
    try:
        buy_queue_url = sqs.get_queue_url(QueueName=buy_queue_name)
        print(f"Buy queue '{buy_queue_name}' already exists with URL: {buy_queue_url['QueueUrl']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Create the buy queue if it doesn't exist
            try:
                response = sqs.create_queue(QueueName=buy_queue_name)
                print(f"Created buy queue '{buy_queue_name}' with URL: {response['QueueUrl']}")
            except ClientError as e:
                print(e.response['Error']['Message'])
        else:
            print(e.response['Error']['Message'])

    # Check if the sell queue exists
    try:
        sell_queue_url = sqs.get_queue_url(QueueName=sell_queue_name)
        print(f"Sell queue '{sell_queue_name}' already exists with URL: {sell_queue_url['QueueUrl']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Create the sell queue if it doesn't exist
            try:
                response = sqs.create_queue(QueueName=sell_queue_name)
                print(f"Created sell queue '{sell_queue_name}' with URL: {response['QueueUrl']}")
            except ClientError as e:
                print(e.response['Error']['Message'])
        else:
            print(e.response['Error']['Message'])


# create_sqs_buy("APPL")
# create_sqs_buy("NVIDIA")


