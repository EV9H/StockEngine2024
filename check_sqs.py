import boto3

def check_sqs_queue(queue_name):
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        print(f"Queue URL: {queue.url}")
        
        # Get queue attributes
        attributes = queue.attributes
        print(f"Number of messages available: {attributes.get('ApproximateNumberOfMessages', 'N/A')}")
        print(f"Number of messages not visible: {attributes.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}")
        
        # Try to receive a message
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5)
        if messages:
            print(f"Received a message. Message body: {messages[0].body[:100]}...")  # Print first 100 chars
        else:
            print("No messages received.")
        
    except Exception as e:
        print(f"Error checking queue {queue_name}: {str(e)}")

def main():
    queue_names = ["NVIDIA_BUY_SQS", "NVIDIA_SELL_SQS"]
    for queue_name in queue_names:
        print(f"\nChecking queue: {queue_name}")
        check_sqs_queue(queue_name)

if __name__ == "__main__":
    main()