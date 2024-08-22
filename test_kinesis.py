import boto3
import json
import time
import random
import uuid
from datetime import datetime

STREAM_NAME = 'StockTradeStream'
REGION_NAME = 'us-east-1'

kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)

def send_test_orders():
    for _ in range(10):  # Send 10 test orders
        order = {
            "UUID": str(uuid.uuid4()),
            "StockID": "1",  # Using "1" as the StockID for NVIDIA
            "UserID": random.choice(["user1", "user2", "user3"]),
            "CreatedAt": datetime.now().isoformat(),
            "Mode": random.choice(["Sell", "Buy"]),
            "NumOfShares": random.randint(1, 50),
            "Price": str(round(random.uniform(50, 200), 2))
        }
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(order),
            PartitionKey=order['StockID']
        )
        print(f"Sent test order: {order}")
        time.sleep(1)  # Wait 1 second between sends

if __name__ == "__main__":
    send_test_orders()