import boto3
import json
from botocore.exceptions import ClientError
import logging

SQS_URL = { 
    "APPL_SELL": "https://sqs.us-east-1.amazonaws.com/553509088460/APPL_SELL_SQS",
    "APPL_BUY": "https://sqs.us-east-1.amazonaws.com/553509088460/APPL_BUY_SQS",
    "NVIDIA_SELL": "https://sqs.us-east-1.amazonaws.com/553509088460/NVIDIA_SELL_SQS",
    "NVIDIA_BUY": "https://sqs.us-east-1.amazonaws.com/553509088460/NVIDIA_BUY_SQS"
}
logger = logging.getLogger(__name__)
sqs = boto3.resource("sqs")

# CONFIG

SQS_MAX_WAIT = 20

SQS_MAX_MSG_NUMBER = 5

# UTIL 
def get_queue( name):
        """
        Gets an SQS queue by name.

        :param name: The name that was used to create the queue.
        :return: A Queue object.
        """
        try:
            queue = sqs.get_queue_by_name(QueueName=name)
            print("Got queue '%s' with URL=%s", name, queue.url)
        except ClientError as error:
            print("Couldn't get queue named %s.", name)
            raise error
        else:
            return queue
def receive_messages(queue, max_number = SQS_MAX_MSG_NUMBER, wait_time = SQS_MAX_WAIT):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time,
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages

def delete_messages(queue, messages):
    """
    Delete a batch of messages from a queue in a single request.

    :param queue: The queue from which to delete the messages.
    :param messages: The list of messages to delete.
    :return: The response from SQS that contains the list of successful and failed
             message deletions.
    """
    try:
        entries = [
            {"Id": str(ind), "ReceiptHandle": msg.receipt_handle}
            for ind, msg in enumerate(messages)
        ]
        response = queue.delete_messages(Entries=entries)
        if "Successful" in response:
            for msg_meta in response["Successful"]:
                logger.info("Deleted %s", messages[int(msg_meta["Id"])].receipt_handle)
        if "Failed" in response:
            for msg_meta in response["Failed"]:
                logger.warning(
                    "Could not delete %s", messages[int(msg_meta["Id"])].receipt_handle
                )
    except ClientError:
        logger.exception("Couldn't delete messages from queue %s", queue)
    else:
        return response

# UUID = msg["UUID"]
# StockID = msg["StockID"] 
# UserID= msg["UserID"] 
# Mode = msg["Mode"] # Not needed 
# NumOfShares= msg["NumOfShares"] # int 
# Price = msg["Price"]

class MatchingEngine:
    def __init__(self, name):
        self.name = name
        self.buy_orders = []
        self.sell_orders = []
        self.buy_sqs = get_queue(name + "_BUY_SQS")
        self.sell_sqs = get_queue(name + "_SELL_SQS")


    def receive_buy_order(self):
        try:
            messages = receive_messages(self.buy_sqs)


            for message_raw in messages:
                print(f"Received message: %s: %s", message_raw.message_id) # 
                msg = json.loads(json.loads(message_raw.body)["Message"]) # dict
                self.buy_orders.append(msg)
                print(f"Received buy order: {msg['UUID']}")
            # delete message from SQS 
            delete_messages(self.buy_sqs, messages)
            print(self.buy_orders)
        except ClientError as error:
            print("Couldn't receive messages from queue: %s", queue)
            raise error

       
    def receive_sell_order(self):
        messages = self.receive_message(self.sell_sqs)
        for message_raw in messages:
            msg = json.loads(json.loads(message_raw.body)["Message"]) # dict
            self.sell_orders.append(msg)
            print(f"Received sell order: {msg['UUID']}")
            
    def send_partial(msg, mode):
        pass

    def send_full(msg):
        pass

print("initializing NVIDIA Matching Engine...")
NVIDIA_MATCHER = MatchingEngine("NVIDIA")

print("receiving buy order ... ")
NVIDIA_MATCHER.receive_buy_order()
print("receiving sell order ... ")

# NVIDIA_MATCHER.receive_sell_order()