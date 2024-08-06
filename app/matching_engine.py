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
dynamodb = boto3.resource('dynamodb')
# CONFIG

SQS_MAX_WAIT = 2

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
def send_message(queue, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body, MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
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
            if len(messages) == 0:
                print("Received 0 buy order")
                return
            for message_raw in messages:
                # print(f"Received message: %s: %s", message_raw.message_id) # 
                msg = json.loads(json.loads(message_raw.body)["Message"]) # dict
                self.buy_orders.append(msg)
                print(f"Received buy order: {msg['UUID']}")
            # delete message from SQS 
            if len(messages) > 0:
                print("Deleting messages from Buy SQS...")
                delete_messages(self.buy_sqs, messages)
                # print(self.buy_orders)
        except ClientError as error:
            print("Couldn't receive messages from queue")
            raise error

       
    def receive_sell_order(self):
        try:
            messages = receive_messages(self.sell_sqs)
            if len(messages) == 0:
                print("Received 0 sell order")
                return

            for message_raw in messages:
                # print(f"Received message: %s: %s", message_raw.message_id) # 
                msg = json.loads(json.loads(message_raw.body)["Message"]) # dict
                self.sell_orders.append(msg)
                print(f"Received sell order: {msg['UUID']}")
            # delete message from SQS 
            if len(messages) > 0:
                print("Deleting messages from Sell SQS...")
                delete_messages(self.sell_sqs, messages)
                # print(self.sell_orders)
            
        except ClientError as error:
            print("Couldn't receive messages from queue")
            raise error
    def match_orders(self):
        
        while self.buy_orders and self.sell_orders: # while both queues have orders
            b = self.buy_orders.pop()
            s = self.sell_orders.pop()
            # if b["StockID"] == s["StockID"] and b["Price"] >= s["Price"]:
            print("Matching orders...", b['UUID'], b['NumOfShares'],"|",s['UUID'] , s['NumOfShares'])
            if b['NumOfShares'] > s['NumOfShares']: # sell fulfilled 
                print("Sell Order Fulfilled")

                shares_left = b['NumOfShares'] - s['NumOfShares']

                # send fulfilled to sell
                self.send_full(s)

                # send partial to buy 
                b['NumOfShares'] = shares_left
                self.send_partial(b, 'Buy')
            elif b['NumOfShares'] < s['NumOfShares']: # buy fulfilled
                print("Buy Order Fulfilled")
                shares_left = s['NumOfShares'] - b['NumOfShares']

                # send fulfilled to buy
                self.send_full(b)

                # send partial to sell
                s['NumOfShares'] = shares_left
                self.send_partial(s, 'Sell')
            else: # Both fulfilled
                print("Both Order Fulfilled")
                self.send_full(b)
                self.send_full(s)
        else:
            while self.buy_orders:
                b = self.buy_orders.pop()
                self.order_return(b, 'Buy')
            while self.sell_orders:
                s = self.sell_orders.pop()
                self.order_return(s, 'Sell')
            print("Not enough order to fulfill")
    def order_return(self, msg, mode):
        if mode == 'Sell':
            send_message(self.sell_sqs , json.dumps({
                'Message': json.dumps(msg)
            }))
        else:
            send_message(self.buy_sqs , json.dumps({
                'Message': json.dumps(msg)
            }))
    def send_partial(self, msg, mode):
        # send back to queue
        if mode == 'Sell': 
            send_message(self.sell_sqs , json.dumps({
                'Message': json.dumps(msg)
            }))

        else: 
            send_message(self.buy_sqs , json.dumps({
                'Message': json.dumps(msg)
            }))

        # update DB
        table = dynamodb.Table('Orders')
        try:
            response = table.update_item(Key = { "UUID": msg['UUID']} ,
                                        UpdateExpression="set NumOfShares=:N, #st=:S",
                                        ExpressionAttributeValues={":N": msg['NumOfShares'], ":S": "Partial"},
                                        ExpressionAttributeNames={
                                            "#st": "Status"             # workaround for conflicting reserved key word 
                                        },
                                        ReturnValues="UPDATED_NEW"   
                                    )
        except ClientError as err:
            print(
                "Couldn't update. Here's why:",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]
    def send_full(self, msg):
        table = dynamodb.Table('Orders')
        try:
            response = table.update_item(Key = { "UUID": msg['UUID']} ,
                                        UpdateExpression="set NumOfShares=:N, #st=:S",
                                        ExpressionAttributeValues={":N": 0, ":S": "Fulfilled"},
                                        ExpressionAttributeNames={
                                            "#st": "Status"             # workaround for conflicting reserved key word 
                                        },
                                        ReturnValues="UPDATED_NEW",    
                                    )
        except ClientError as err:
            print(
                "Couldn't update. Here's why:",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]
    def run(self, iteration = 10):
        i = 0 
        while(True): 
            
            print("*********** Iteration", i, "***********")
            print("Receiving Buy Orders ...")
            self.receive_buy_order()
            print("Receiving Sell Orders ...")
            self.receive_sell_order()
            self.match_orders()
            i += 1
            
# print("Generating Orders... ")
# batch_order_generator_NVIDIA(15)
print("*"*40)
print("initializing NVIDIA Matching Engine...")
NVIDIA_MATCHER = MatchingEngine("NVIDIA")

NVIDIA_MATCHER.run(iteration=15)
