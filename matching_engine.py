import boto3
import json
from botocore.exceptions import ClientError
import logging
from OrdersAPI import batch_order_generator, batch_order_generator_NVIDIA
import time
import uuid
from datetime import datetime
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

from decimal import Decimal

import json
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
class MatchingEngine:
    def __init__(self, name):
        self.name = name
        self.buy_orders = []
        self.sell_orders = []
        self.buy_sqs = get_queue(name + "_BUY_SQS")
        self.sell_sqs = get_queue(name + "_SELL_SQS")
        self.kinesis_client = boto3.client('kinesis')
        self.kinesis_stream_name = f"StockTradeStream"
    def send_trade_to_kinesis(self, trade_data):
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.kinesis_stream_name,
                Data=json.dumps(trade_data),
                PartitionKey=trade_data['StockID']
            )
            print(f"Sent trade data to Kinesis: {trade_data['UUID'][:8]}")
        except ClientError as e:
            print(f"Error sending trade data to Kinesis: {e}")
    def receive_buy_order(self):
        messages = receive_messages(self.buy_sqs)
        self.buy_orders.extend([self.parse_order(json.loads(json.loads(m.body)["Message"])) for m in messages])
        if messages:
            delete_messages(self.buy_sqs, messages)
        print(f"Received {len(messages)} buy order(s)")

    def receive_sell_order(self):
        messages = receive_messages(self.sell_sqs)
        self.sell_orders.extend([self.parse_order(json.loads(json.loads(m.body)["Message"])) for m in messages])
        if messages:
            delete_messages(self.sell_sqs, messages)
        print(f"Received {len(messages)} sell order(s)")

    def parse_order(self, order):
        order['Price'] = Decimal(order['Price'])
        order['NumOfShares'] = int(order['NumOfShares'])
        return order

    def match_orders(self):
        self.buy_orders.sort(key=lambda x: (x['Price'], x['CreatedAt']), reverse=True)
        self.sell_orders.sort(key=lambda x: (x['Price'], x['CreatedAt']))
        
        matches = 0
        while self.buy_orders and self.sell_orders:
            buy = self.buy_orders[0]
            sell = self.sell_orders[0]
            buy['Price'] = Decimal(str(buy['Price']))  # Convert string back to Decimal
            sell['Price'] = Decimal(str(sell['Price']))  # Convert string back to Decimal
            if buy['Price'] >= sell['Price']:
                trade_price = (buy['Price'] + sell['Price']) / Decimal('2')
                trade_shares = min(buy['NumOfShares'], sell['NumOfShares'])
                
                print(f"\nMatch found:")
                print(f"  Buy:  {buy['UUID'][:8]} - {buy['NumOfShares']} shares @ ${buy['Price']:.2f}")
                print(f"  Sell: {sell['UUID'][:8]} - {sell['NumOfShares']} shares @ ${sell['Price']:.2f}")
                print(f"  Trade: {trade_shares} shares @ ${trade_price:.2f}")
                
                if buy['NumOfShares'] > sell['NumOfShares']:
                    self.send_full(sell, str(trade_price))
                    buy['NumOfShares'] -= trade_shares
                    self.send_partial(buy, 'Buy', str(trade_price))
                    self.sell_orders.pop(0)
                    print(f"  Result: Sell order fulfilled, Buy order partially filled ({buy['NumOfShares']} shares remaining)")
                elif buy['NumOfShares'] < sell['NumOfShares']:
                    self.send_full(buy, str(trade_price))
                    sell['NumOfShares'] -= trade_shares
                    self.send_partial(sell, 'Sell', str(trade_price))
                    self.buy_orders.pop(0)
                    print(f"  Result: Buy order fulfilled, Sell order partially filled ({sell['NumOfShares']} shares remaining)")
                else:
                    self.send_full(buy, str(trade_price))
                    self.send_full(sell, str(trade_price))
                    self.buy_orders.pop(0)
                    self.sell_orders.pop(0)
                    print("  Result: Both orders fully matched and fulfilled")
                
                matches += 1
                trade_data = {
                    'UUID': str(uuid.uuid4()),  # Generate a new UUID for the trade
                    'BuyOrderID': buy['UUID'],
                    'SellOrderID': sell['UUID'],
                    'StockID': "1",  # Assuming both buy and sell orders have the same StockID
                    'UserID': buy['UserID'],
                    'SellerID': sell['UserID'],
                    'NumOfShares': trade_shares,
                    'Price': str(trade_price.quantize(Decimal('0.01'))),  # Round to 2 decimal places
                    'Timestamp': datetime.now().isoformat(),
                    'Type': 'Trade'
                }
                self.send_trade_to_kinesis(trade_data)
            else:
                break
        
        print(f"\nMatching complete. {matches} matches made.")
        print(f"Remaining: {len(self.buy_orders)} buy orders, {len(self.sell_orders)} sell orders")
        
        for order in self.buy_orders:
            self.order_return(order, 'Buy')
        for order in self.sell_orders:
            self.order_return(order, 'Sell')
        self.buy_orders.clear()
        self.sell_orders.clear()

    def order_return(self, msg, mode):
        queue = self.sell_sqs if mode == 'Sell' else self.buy_sqs
        msg['Price'] = str(msg['Price'])  # Convert Decimal back to string
        send_message(queue, json.dumps({'Message': json.dumps(msg)}))
        # print(f"Returned unmatched {mode} order: {msg['UUID'][:8]} - {msg['NumOfShares']} shares @ ${msg['Price']}")

    def send_partial(self, msg, mode, trade_price):
        msg['Price'] = str(msg['Price'])  # Convert Decimal back to string
        self.order_return(msg, mode)
        self.update_order(msg['UUID'], msg['NumOfShares'], "Partial", trade_price)
        print(f"Updated partial {mode} order: {msg['UUID'][:8]} - {msg['NumOfShares']} shares remaining")

    def send_full(self, msg, trade_price):
        self.update_order(msg['UUID'], 0, "Fulfilled", trade_price)
        print(f"Fulfilled order: {msg['UUID'][:8]} - {msg['NumOfShares']} shares @ ${msg['Price']}")

    def update_order(self, uuid, shares, status, trade_price):
        table = dynamodb.Table('Orders')
        try:
            response = table.update_item(
                Key={"UUID": uuid},
                UpdateExpression="set NumOfShares=:N, #st=:S, TradePrice=:P",
                ExpressionAttributeValues={":N": shares, ":S": status, ":P": trade_price},
                ExpressionAttributeNames={"#st": "Status"},
                ReturnValues="ALL_NEW"
            )
            print(f"Updated order in DynamoDB: {uuid[:8]} - Status: {status}, Shares: {shares}, TradePrice: {trade_price}")
        except ClientError as err:
            print(f"Couldn't update order {uuid}. Error: {err.response['Error']['Message']}")
            raise

    def run(self, iteration=10):
        for i in range(iteration):
            print(f"\n{'=' * 20} Iteration {i+1} {'=' * 20}")
            print("Receiving Buy Orders ...")
            self.receive_buy_order()
            print("Receiving Sell Orders ...")
            self.receive_sell_order()
            self.match_orders()
            time.sleep(0.5)
# print("Generating Orders... ")
# batch_order_generator_NVIDIA(15)
print("*"*40)
print("initializing NVIDIA Matching Engine...")
NVIDIA_MATCHER = MatchingEngine("NVIDIA")

NVIDIA_MATCHER.run(iteration=100)
