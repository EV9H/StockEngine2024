import boto3
import json
import time
from decimal import Decimal
from botocore.exceptions import ClientError
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Configuration
REGION_NAME = 'us-east-1'
STREAM_NAME = 'StockTradeStream'
STOCK_PRICES_TABLE = 'StockPrices'

class RealtimeStockPriceProcessor:
    def __init__(self):
        self.kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)
        self.dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
        self.table = self.dynamodb.Table(STOCK_PRICES_TABLE)
        self.stock_prices = {}

    def get_shard_iterator(self):
        response = self.kinesis_client.describe_stream(StreamName=STREAM_NAME)
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        response = self.kinesis_client.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )
        return response['ShardIterator']

    def process_records(self, records):
        for record in records:
            try:
                data = json.loads(record['Data'].decode('utf-8'))
                self.process_trade(data)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON: {record['Data']}")
            except KeyError as e:
                logger.error(f"Missing key in record: {e}")
                logger.error(f"Record data: {data}")

    def process_trade(self, trade_data):
        stock_id = trade_data['StockID']
        trade_price = Decimal(trade_data['Price'])
        trade_shares = int(trade_data['NumOfShares'])

        if stock_id not in self.stock_prices:
            self.stock_prices[stock_id] = {
                'price': trade_price,
                'volume': trade_shares,
                'vwap': trade_price,
                'high': trade_price,
                'low': trade_price,
                'open': trade_price,
                'close': trade_price,
                'last_updated': datetime.now().isoformat()
            }
        else:
            current_data = self.stock_prices[stock_id]
            new_volume = current_data['volume'] + trade_shares
            new_vwap = ((current_data['vwap'] * current_data['volume']) + (trade_price * trade_shares)) / new_volume

            self.stock_prices[stock_id] = {
                'price': trade_price,
                'volume': new_volume,
                'vwap': new_vwap.quantize(Decimal('0.01')),
                'high': max(current_data['high'], trade_price),
                'low': min(current_data['low'], trade_price),
                'open': current_data['open'],
                'close': trade_price,
                'last_updated': datetime.now().isoformat()
            }

        self.save_stock_price(stock_id)
        logger.info(f"Processed trade for {stock_id}. Price: ${trade_price}, Volume: {trade_shares}")

    def save_stock_price(self, stock_id):
        try:
            timestamp = int(time.time() * 1000)  # current time in milliseconds
            item = {
                'StockID': stock_id,
                'Timestamp': timestamp,
                'LatestPrice': self.stock_prices[stock_id]['price'],
                'VWAP': self.stock_prices[stock_id]['vwap'],
                'Volume': self.stock_prices[stock_id]['volume'],
                'HighPrice': self.stock_prices[stock_id]['high'],
                'LowPrice': self.stock_prices[stock_id]['low'],
                'OpenPrice': self.stock_prices[stock_id]['open'],
                'ClosePrice': self.stock_prices[stock_id]['close'],
                'LastUpdated': self.stock_prices[stock_id]['last_updated']
            }
            self.table.put_item(Item=item)
            logger.info(f"Updated stock price for {stock_id} in DynamoDB")
        except ClientError as e:
            logger.error(f"Error saving stock price to DynamoDB: {e}")

    def run(self):
        shard_iterator = self.get_shard_iterator()
        
        while True:
            try:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                records = response['Records']
                if records:
                    logger.info(f"Processing {len(records)} records")
                    self.process_records(records)
                else:
                    logger.info("No new records found")
                
                shard_iterator = response['NextShardIterator']
                time.sleep(1)  # Wait for 1 second before next poll
            
            except ClientError as e:
                logger.error(f"Error getting records from Kinesis: {e}")
                time.sleep(5)  # Wait for 5 seconds before retrying

if __name__ == "__main__":
    processor = RealtimeStockPriceProcessor()
    logger.info("Starting Realtime Stock Price Processor")
    processor.run()