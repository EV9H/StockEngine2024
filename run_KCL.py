import sys
import time
import logging
import traceback
from amazon_kclpy import kcl
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3
from kinesis_consumer import StockPriceConsumer

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_kinesis_stream(stream_name):
    try:
        kinesis_client = boto3.client('kinesis')
        response = kinesis_client.describe_stream(StreamName=stream_name)
        logger.info(f"Kinesis stream '{stream_name}' exists and is {response['StreamDescription']['StreamStatus']}")
        return True
    except ClientError as e:
        logger.error(f"Error checking Kinesis stream: {e}")
        return False

def check_dynamodb_table(table_name):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        table.load()
        logger.info(f"DynamoDB table '{table_name}' exists")
        return True
    except ClientError as e:
        logger.error(f"Error checking DynamoDB table: {e}")
        return False

def run_kcl():
    stream_name = "StockTradeStream"
    table_name = "Orders"
    
    if not check_kinesis_stream(stream_name) or not check_dynamodb_table(table_name):
        logger.error("Failed to verify Kinesis stream or DynamoDB table. Exiting.")
        return

    config = Config(
        read_timeout=70,
        connect_timeout=70,
        retries={"max_attempts": 10}
    )

    try:
        logger.info("Initializing KCL worker")
        worker = kcl.KCLProcess(StockPriceConsumer())
        logger.info("Starting KCL worker")
        worker.run()
    except Exception as e:
        logger.error(f"Error in KCL process: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    logger.info("Starting main process")
    run_kcl()
    logger.info("Main process ended")

    # Keep the script running
    try:
        while True:
            logger.info("KCL process is still running...")
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Script is exiting.")