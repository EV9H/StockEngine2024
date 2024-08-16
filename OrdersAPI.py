import boto3 
import requests
import json     
client = boto3.client('apigateway')
import uuid
import random
import datetime
import time 
restApiId='zuiz4od611'

# CALL API GATEWAY OF THIS API: arn:aws:execute-api:us-east-1:553509088460:zuiz4od611/*/GET/users/{user_id}
# supply primary key: "user1"



def get_user(user_id):
    api_gateway_url = "https://zuiz4od611.execute-api.us-east-1.amazonaws.com/test/users/{user_id}"
    url = api_gateway_url.format(user_id=user_id)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print(f"Request failed with status code: {response.status_code}")


# def create_order(order):
#     api_gateway_url = "https://zuiz4od611.execute-api.us-east-1.amazonaws.com/test/orders"
#     response = requests.post(api_gateway_url, json=order)
#     if response.status_code == 201:
#         data = response.json()
#         print("[Order Created]:", order['StockID'], order['UserID'],order['Mode'], order['NumOfShares'], order['UUID'])
#         print(data)
#         return data['message']
#     else:
#         print(f"Request failed with status code: {response.status_code}")
#         print(response.json())
#         return None
api_gateway_url = "https://zuiz4od611.execute-api.us-east-1.amazonaws.com/test/orders"
    
def create_order(order):
    try:
        response = requests.post(api_gateway_url, json=order)
        response.raise_for_status()
        print(f"Order created successfully: {order['UUID']}")
        return response.json()
    except requests.RequestException as e:
        print(f"Error creating order: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status code: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        print(f"Request payload: {json.dumps(order, indent=2)}")
        return None
from decimal import Decimal
def batch_order_generator(num):
    for i in range(num):
        order = {
            "UUID": str(uuid.uuid4()),
            "StockID": str(random.choice([1,2])),
            "UserID": random.choice(["user1","user2", "user3"]),
            "CreatedAt": str(datetime.datetime.now()),
            "Mode": random.choice(["Sell","Buy"]),
            "NumOfShares": random.randint(1,50),
            "Price": Decimal(str(round(random.uniform(50, 200), 2)))  # Generate a random price between 50 and 200
        }
        create_order(order)


def batch_order_generator_NVIDIA(num):
    for i in range(num):
        order = {
            "UUID": str(uuid.uuid4()),
            "StockID": "1",
            "UserID": random.choice(["user1", "user2", "user3"]),
            "CreatedAt": str(datetime.datetime.now()),
            "Mode": random.choice(["Sell", "Buy"]),
            "NumOfShares": random.randint(1, 50),
            "Price": str(round(random.uniform(50, 200), 2))
        }
        result = create_order(order)
        if result is None:
            print(f"Stopping order generation due to error on order {i+1}")
            break
order130 = {
  
  
    "UUID": "order136",
    "StockID": "2",
    "UserID": "user2",
    "CreatedAt": "2024-02-12T10:30:00Z",
    "Mode": "Sell",
    "NumOfShares": 2,
    "Price": 120.1
  
 
}
# create_order(order130)

def cancel_order(order_id):
    pass
def get_order(order_id):
    api_gateway_url = "https://zuiz4od611.execute-api.us-east-1.amazonaws.com/test/orders/{order_id}"
    url = api_gateway_url.format(order_id=order_id)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print(f"Request failed with status code: {response.status_code}")
# get_user("user1")

# get_order("order123")
if '__name__' == '__main__':
    batch_order_generator(5)