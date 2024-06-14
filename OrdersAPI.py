import boto3 
import requests
import json     
client = boto3.client('apigateway')


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



def create_order(order):
    api_gateway_url = "https://zuiz4od611.execute-api.us-east-1.amazonaws.com/test/orders"
    response = requests.post(api_gateway_url, json=order)
    if response.status_code == 201:
        data = response.json()
        print(data)
        return data['message']
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.json())
        return None
    

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
