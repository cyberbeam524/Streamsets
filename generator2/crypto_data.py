# # !pip install websocket alpaca-trade-api pprint
# from websocket import create_connection
# import json as simplejson
# # import pprint
import json

# uri = 'wss://stream.data.alpaca.markets/v1beta1/crypto?exchanges=CBSE'
# ws = create_connection(uri)

# key = "PKX85ZA8Q4S8JSUMCYUH"
# secret_key = "Qwkk2b71Og92FKOqBVPVaB5oJvT10LwnUwlEXMm3"
# auth_message = {"action":"auth","key": key, "secret": secret_key}
# ws.send(json.dumps(auth_message))

# subscription = {"action":"subscribe","trades":["BTCUSD", "ETHUSD", "DOGEUSD"]}
# ws.send(json.dumps(subscription))
# # while True:
# #     data = json.loads(ws.recv())
# #     pprint.pprint(data[0])


import random
import time, calendar
from random import randint
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep


def write_data(producer):
    data_cnt = 20000
    order_id = calendar.timegm(time.gmtime())
    max_price = 100000
    topic = "crypto_msg"

    for i in range(data_cnt):
        # data = json.loads(ws.recv())
        # ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime())
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # ts = time.strftime("%Y-%m-%dd", time.localtime())
        # rd = random.random()
        # order_id += 1
        # pay_amount = max_price * rd
        # pay_platform = 0 if random.random() < 0.9 else 1
        # province_id = randint(0, 6)
        # latitude = round(random.uniform(-90.0, 90.0), 2) 
        # longitude = round(random.uniform(-180.0, 180.0), 2) 
        i = round(random.uniform(0,454528175), 0) 
        p = round(random.uniform(0,16365.03), 2) 
        s = round(random.uniform(0, 0.004), 3) 

        coins = ["BTCUSD", "ETHUSD", "DOGEUSD"]
        cur_data = {'S': coins[randint(0, len(coins) - 1)],
                    'T': 't',
                    'i': i,
                    'p': p,
                    's': s,
                    't': ts,
                    'tks': 'B',
                    'x': 'CBSE'}
        producer.send(topic, value=cur_data)
        sleep(0.5)
        # print(cur_data)

def create_producer():
    print("Connecting to Kafka brokers crypto_msg")
    for i in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    print("crypto_msg_here")
    producer = create_producer()
    write_data(producer)
