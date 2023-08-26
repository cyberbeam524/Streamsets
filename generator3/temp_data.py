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
import datetime


def write_data(producer):
    data_cnt = 20000
    order_id = calendar.timegm(time.gmtime())
    max_price = 100000
    topic = "temp_iot_msg"

    for i in range(data_cnt):
        # yyyy-MM-dd HH:mm:ss'
        # ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime())
        
        ts = round(time.time() * 1000)
        
        temp = round(random.uniform(31.0, 25.0), 1) 
        deviceid = round(random.uniform(0, 10)) 
        cur_data = {'deviceid': deviceid,
                    'temp': temp,
                    'timelog': ts
                    }
        if deviceid == 1:
            cur_data = {'deviceid': deviceid,
                    'temp': 0,
                    'timelog': ts
                    }

        producer.send(topic, value=cur_data)
        sleep(0.5)
        # print(cur_data)

def create_producer():
    print("Connecting to Kafka brokers temp_iot_msg")
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
    print("temp_iot_msg here")
    producer = create_producer()
    write_data(producer)
