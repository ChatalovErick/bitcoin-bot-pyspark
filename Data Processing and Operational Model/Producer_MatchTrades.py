import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from datetime import datetime
import asyncio
import time
import math
import json
import uuid
from kafka import KafkaConsumer, KafkaProducer
import  sys 
import six
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


class sliding_window():

    def __init__(self):
        self.MatchTradesdata = None
        self.data = {}

    def new_MatchTrades(self,data):
        self.MatchTradesdata = data

    def get_MatchTrades(self):
        return self.MatchTradesdata

    def send_MatchTrades(self):
        # send json data 
        self.data["date"] = str(datetime.now())
        self.data["amount"] = self.MatchTradesdata["amount"][0]
        self.data["price"] = self.MatchTradesdata["price"][0]
        return json.dumps(self.data)


async def run_sliding_window():
    
    conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    sw = sliding_window()

    while True:

        clientdb = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        matchtrades = pd.DataFrame(clientdb.BTCEUR.MatchTrades.find({}).sort({"_id":-1}).limit(1)).drop("_id",axis=1)
        clientdb.close()

        # send match trades data #
        try:          
            sw.get_MatchTrades() != None
            if (((sw.get_MatchTrades())["timestamp"])[0] != (matchtrades["timestamp"])[0]):
                sw.new_MatchTrades(matchtrades)
                print(sw.send_MatchTrades())

                # post to kafka #
                try:
                    producer = KafkaProducer(bootstrap_servers="localhost:9092")
                    producer.send('MatchTrades-data', key=bytes(str(uuid.uuid4()), 'utf-8'), value= bytes( str( sw.send_MatchTrades() ),'utf-8' ))
                    producer.close()
                except:
                    pass

        except:
            sw.new_MatchTrades(matchtrades)
            print(sw.send_MatchTrades())
            
            # post to kafka #
            try:
                producer = KafkaProducer(bootstrap_servers="localhost:9092")
                producer.send('MatchTrades-data', key=bytes(str(uuid.uuid4()), 'utf-8'), value= bytes( str( sw.send_MatchTrades() ),'utf-8' ))
                producer.close()
            except:
                pass


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sliding_window())

while True:
    try: 
        main()
    except:
        continue
