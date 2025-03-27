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

# --------------------------------------------------------------- #
# calculate attributes from the lob data
# --------------------------------------------------------------- #

class Lob():

    def __init__(self):
        self.lobdata = None

    def new_lob(self,data):
        self.lobdata = data
    
    def get_lob(self):
        self.timestamp = self.lobdata["timestamp"].item()
        self.bids = self.lobdata["bids"][0]
        self.asks = self.lobdata["asks"][0]
        return json.dumps({"timestamp":self.timestamp,"bids":list(self.bids),"asks":list(self.asks)})

    def get_timestamp(self):
        return self.lobdata["timestamp"].item()

async def send_lob_data():
    
    conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    lob = Lob()

    while True:

        clientdb = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        lob_data = pd.DataFrame(clientdb.BTCEUR.LimitOrderBook.find({}).sort({"_id":-1}).limit(1)).drop("_id",axis=1)
        clientdb.close()
        
        # send lob data #
        try:          
            lob.get_lob() != None
            
            if (lob.get_timestamp() != (lob_data["timestamp"])[0]):
                
                lob.new_lob(lob_data)
                
                # post to kafka #
                try:
                    producer = KafkaProducer(bootstrap_servers="localhost:9092")
                    producer.send('Lob-data', key=bytes(str(uuid.uuid4()), 'utf-8'), value= bytes( str( lob.get_lob() ),'utf-8' ))
                    producer.close()
                except:
                    pass

                print(lob.get_timestamp())
                
        except:
            
            lob.new_lob(lob_data)

            # post to kafka #
            try:
                producer = KafkaProducer(bootstrap_servers="localhost:9092")
                producer.send('Lob-data', key=bytes(str(uuid.uuid4()), 'utf-8'), value= bytes( str( lob.get_lob() ),'utf-8' ))
                producer.close()
            except:
                pass

            print(lob.get_timestamp())
            
        time.sleep(1)
        # ------------- #

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_lob_data())

while True:
    try: 
        main()
    except:
        continue
