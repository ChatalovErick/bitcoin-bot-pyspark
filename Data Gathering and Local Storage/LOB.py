import asyncio
import time
from datetime import datetime
from tokenize import group
import numpy as np
import pandas as pd
import pymongo
import math
import requests

async def LOB():
    
    # Mongodb connection.

    conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    time_start = math.floor(datetime.now().timestamp())

    while True:

        time_now = math.floor(datetime.now().timestamp())

        if (time_now >= (time_start + 20)):
            
            time_start = time_now

            try: 
                response = requests.get("https://www.binance.com/api/v3/depth?symbol=BTCUSDT&limit=50").json()
                data_LOB = {"timestamp":time_now,
                            "bids":response.get("bids"),
                            "asks":response.get("asks")}
            except:
                pass


            try:     
                # send data for the collection 1 document at a time
                clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

                # mongodb collections from BTCUSDT Database.
                postsMT = clientdb.BTCUSDT.LimitOrderBook
                postsMT.insert_one(data_LOB)

                clientdb.close()

            except:
                pass

        time.sleep(0.1)         
        
        
def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(LOB())

while True:
    try: 
        main()
    except:
        continue
