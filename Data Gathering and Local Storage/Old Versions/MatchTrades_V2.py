import asyncio
import time
from datetime import datetime
from tokenize import group
import numpy as np
import pandas as pd
import pymongo
import calendar
import math
import sys
import json
import requests
import ipaddress

async def MatchTrades():
    
    # Mongodb connection.
    conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # send data for the collection 1 document at a time
    clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    # mongodb collections from BTCUSDT Database.
    clientdb.BTCUSDT.MatchTrades_V2.delete_many({})
    
    obj_MT  = None

    data_list = []
    
    while True:

        response = requests.get("https://www.binance.com/api/v3/trades?limit=1&symbol=BTCUSDT").json()
        
        try:
            obj_MT != None
            if (obj_MT["time"] != response[0]["time"]):
                
                obj_MT = response[0]
                
                data_MT = {"timestamp":response[0]["time"],
                        "qty": float(response[0]["qty"]),
                        "price": round(float(response[0]["price"]),2),
                        "isBuyerMaker":response[0]["isBuyerMaker"],
                        "isBestMatch":response[0]["isBestMatch"]} 
                
                data_list.append(data_MT)

                try:
                    if (len(data_list) >= 20):
                        
                        # send data for the collection 1 document at a time
                        clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    
                        # mongodb collections from BTCUSDT Database.
                        postsMT = clientdb.BTCUSDT.MatchTrades_V2
                        postsMT.insert_many(data_list)
    
                        clientdb.close()
    
                        data_list = []
                    
                except:
                    pass
            
        except:
           
            obj_MT = response[0]
            
            data_MT = {"timestamp":response[0]["time"],
                        "qty": float(response[0]["qty"]),
                        "price": round(float(response[0]["price"]),2),
                        "isBuyerMaker":response[0]["isBuyerMaker"],
                        "isBestMatch":response[0]["isBestMatch"]}
            
            data_list.append(data_MT)

        time.sleep(0.05)
        
def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MatchTrades())

while True:
    try: 
        main()
    except:
        continue