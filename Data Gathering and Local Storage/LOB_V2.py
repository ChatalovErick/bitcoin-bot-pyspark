import asyncio
import time
from datetime import datetime
from tokenize import group
import numpy as np
import pandas as pd
import pymongo
import math
import requests
import torch

async def LOB():
    
    # Mongodb connection.
    conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

     # send data for the collection 1 document at a time
    clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

    # mongodb collections from BTCUSDT Database.
    clientdb.BTCUSDT.LimitOrderBook_V2.delete_many({})

    data_list = []
    
    while True:

        response = requests.get("https://www.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5").json()
        
        bids = [[float(x) for x in sublist] for sublist in response.get("bids")]
        asks = [[float(x) for x in sublist] for sublist in response.get("asks")]
        
        data_LOB = {"timestamp":int(datetime.now().timestamp()*1000),
                    "bids":bids,
                    "asks":asks}
                
        try: 
            (asks_tensors != None)
            
            # Convert each list into a tensor 
            new_bids_tensors = torch.tensor(bids, dtype=torch.float32) 
            new_asks_tensors = torch.tensor(asks, dtype=torch.float32)

            # get the difference from the colunm values from a LOB snapshot to another #
            bids_diff = new_bids_tensors - bids_tensors 
            asks_diff = new_asks_tensors - asks_tensors

            # Sum the differences over rows to get per-column differences
            bids_diff_per_column = bids_diff.sum(dim=1)
            asks_diff_per_column = asks_diff.sum(dim=1)

            bids_tensors = new_bids_tensors
            asks_tensors = new_asks_tensors
            
            if ( (torch.all(bids_diff_per_column != 0)) and (torch.all(asks_diff_per_column != 0))):

                data_list.append(data_LOB)

                if (len(data_list) >= 20):
                    # send data for the collection 1 document at a time
                    clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        
                    # mongodb collections from BTCUSDT Database.
                    postsMT = clientdb.BTCUSDT.LimitOrderBook_V2
                    postsMT.insert_many(data_list)
        
                    clientdb.close()

                    data_list = []

        except:
            
            bids_tensors = torch.tensor(bids, dtype=torch.float32) 
            asks_tensors = torch.tensor(asks, dtype=torch.float32)

            data_list.append(data_LOB)

    time.sleep(0.05)         
        
        
def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(LOB())

while True:
    try: 
        main()
    except:
        continue
