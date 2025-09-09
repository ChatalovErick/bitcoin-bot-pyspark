import websockets
import json
import pymongo
import time
from datetime import datetime
import numpy as np
import pandas as pd
import motor.motor_asyncio
import asyncio
import threading 

# Mongodb connection.
conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

## ----------------------------------------- ##
## Remove the data from the mongodb database ##

# send data for the collection 1 document at a time
clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
# mongodb collections from BTCUSDT Database.
clientdb.BTCUSDT.LimitOrderBook_V2.delete_many({})

## ----------------------------------------- ##
## ----------------------------------------- ##

data_list = []
last_bids = None
last_asks = None

# Persistent async MongoDB client
async_client = motor.motor_asyncio.AsyncIOMotorClient(conn_str)
db = async_client.BTCUSDT.LimitOrderBook_V2

async def write_batch(data_list):
    try:
        await db.insert_many(data_list)
    except Exception as e:
        print("MongoDB insert error:", e)
        await asyncio.sleep(5)  # non-blocking retry
        await write_batch(data_list)


async def handle_message(message):
    global data_list, last_bids, last_asks
    
    try:
        data = json.loads(message)
        bids = [[float(price), float(qty)] for price, qty in data.get("bids", [])]
        asks = [[float(price), float(qty)] for price, qty in data.get("asks", [])]

        data_LOB = {
            "timestamp": int(datetime.now().timestamp() * 1000),
            "bids": bids,
            "asks": asks
        }
        
        if bids != last_bids or asks != last_asks:
            last_bids = bids
            last_asks = asks
            data_list.append(data_LOB)

            if len(data_list) >= 20:
                # Schedule async insert without blocking
                batch = data_list.copy()
                data_list.clear()
                asyncio.create_task(write_batch(batch))

        elif last_bids is None and last_asks is None:
            # First iteration
            last_bids = bids
            last_asks = asks
            data_list.append(data_LOB)

    except Exception as e:
        print("Processing error:", e)

async def start_ws():
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                print("Connected to Binance WebSocket")
                async for message in ws:
                    await handle_message(message)
        except Exception as e:
            print("WebSocket error:", e)
            print("Reconnecting...")

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_ws())
    except RuntimeError:  # already running loop (Jupyter case)
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.get_event_loop().create_task(start_ws())