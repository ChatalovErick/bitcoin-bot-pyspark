import websocket
import json
import asyncio
import json
import motor.motor_asyncio
import websockets
import pymongo

# Mongodb connection.
conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

## ----------------------------------------- ##
## Remove the data from the mongodb database ##

# send data for the collection 1 document at a time
clientdb =  pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
# mongodb collections from BTCUSDT Database.
clientdb.BTCUSDT.MatchTrades_V2.delete_many({})

## ----------------------------------------- ##
## ----------------------------------------- ##

async_client = motor.motor_asyncio.AsyncIOMotorClient(conn_str)
db = async_client.BTCUSDT.MatchTrades_V2

# Batch buffer
data_list = []

async def write_batch(batch):
    try:
        await db.insert_many(batch)
    except Exception as e:
        print("MongoDB insert error:", e)
        await asyncio.sleep(5)
        await write_batch(batch)

async def handle_message(message):
    global data_list
    try:
        trade = json.loads(message)
        data_MT = {
            "timestamp": trade["T"],  
            "qty": float(trade["q"]),
            "price": round(float(trade["p"]), 2),
            "isBuyerMaker": trade["m"],
            "isBestMatch": True
        }
        data_list.append(data_MT)

        if len(data_list) >= 50:
            batch = data_list.copy()
            data_list.clear()
            asyncio.create_task(write_batch(batch))

    except Exception as e:
        print("Processing error:", e)

async def start_ws():
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
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