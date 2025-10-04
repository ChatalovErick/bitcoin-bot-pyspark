from pymongo.mongo_client import MongoClient
import time
from datetime import datetime, timedelta
#import os.path
import pandas as pd
#import matplotlib.pyplot as plt
import numpy as np
import statistics
import pickle
import math 
import json
from bson.son import SON
from bson import json_util

###################################
# connect to the mongodb database #
###################################

def get_MatchTrades(start_ts,end_ts):

    conn_str ="mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # get the data from the day for the Match trades 
    clientdb = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    
    # 1. Count total documents
    total = clientdb.BTCUSDT.MatchTrades_V2.count_documents({})
    half = math.floor(total / 2)
    
    # 3. Get the first half
    first_half_cursor = (clientdb.BTCUSDT.MatchTrades_V2.find()
                         .sort([("$natural", 1)])  # or use a field, e.g. ("createdAt",1)
                         .limit(half))
    
    first_half_docs = list(first_half_cursor)
    
    #raw_data = list(clientdb.BTCUSDT.MatchTrades_V2.find(query,{'_id': 0}))
    
    # 4. last half
    second_half_cursor = (clientdb.BTCUSDT.MatchTrades_V2.find()
                    .sort([("$natural", 1)])   # or use a field, e.g. ("createdAt", 1)
                    .skip(half))

    second_half_docs = list(second_half_cursor)
    
    # 5. combine lists
    all_docs = first_half_docs + second_half_docs
    
    json_raw_data = json_util.dumps(all_docs)
    
    # Save to file
    with open("MarketData/MatchTrades/"+"MatchTrades "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
        f.write(json_raw_data)

    # save the file to data analysis folder
    with open("/home/erickchatalov/Data Analysis/Market Trades Data/Data/"+"MatchTrades "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
        f.write(json_raw_data)

    # Query for the previous day's MatchTrades
    query = {
        "timestamp": {
            "$gte": end_ts,
            "$lt": start_ts
        }
    }
    
    #delete all the data from the day before
    clientdb.BTCUSDT.MatchTrades_V2.delete_many(query)
    clientdb.close()

def get_LOB(start_ts,end_ts):

    conn_str ="mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # get the data from the day for the Match trades 
    clientdb = MongoClient(conn_str, serverSelectionTimeoutMS=5000)

    # 1. Count total documents
    total = clientdb.BTCUSDT.LimitOrderBook_V2.count_documents({})
    half = math.floor(total / 2)

    # 3. Get the first half LimitOrderBook_V2
    first_half_cursor = (clientdb.BTCUSDT.LimitOrderBook_V2.find()
                         .sort([("$natural", 1)])  # or use a field, e.g. ("createdAt",1)
                         .limit(half))
    
    first_half_docs = list(first_half_cursor)
    
    # 4. Get the second half
    second_half_cursor = (clientdb.BTCUSDT.LimitOrderBook_V2.find()
                    .sort([("$natural", 1)])   # or use a field, e.g. ("createdAt", 1)
                    .skip(half))

    second_half_docs = list(second_half_cursor)
    
    # 5. combine lists
    all_docs = first_half_docs + second_half_docs
    
    json_raw_data = json_util.dumps(all_docs)

    # Save to file
    with open("MarketData/LimitOrderBook/"+"LOB "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
        f.write(json_raw_data)

    # save the file to data analysis folder
    with open("/home/erickchatalov/Data Analysis/LOB Data/Data/"+"LOB "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
        f.write(json_raw_data)

    # Query for the previous day's MatchTrades
    query = {
        "timestamp": {
            "$gte": end_ts,
            "$lt": start_ts
        }
    }
    
    #delete all the data from the day before
    clientdb.BTCUSDT.LimitOrderBook_V2.delete_many(query)
    clientdb.close()

#########################################################################################
# ------------------------------------------------------------------------------------- #
#########################################################################################
import schedule
import time

def job():
    
    conn_str ="mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # get the data from the day for the Match trades 
    clientdb = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    
    time_doc = clientdb.BTCUSDT.MatchTrades_V2.find({}).sort({"_id": -1}).limit(1)
    start_ts = int(list(time_doc)[0]["timestamp"]) # onde acaba
    
    time_doc = clientdb.BTCUSDT.MatchTrades_V2.find({}).sort({"_id": 1}).limit(1)
    end_ts = int(list(time_doc)[0]["timestamp"]) # onde comeca
    
    clientdb.close()
    
    get_MatchTrades(start_ts,end_ts)
    get_LOB(start_ts,end_ts)
    print("job done")

# Or specific times
schedule.every().day.at("06:00").do(job)
schedule.every().day.at("12:00").do(job)
schedule.every().day.at("18:00").do(job)
schedule.every().day.at("00:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
