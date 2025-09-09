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

class mongo():

    def __init__(self):
        self.conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    def get_MatchTrades(self):
        # get the data from the day for the Match trades 
        self.clientdb = MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)

        time_doc = self.clientdb.BTCUSDT.MatchTrades_V2.find({}).sort({"_id": -1}).limit(1)
        start_ts = int(list(time_doc)[0]["timestamp"]) # onde acaba

        time_doc = self.clientdb.BTCUSDT.MatchTrades_V2.find({}).sort({"_id": 1}).limit(1)
        end_ts = int(list(time_doc)[0]["timestamp"]) # onde comeca
        
        # Query for the previous day's MatchTrades
        query = {
            "timestamp": {
                "$gte": end_ts,
                "$lt": start_ts
            }
        }
        
        raw_data = list(self.clientdb.BTCUSDT.MatchTrades_V2.find(query,{'_id': 0}))
        
        json_raw_data = json_util.dumps(raw_data)
        
        # Save to file
        with open("Data/MatchTrades/"+"MatchTrades "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
            f.write(json_raw_data)
        
        #delete all the data from the day before
        #self.clientdb.BTCUSDT.MatchTrades_V2.delete_many(query)
        self.clientdb.close()

    def get_LOB(self):
        # get the data from the day for the Match trades 
        self.clientdb = MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)

        time_doc = self.clientdb.BTCUSDT.LimitOrderBook_V2.find({}).sort({"_id": -1}).limit(1)
        start_ts = int(list(time_doc)[0]["timestamp"]) # onde acaba

        time_doc = self.clientdb.BTCUSDT.LimitOrderBook_V2.find({}).sort({"_id": 1}).limit(1)
        end_ts = int(list(time_doc)[0]["timestamp"]) # onde comeca
        
        # Query for the previous day's MatchTrades
        query = {
            "timestamp": {
                "$gte": end_ts,
                "$lt": start_ts
            }
        }

        raw_data = list(self.clientdb.BTCUSDT.LimitOrderBook_V2.find(query,{'_id': 0}))
        json_raw_data = json_util.dumps(raw_data)

        # Save to file
        with open("Data/LOB/"+"LOB "+ datetime.fromtimestamp(end_ts/1000).strftime("%Y-%m-%d %H:%M:%S") + " to " + datetime.fromtimestamp(start_ts/1000).strftime("%Y-%m-%d %H:%M:%S") +".json", "w") as f:
            f.write(json_raw_data)
        
        #delete all the data from the day before
        #clientdb.BTCUSDT.LimitOrderBook_V2.delete_many(query)
        self.clientdb.close()

send_data = mongo()
send_data.get_MatchTrades()
send_data.get_LOB()