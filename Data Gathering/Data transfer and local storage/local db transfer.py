from pymongo.mongo_client import MongoClient
import schedule
import time
from datetime import datetime, timedelta
#import os.path
import pandas as pd
#import matplotlib.pyplot as plt
import numpy as np
import statistics
import pickle
import math 

###################################
# connect to the mongodb database #
###################################

class mongo():

    def __init__(self):
        self.conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    
    # --------------------------------------------------- 
    # Match Trades
    # ---------------------------------------------------

    def get_MatchTrades(self,time_x):
        # get the data from the day for the Match trades 
        self.clientdb = MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)
    
        self.data = pd.DataFrame(self.clientdb.BTCEUR.MatchTrades.find({}))
        self.data["day"] = self.data["timestamp"].transform(lambda x: datetime.fromtimestamp(x/1000).strftime("%d"))
        self.data["trade_date"] = self.data["timestamp"].transform(lambda x: datetime.fromtimestamp(x/1000).strftime("%H:%M:%S"))
        self.data = self.data[self.data["day"] != datetime.fromtimestamp(time_x).strftime("%d")] # datetime.today().strftime("%d")

        with open("dailydata/MatchTrades_data/"+"MatchTrades "+datetime.fromtimestamp((time_x-86400),).strftime("%d-%m-%Y")+".pickle", 'wb') as handle:
            pickle.dump(self.data,handle)

        #delete all the data from the day before
        self.clientdb.BTCEUR.MatchTrades.delete_many({"_id":{"$in": list(self.data["_id"])}})
        self.clientdb.close()

    # -----------------------------------------------
    #  lob data 
    # -----------------------------------------------
    
    def get_LOB(self,time_x):
        # get the data from the day for the Match trades 
        self.clientdb = MongoClient(self.conn_str, serverSelectionTimeoutMS=5000)
    
        self.data = pd.DataFrame(self.clientdb.BTCEUR.LimitOrderBook.find({}))
        self.data["day"] = self.data["timestamp"].transform(lambda x: datetime.fromtimestamp(x).strftime("%d"))
        self.data["trade_date"] = self.data["timestamp"].transform(lambda x: datetime.fromtimestamp(x).strftime("%H:%M:%S"))
        self.data = self.data[self.data["day"] != datetime.fromtimestamp(time_x).strftime("%d")] # datetime.today().strftime("%d")

        with open("dailydata/LimitOrderBook_data/"+"LimitOrderBook "+datetime.fromtimestamp((time_x-86400)).strftime("%d-%m-%Y")+".pickle", 'wb') as handle:
            pickle.dump(self.data,handle)

        #delete all the data from the day before
        self.clientdb.BTCEUR.LimitOrderBook.delete_many({"_id":{"$in": list(self.data["_id"])}})
        self.clientdb.close()

##################################
## used in the schedule routine ##
##################################

#################################################################################
# picking daily data form mongodb and storing it in the computer ################
#################################################################################
#time_start = "13"

while(True):

    time_now = math.floor(datetime.today().timestamp())
    hour_now = datetime.fromtimestamp(time_now).strftime("%H")

    if (hour_now == "02"):
        mg = mongo()
        mg.get_MatchTrades(time_now)
        mg.get_LOB(time_now)
        #time.sleep(23*(60*60))

    time.sleep(60)
    