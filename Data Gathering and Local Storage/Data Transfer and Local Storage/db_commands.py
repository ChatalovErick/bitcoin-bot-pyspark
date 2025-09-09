import pymongo

conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
clientdb = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

#postsMatchTrades = clientdb.BTCUSDT.MatchTrades
#postsMatchTrades1 = clientdb.BTCUSDT.MatchTrades1
postsMatchTrades_V2 = clientdb.BTCUSDT.MatchTrades_V2
postLimitOrderBook = clientdb.BTCUSDT.LimitOrderBook
postLimitOrderBook_V2 = clientdb.BTCUSDT.LimitOrderBook_V2
bot_orders = clientdb.BTCUSDT.bot_orders

# remove data from mongo database 
#postsMatchTrades.delete_many({})
#postsMatchTrades1.delete_many({})
postsMatchTrades_V2.delete_many({})
postLimitOrderBook.delete_many({})
postLimitOrderBook_V2.delete_many({})
bot_orders.delete_many({})