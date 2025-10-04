import pymongo

conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
clientdb = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

postsMatchTrades_V2 = clientdb.BTCUSDT.MatchTrades_V2
postLimitOrderBook_V2 = clientdb.BTCUSDT.LimitOrderBook_V2
bot_orders = clientdb.BTCUSDT.bot_orders

# remove data from mongo database 
postsMatchTrades_V2.delete_many({})
postLimitOrderBook_V2.delete_many({})
bot_orders.delete_many({})