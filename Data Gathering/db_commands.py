import pymongo

conn_str = "mongodb+srv://erickchatalov:25e12c15r45f17@cluster0.8tszpip.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
clientdb = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

postsMatchTrades = clientdb.BTCEUR.MatchTrades
postLimitOrderBook = clientdb.BTCEUR.LimitOrderBook

# remove data from mongo database 
postsMatchTrades.delete_many({})
postLimitOrderBook.delete_many({})
