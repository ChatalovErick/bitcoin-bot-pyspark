import numpy as np
from tick.hawkes import SimuHawkes, HawkesKernelExp
import matplotlib.pyplot as plt
import json
import pandas as pd
import os
from bson import json_util
import pymongo
from datetime import datetime, timedelta
import torch

# ------------------------------------------------- #
# (1) Handle data
# ------------------------------------------------- #

MatchTrade_data = os.listdir("Data/MatchTrades")
MatchTrade_data = [f for f in MatchTrade_data if f != '.ipynb_checkpoints']
#print(MatchTrade_data)

LOB_data = os.listdir("Data/LOB")
LOB_data = [f for f in LOB_data if f != '.ipynb_checkpoints']
#print(LOB_data)

with open("Data/MatchTrades/"+MatchTrade_data[0]) as f:
    MatchTrade_data = pd.read_json(f)    
    MatchTrade_data["timestamp"] = pd.to_datetime(MatchTrade_data["timestamp"], unit='ms')
    MatchTrade_data["trade"] = MatchTrade_data[["qty","price","isBuyerMaker","isBestMatch"]].values.tolist()
    MatchTrade_data["item"] = "match trade"
    MatchTrade_data = MatchTrade_data[["timestamp","item","trade"]]
    
with open("Data/LOB/"+LOB_data[0]) as f:
    LOB_data = pd.read_json(f)
    LOB_data["timestamp"] = pd.to_datetime(LOB_data["timestamp"], unit='ms')
    LOB_data["item"] = "lob"
    LOB_data["lob snapshot"] = LOB_data[["bids","asks"]].values.tolist()
    LOB_data = LOB_data[["timestamp","item","lob snapshot"]]

merged = pd.merge(MatchTrade_data, LOB_data, on=["timestamp","item"], how="outer").sort_values("timestamp")

# ------------------------------------------------- #
# (1) End
# ------------------------------------------------- #

# ------------------------------------------------- #
# (2) Get the timestamp for each event type
# ------------------------------------------------- #

def make_events(data):

    if not np.issubdtype(type(data), pd.core.frame.DataFrame):
        raise TypeError(f"data is not a pd.DataFrame")
        
    ts_buy_market_orders = []
    ts_sell_market_orders = []
    ts_buy_limit_orders = []
    ts_sell_limit_orders = []
    ts_buy_cancelation_orders = []
    ts_sell_cancelation_orders = []
    
    bids_tensors = None
    asks_tensors = None

    first_time = ((data.iloc[0])["timestamp"]).timestamp()
    
    for index, row in data.iterrows():
        
        if (row["item"] == "lob"):
            
            bids = [float(x) for x in ((row["lob snapshot"])[0])[0]]
            asks = [float(x) for x in ((row["lob snapshot"])[1])[0]]

            # --------------------------------------------- #
            # (1) Limit oders
            # --------------------------------------------- #

            if (asks_tensors != None):

                # Convert each list into a tensor 
                new_bids_tensors = torch.tensor(bids, dtype=torch.float32) 
                new_asks_tensors = torch.tensor(asks, dtype=torch.float32)
    
                new_bid_vol = (new_bids_tensors[1])
                new_ask_vol = (new_asks_tensors[1])

                # ------------------------------------ #
                # (1.1) buy limit order 
                
                if (new_bid_vol > bid_vol):
                    ts_buy_limit_orders.append(row["timestamp"].timestamp() - first_time)
                elif (new_bid_vol < bid_vol):
                    ts_buy_cancelation_orders.append(row["timestamp"].timestamp() - first_time)

                # (1.1) End 
                # ------------------------------------ #

                # ------------------------------------ #
                # (1.2) sell limit order 

                if (new_ask_vol > ask_vol):
                    ts_sell_limit_orders.append(row["timestamp"].timestamp() - first_time)
                elif (new_ask_vol < ask_vol):
                    ts_sell_cancelation_orders.append(row["timestamp"].timestamp() - first_time)

                # (1.2) End 
                # ------------------------------------ #
                
            else:

                bids_tensors = torch.tensor(bids, dtype=torch.float32)
                asks_tensors = torch.tensor(asks, dtype=torch.float32)
                
                bid_vol = (bids_tensors[1])
                ask_vol = (bids_tensors[1])
                
            # --------------------------------------------- #
            # (1) End                                       #
            # --------------------------------------------- #

        
        elif (row["item"] == "match trade"):
            
            if (row["trade"])[2] == True:
                ts_buy_market_orders.append(row["timestamp"].timestamp() - first_time)
            else:
                ts_sell_market_orders.append(row["timestamp"].timestamp() - first_time) 


    return [np.array(ts_buy_limit_orders, dtype=float), # type 0: buy limit orders at best bid
            np.array(ts_sell_limit_orders, dtype=float), # type 1: sell limit orders at best ask 
            np.array(ts_buy_market_orders, dtype=float), # type 2: buy market orders
            np.array(ts_sell_market_orders, dtype=float), # type 3: sell market orders
            np.array(ts_buy_cancelation_orders, dtype=float), # type 4: cancel buy limit orders
            np.array(ts_sell_cancelation_orders, dtype=float) # type 5: cancel sell limit orders
           ]

timestamps = make_events(merged)

# ------------------------------------------------- #
# (2) End
# ------------------------------------------------- #

# ------------------------------------------------- #
# (3) Estimate Hawkes Parameters
# ------------------------------------------------- #
from tick.hawkes import HawkesExpKern

decays_to_try = [0.5, 1.0, 1.5, 2.0]
best_ll = -np.inf
best_decay = None
for d in decays_to_try:
    learner = HawkesExpKern(decays=d)
    learner.fit(timestamps)
    ll = learner.score()  # log-likelihood
    if ll > best_ll:
        best_ll = ll
        best_decay = d
        best_learner = learner
        
print("Best decay:", best_decay)

# Extract parameters
baseline_est = best_learner.baseline       # shape (6,)
adjacency_est = best_learner.adjacency     # shape (6,6)

print("Baseline intensities:", baseline_est)
print("Excitation matrix:", adjacency_est)

# ------------------------------------------------- #
# (3) End
# ------------------------------------------------- #

# ------------------------------------------------- #
# (4) Simulate Hawkes LOB
# ------------------------------------------------- #

import numpy as np
from tick.hawkes import SimuHawkes, HawkesKernelExp

n_events = 6

# Build kernels from estimated adjacency
kernels = [[HawkesKernelExp(adjacency_est[i,j], best_decay) for j in range(n_events)]
           for i in range(n_events)]

hawkes = SimuHawkes(baseline=baseline_est, kernels=kernels, end_time=100, seed=42)
hawkes.simulate()
sim_timestamps = hawkes.timestamps

# -------------------------------- #
# (4.1) Simulate Hawkes LOB

# Initialize LOB
mid_price = 100.0
spread = 0.02
best_bid = mid_price - spread/2
best_ask = mid_price + spread/2

lob_evolution = [(0, best_bid, best_ask, mid_price, spread)]

# Merge all simulated events
all_events = []
for i, ts in enumerate(sim_timestamps):
    for t in ts:
        all_events.append((t, i))
all_events.sort(key=lambda x: x[0])

# Process events
for t, etype in all_events:
    if etype == 0: best_bid += 0.01          # buy limit
    elif etype == 1: best_ask -= 0.01        # sell limit
    elif etype == 2: best_ask += 0.01        # buy market
    elif etype == 3: best_bid -= 0.01        # sell market
    elif etype == 4: best_bid -= 0.01        # cancel buy
    elif etype == 5: best_ask += 0.01        # cancel sell

    mid_price = (best_bid + best_ask)/2
    spread = best_ask - best_bid
    lob_evolution.append((t, best_bid, best_ask, mid_price, spread))

# (4.1) End
# -------------------------------- #

# ------------------------------------------------- #
# (4) End
# ------------------------------------------------- #

# ------------------------------------------------- #
# (5) Visualize Mid-Price and Spread
# ------------------------------------------------- #

import matplotlib.pyplot as plt

lob_array = np.array(lob_evolution)
times = lob_array[:,0]
mid_prices = lob_array[:,3]
spreads = lob_array[:,4]

plt.figure(figsize=(12,5))
plt.subplot(2,1,1)
plt.plot(times, mid_prices, label='Mid-price')
plt.ylabel('Price')
plt.legend()

plt.subplot(2,1,2)
plt.plot(times, spreads, color='orange', label='Spread')
plt.ylabel('Spread')
plt.xlabel('Time')
plt.legend()
plt.tight_layout()
plt.show()

# ------------------------------------------------- #
# (5) End
# ------------------------------------------------- #
