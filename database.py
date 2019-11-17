""" 
Author: Aleksandra Sokolowska
for Validity Labs AG
"""

from web3 import Web3
from organize import *

import time
import requests
from os import path

#uncomment one of the options below
# 1. connection via Infura
web3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/9d4b89c7861946969efa409617d2504d"))

# 2. or connection via local node 
#web3 = Web3(Web3.IPCProvider('/your-path-to/geth.ipc'))

# load a block.
Nblocks = 100
output_every = 2
start_time = time.time()
try:
    with open('lastblock.txt', 'r') as f:
        start = int(f.read())+1
except FileNotFoundError:
    start = 2000000

#define tokens to track
token_file_path = "./tokens.txt"
token_list = []

# get top 5 tokens by marketcap
num_tokens = 5
resp = requests.get("http://api.ethplorer.io/getTop?apiKey=freekey&limit=" + str(num_tokens))
if resp.status_code != 200:
    raise ApiError("Failed to retrieve token data")

if path.exists(token_file_path):
    # if tokens.txt already exists, read
    f = open(token_file_path, "r")
    
    line = f.readline()
    while line:
        line = line.rstrip().split(",")
        token_list.append({"address": line[1], "symbol": line[0]})

        line = f.readline()
    f.close()
else:
    # no tokens.txt exists, create
    f = open(token_file_path, "w")

    # API call
    resp = resp.json()["tokens"]
    for t in resp:
        if t["symbol"] == "ETH":
            continue
        address = web3.toChecksumAddress(t["address"])
        symbol = t["symbol"]
        token_list.append({"address": address, "symbol": symbol})
        f.write(symbol + "," + address + "\n")
    f.close()

#define tables that will go to the SQLite database
table_quick = []
table_tx = []
table_block = []
table_tokens = []

count = 0
#loop over all blocks
for block in range(start, start+Nblocks):
    
    block_table, block_data = order_table_block(block,web3)
    #list of block data that will go to the DB
    table_block.append(block_table)

    # addresses to scan for tokens
    wallet_addresses = []

    #all transactions on the block
    for hashh in block_data['transactions']:
        #print(web3.toHex(hashh))       
        quick_table, tx_data = order_table_quick(hashh,block, web3)
        table_quick.append(quick_table)
        
        # scrap quick table for wallet addresses
        wallet_addresses.append(quick_table["from"])
        wallet_addresses.append(quick_table["to"])

        #list of tx data that will go to the DB
        TX_table = order_table_tx(tx_data,hashh, web3)
        table_tx.append(TX_table)
    count = count + 1

    #find token balances
    for w in wallet_addresses:
        token_entry = order_table_token(w, token_list, web3)
        table_tokens.append(token_entry)
    del wallet_addresses

    #dump output every 2 blocks
    if (count % output_every) == 0:
        execute_sql(table_quick, table_tx, table_block, table_tokens, token_list)
        
        #free up memory
        del table_quick
        del table_tx
        del table_block
        del table_tokens
        table_quick = []
        table_tx = []
        table_block = []
        table_tokens = []
        
        #update the current block number to a file
        with open('lastblock.txt', 'w') as f:
            f.write("%d" % block)
    if (count % 10) == 0:
        end = time.time()
        with open('timeperXblocks.txt', 'a') as f:
            f.write("%d %f \n" % (block, end-start_time))
    if (count % 100) == 0:
        print("100 new blocks completed.")
