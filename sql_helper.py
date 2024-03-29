""" 
Author: Aleksandra Sokolowska
for Validity Labs AG
"""
def create_database(cur, token_list):
    """ create the schema for the database"""
    quick = """
    CREATE TABLE IF NOT EXISTS Quick (
     balanceFrom TEXT,
     balanceTo TEXT,
     blockNumber INTEGER, 
     sender TEXT,
     nonce INTEGER, 
     recipient TEXT,
     txHash TEXT PRIMARY KEY,
     value TEXT);"""

    tx = """
    CREATE TABLE IF NOT EXISTS TX (
     blockNumber INTEGER,
     contractAddress TEXT,
     cumulativeGasUsed INTEGER, 
     gas INTEGER, 
     gasPrice INTEGER, 
     gasUsed INTEGER,
     input TEXT, 
     logs TEXT, 
     logsBloom TEXT, 
     r TEXT, 
     s TEXT, 
     status INTEGER, 
     txHash TEXT PRIMARY KEY,
     transactionIndex INTEGER, 
     v INTEGER);"""

    blck = """
    CREATE TABLE IF NOT EXISTS block ( 
     blockGasUsed INTEGER, 
     blockHash TEXT, 
     blockLogsBloom TEXT, 
     blockNonce TEXT, 
     blockNumber INTEGER PRIMARY KEY, 
     difficulty TEXT, 
     extraData TEXT, 
     gasLimit INTEGER, 
     miner TEXT, 
     mixHash TEXT,      
     parentHash TEXT, 
     receiptsRoot TEXT, 
     sha3Uncles TEXT, 
     size INTEGER, 
     stateRoot TEXT, 
     timestamp INTEGER, 
     totalDifficulty TEXT, 
     transactions TEXT, 
     transactionsRoot TEXT, 
     uncles TEXT); """

    # append after api call
    token_balance = """
    CREATE TABLE IF NOT EXISTS token_balance (
     wallet_address TEXT,"""
    for t in token_list:
        token_balance += " " + t["symbol"] + " TEXT,"
    token_balance = token_balance[:-1]
    token_balance += ");";

    cur.execute(quick)
    cur.execute(blck)
    cur.execute(tx)
    cur.execute(token_balance)

def create_index(cur):
    quick = "CREATE INDEX index_quick ON Quick(value, sender, recipient);"
    tx = "CREATE INDEX index_TX ON TX(blockNumber, status);"
    blck = "CREATE INDEX index_block ON block(timestamp);"
    tokens = "CREATE INDEX index_token ON token_balance(wallet_address);"
    
    cur.execute(quick)
    cur.execute(blck)
    cur.execute(tx)
    cur.execute(tokens)
    
def update_database(cur, table_quick, table_tx, table_block, table_tokens):
    """ write lists of dictionaries into the database"""
    quick = """INSERT INTO Quick VALUES (:balanceFrom, :balanceTo, :blockNumber, :from, :nonce, :to, :txHash, :value); """
    tx = """ INSERT INTO TX VALUES (:blockNumber, :contractAddress, :cumulativeGasUsed, :gas, :gasPrice, :gasUsed, :input, :logs, :logsBloom, :r, :s, :status, :txHash, :transactionIndex, :v); """
    blck = """ INSERT INTO block VALUES (:blockGasUsed, :blockHash, :blockLogsBloom, :blockNonce, :blockNumber,  :difficulty, :extraData, :gasLimit, :miner, :mixHash, :parentHash, :receiptsRoot, :sha3Uncles, :size, :stateRoot, :timestamp, :totalDifficulty, :transactions, :transactionsRoot, :uncles); """ 
    tokens = """ INSERT OR REPLACE INTO token_balance VALUES ( :wallet_address,"""
    
    if len(table_tokens) > 0:
        for t in table_tokens[0]:
            if t == "wallet_address":
                continue
            tokens += " :" + t + ","
        tokens = tokens[:-1]
        tokens += ");"

    cur.executemany(quick, table_quick)
    cur.executemany(tx, table_tx)
    cur.executemany(blck, table_block)
    if len(table_tokens) > 0:
        cur.executemany(tokens, table_tokens)
