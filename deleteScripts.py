from utils import *

# DELETE

# Deleting all customers
def deleteCustomer():    
    conn = openConnection()
    db = conn['Bank112']
    collection = db['customer']
    results = collection.delete_many({})
    print(results)
    closeConnection(conn)

# Deleting all accounts
def deleteAccount():    
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.delete_many({})
    print(results)
    closeConnection(conn)

# Deleting all issuers
def deleteIssuer():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['issuer']
    results = collection.delete_many({})
    print(results)
    closeConnection(conn)

# Deleting all shares
def deleteShares():    
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.delete_many({})
    print(results)
    closeConnection(conn)

# Deleting all transactions
def deleteTransaction():    
    conn = openConnection()
    db = conn['Bank112']
    collection = db['transaction']
    results = collection.delete_many({})
    print(results)
    closeConnection(conn)

# Deleting all aggregates
def deleteAggregates():
    conn = openConnection()
    db = conn['Bank112']
    coll1 = db['totalDividends']
    coll2 = db['dividendsByTime']
    coll3 = db['sortedSharesofAcc']
    coll4 = db['sharesByCity']
    coll5 = db['totalBalanceByCurrency']
    result1 = coll1.delete_many({})
    result2 = coll2.delete_many({})
    result3 = coll3.delete_many({})
    result4 = coll4.delete_many({})
    result5 = coll5.delete_many({})
    closeConnection(conn) 

# Deleting shares documents without any shares owned
def deleteEmptyShares():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.delete_many({ 'total_owned': 0 })
    print(results)
    closeConnection(conn)
