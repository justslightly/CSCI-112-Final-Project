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

# Deleting shares documents without any shares owned
def deleteEmptyShares():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['shares']

    results = collection.delete_many({ 'total_owned': 0 })

    print(results)

    closeConnection(conn)
