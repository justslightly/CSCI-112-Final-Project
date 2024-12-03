# READ
from utils import *

# Retrieve customers
def getCustomers():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['customer']

    results = collection.find()

    for result in results:
        print(result)

    closeConnection(conn)


# Get all dividend-earning accounts  (client Accounts)
def getAllClientAccs():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'clientAcc': True })

    for result in results:
        print(result)

    closeConnection(conn)


# Get all accounts of a customer
def getAllClientAccs():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'customer_id': 4 })

    for result in results:
        print(result)

    closeConnection(conn)


# Get all dividends paid within October 2024
def getDividends():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['transaction']

    results = collection.find({ 'dividend': True, 'date_time': {'$gte': (2024, 10, 1), '$lte': (2024, 10, 31)} })

    for result in results:
        print(result)

    closeConnection(conn)

# Get certain account
def readFindOneAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    results = collection.find_one({ 'account_number': "0-20170209" })

    closeConnection(conn)

    return results


# Get accounts more than a year ago 
def getInactiveAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    one_year = datetime.now() - timedelta(days=365)

    results = collection.find({ 'date_created': { '$lt': one_year } })

    for result in results:
        print(result)

    closeConnection(conn)


# Get accounts exceeding threshold
def getExceedingAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'balance': { '$gt': 500000 } }) # can modify 

    for result in results:
        print(result)

    closeConnection(conn)


# Get accounts sorted by balance
def getAccBalance():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find().sort('balance', -1)

    for result in results:
        print(result)

    closeConnection(conn)


# Get unpaid shares
def getOverdueShares():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['shares']

    results = collection.find({ 'status': 'Unpaid' })

    for result in results:
        print(result)

    closeConnection(conn)


    # Get payment date of specific dividend 
def getDivPaymentDate(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    result = collection.find_one({ 'account_number': account_number }, { 'due_date': 1, '_id': 0 })

    closeConnection(conn)

    return results


# AGGREGATION PIPELINE

# Get total dividends history of a client & total per issuer. Sorted from 
def getTotalDividends():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['transaction']

    pipeline = [
        {
            '$match': {
                'dividend': True,
                'account_to': "0-20170209"
            }
        }, {
            '$group': {
                '_id': '$share_id', 
                'issuer': '$account_from'
                'total': {
                    '$sum': '$amount'
                }
            }
        }, {
            '$sort': {
                'total': -1
            }
        }, {
            '$project': {
                '_id': 0,
                'issuer': 1,
                'total': 1
            }
        }, {
            '$out': 'totalDividends'
        }
    ]

    results = collection.aggregate(pipeline)


# Get shares owned by a customer. Sort by most to least shares.
def getShares():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$match': {
                'account_id': "0-20170209"
            }
        }, {
            '$group': {
                '_id': '$issuer_id', 
                'total': {
                    '$sum': '$total_owned'
                }
            }
        }, {
            '$sort': {
                'total': -1
            }
        }, {
            '$project': {
                '_id': 1,
                'total': 1
            }
        }, {
            '$out': 'sortedSharesofAcc'
        }
    ]

    results = collection.aggregate(pipeline)

    # Get shares owned by all customers. Grouped by city - not sure if correct ?
def getSharesByCity():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$lookup': {
                'from': 'customer',
                'localField': 'customer_id',
                'foreignField': '_id',
                'as': 'customer_data'
            }
        },
        {
            '$unwind': '$customer_data'
        },
        {
            '$group': {
                '_id': '$customer_data.address',
                'total_shares': { '$sum': '$total_owned' }
            }
        },
        {
            '$sort': { 'total_shares': -1 }
        }
    ]

    results = collection.aggregate(pipeline)


    # Get total balance accross accounts by currency
def getTotalBalanceByCurrency():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    pipeline = [
        {
            '$group': {
                '_id': '$currency',
                'total_balance': { '$sum': '$balance' }
            }
        },
        {
            '$sort': { 'total_balance': -1 }
        }
    ]

    results = collection.aggregate(pipeline)


# FOR REVIEW

# Updating due date for shares of a certain issuer.
def setDueDate():
    conn = openConnection()

    # because not all shares earn dividends ?

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$match': {
                'issuer_id': "8"
            }
        }, {
            '$set': {
                'due_date': (2024, 11, 30)
            }
        }, {
            '$out': 'shares'
        }
    ]

    results = collection.aggregate(pipeline)


# Marking shares that are past due dates in dividends. Unmarking those that are not.
def markOverdue():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
    	# set overdue to overdue ones
        {
            '$match': {
                'due_date': {'$exists': True, '$lt': (2024, 12, 7)}
            }
        }, {
            '$set': {
                'status': 'overdue'
            }
        }, 

        # set status 'today' to dividends due today
        {	
            '$match': {
                'due_date': {'$exists': True, '$eq': (2024, 12, 7)}
            }
        }, {
            '$set': {
                'status': 'today'            
            }
        }, 

        # remove status for dividends not urgent
        {	
            '$match': {
                'due_date': {'$exists': True, '$gt': (2024, 12, 7)}
            }
        }, {
            '$unset': {
                'status': ""
            }
        }, 

        {
            '$out': 'shares'
        }
    ]

    results = collection.aggregate(pipeline)