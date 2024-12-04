from pymongo import *
from datetime import *
from utils import *
from readScripts import *
import random as r

# AGGREGATION PIPELINES

# TESTED Get total dividends history of a client & total per issuer. Sorted from most to least.
def aggregateTotalDividends(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['transaction']

    pipeline = [
        {
            '$match': {
                # 'dividend': True,
                'account_to': account_number
            }
        }, {
            '$group': {
                '_id': '$account_from', 
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
                'Issuer_ID': '$_id',
                'total': 1
            }
        }, {
            '$out': 'totalDividends'
        }
    ]

    results = collection.aggregate(pipeline)
    
    for result in results:
        print(result)

    closeConnection(conn)

# aggregateTotalDividends("1-05072021-1")
# displayAggTotalDiv()

# Get total dividends history of a client & total per issuer. Sorted from least to most.
def aggregateTotalDividendsAsc():
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
                'issuer': '$account_from',
                'total': {
                    '$sum': '$amount'
                }
            }
        }, {
            '$sort': {
                'total': 1
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
    
    for result in results:
        print(result)

    closeConnection(conn)


# Get total dividends history of a client & total per time period. Sorted from most recent to latest. (FOR CHECKING)
def aggregateDividendsByTime():
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
                'date': '$transaction_date',
                'total': {
                    '$sum': '$amount'
                }
            }
        }, {
            '$sort': {
                'date': -1
            }
        }, {
            '$project': {
                '_id': 0,
                'date': 1,
                'total': 1
            }
        }, {
            '$out': 'dividendsByTime'
        }
    ]

    results = collection.aggregate(pipeline)
    
    for result in results:
        print(result)

    closeConnection(conn)


# Get shares owned by a customer. Sort by most to least shares.
def aggregateSharesByCustomerDesc():
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
    
    for result in results:
        print(result)

    closeConnection(conn)


# Get shares owned by a customer. Sort by least to most shares.
def aggregateSharesByCustomerAsc():
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
                'total': 1
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
    
    for result in results:
        print(result)

    closeConnection(conn)


# Get shares owned by all customers. Grouped by city - not sure if correct ?
def aggregateSharesByCity():
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
    
    for result in results:
        print(result)

    closeConnection(conn)


# Get total balance accross accounts by currency
def aggregateTotalBalanceByCurrency():
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
    
    for result in results:
        print(result)

    closeConnection(conn)


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
    
    for result in results:
        print(result)

    closeConnection(conn)
