from utils import *

# READ

# Retrieve customers
def readCustomers():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['customer']

    results = collection.find()

    for result in results:
        print(result)

    closeConnection(conn)


# Read issuer
def readIssuer(issuer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['issuer']

    result = collection.find(
        {'issuer_id': issuer_id}
    )
    return [i for i in result][0]


# Read shares
def readShares(account_number, issuer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']   

    result = collection.find(
        {
            'account_number': account_number,
            'issuer_id': issuer_id
        }
    )
    return [i for i in result][0]


# Get all dividend-earning accounts (client accounts)
def readAllClientAccs():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'clientAcc': 1 })

    for result in results:
        print(result)

    closeConnection(conn)


# Get all accounts of a customer
def readAllClientAccs():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'customer_id': 4 })

    for result in results:
        print(result)

    closeConnection(conn)


# Get all dividends paid within October 2024
def readDividendsInOct2024():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['transaction']

    results = collection.find({ 'dividend': True, 'date_time': {'$gte': (2024, 10, 1), '$lte': (2024, 10, 31)} })

    for result in results:
        print(result)

    closeConnection(conn)


# Get certain account
def readOneAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    results = collection.find_one({ 'account_number': "0-20170209" })

    closeConnection(conn)

    if result:
        print(' ')
        customer_id = result['customer_id']
        account_number = result['account_number']
        account_type = result['account_type']
        balance = result['balance']
        date_created = result['date_created']
        address = result['address']
        currency =  result['currency']
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Currency: { currency }')
        print(f'Balance: { balance }')

        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')
        # print(result)

# readOneAcc("0-04022023-0")

# Get accounts more than a year ago 
def getInactiveAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    one_year_ago = datetime.now() - timedelta(days=365)

    results = collection.find({ 'last_activity_date': { '$lt': one_year_ago } })

    for result in results:
        print(result)

    closeConnection(conn)


# Get accounts exceeding threshold
def readExceedingAcc():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['account']

    results = collection.find({ 'balance': { '$gt': 500000 } }) # can modify 

    for result in results:
        print(result)

    closeConnection(conn)


# Get accounts sorted by balance
def readAccBalance():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']

    results = collection.find().sort('balance', -1)

    for result in results:
        print(result)

    closeConnection(conn)


# Get overdue shares
def readOverdueShares():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['shares']

    results = collection.find({ 'status': 'overdue' })

    for result in results:
        print(result)

    closeConnection(conn)


# AGGREGATION PIPELINES

# Get total dividends history of a client & total per issuer. Sorted from most to least.
def aggregateTotalDividends():
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


# Get total dividends history of a client & total per issuer. Sorted from least to most.
def aggregateTotalDividends():
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


# Get shares owned by a customer. Sort by most to least shares.
def aggregateSharesByCustomer():
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


# Get shares owned by a customer. Sort by least to most shares.
def aggregateSharesByCustomer():
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
