from utils import *
from pymongo import *
from datetime import datetime
import random as r

# PART 1: READ FUNCTIONS
# TESTED ONES:
    # readAccount("0-04022023-0")
    # readCustomer(2)
    # readCustomers()
    # readClientAccs()
    # readCustomerAccs(4)
    # readCustomerAccs(979)
    # readDividendHistory("648-18092017-1", 2024, 10, 1, 2024, 10, 31)

# PART 2: AGG PIPELINES
# TESTED ONES:
    # aggregateTotalDividends()
    # displayAggTotalDiv()

# TESTED: Get certain account
def readAccount(account_no):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    result = collection.find_one({ 'account_number': account_no })
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

# TESTED: Retrieve one customer
def readCustomer(customer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['customer']
    result = collection.find_one({ 'customer_id': customer_id })
    if result:
        print(' ')
        customer_id = result['customer_id']
        date_opened = result['date_opened']
        address = result['address']
        birthdate =  result['birthdate']
        name = result['first_name'] + ' ' + result['last_name']
        print(f'Customer ID: { customer_id }')
        print(f'Customer Name: { name }')
        print(f'Date Opened: { date_opened }')
        print(f'Address: { address }')
        print(f'Birthdate: { birthdate }')
    closeConnection(conn)

# TESTED: Retrieve one issuer
# ADD PRINTS
def readIssuer(issuer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['issuer']
    result = collection.find(
        {'issuer_id': issuer_id}
    )
    return [i for i in result][0]

# TESETED: Read shares
# ADD PRINTS
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

# TESTED: Retrieve all customers
def readCustomers():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['customer']
    results = collection.find()
    for result in results:
        print(' ')
        customer_id = result['customer_id']
        date_opened = result['date_opened']
        address = result['address']
        birthdate =  result['birthdate']
        name = result['first_name'] + ' ' + result['last_name']
        print(f'Customer ID: { customer_id }')
        print(f'Customer Name: { name }')
        print(f'Date Opened: { date_opened }')
        print(f'Address: { address }')
        print(f'Birthdate: { birthdate }')
    closeConnection(conn)

# TESTED: Get all dividend-earning accounts (client accounts)
def readClientAccs():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.find({ 'clientAcc': 1 })
    for result in results:
        print(' ')
        customer_id = result['customer_id']
        account_number = result['account_number']
        account_type = result['account_type']
        balance = result['balance']
        date_created = result['date_created']
        address = result['address']
        currency =  result['currency']
        orders = result['orders']
        selling = result['selling']
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
    closeConnection(conn)

# TESTED: Get all accounts of a customer
def readCustomerAccs(customer_id):
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']
    results = collection.find({ 'customer_id': customer_id })

    for result in results:
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

    closeConnection(conn)

# TESTED: Get all dividends paid within October 2024
# ADD DIVIDEND:TRUE in find
def readDividendHistory(account_number, start_year, start_month, start_day, end_year, end_month, end_day):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['transaction']
    results = collection.find(
        {   'account_to': account_number,
            # 'dividend': 1,
            'transaction_date': {
                '$gte': datetime(int(start_year), int(start_month), int(start_day)), 
                '$lte': datetime(int(end_year), int(end_month), int(end_day))
                },
        })
    for result in results:
        print(' ')
        account_from = result['account_from']
        account_to = result['account_to']
        amount = result['amount']
        transaction_date = result['transaction_date']
        reference_number = result['reference_number']
        # share_id = result['share_id']
        # dividend = result['dividend']
        print(f'Account from: { account_from }')
        print(f'Account to: { account_to }')
        print(f'Amount: { amount }')
        print(f'Transaction Date: { transaction_date }')
        print(f'Reference No.: { reference_number }')
    closeConnection(conn)

# Get accounts more than a year ago 
# accoutn 648-18092017-1 appears twice so it's good for demo
# readDividendHistory("648-18092017-1", 2024, 10, 1, 2024, 10, 31)
# Read inactive accounts (more than a year)
# ADD PRINT
def readInactiveAcc():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    one_year_ago = datetime.now() - timedelta(days=365)
    results = collection.find({ 'last_activity_date': { '$lt': one_year_ago } })
    for result in results:
        print(result)
    closeConnection(conn)

# Get accounts exceeding threshold
# ADD PRINT
def readExceedingAcc(threshold):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.find({ 'balance': { '$gt': threshold } }) # can modify 
    for result in results:
        print(result)
    closeConnection(conn)

# Get accounts sorted by balance
# ADD PRINT
def readAccBalance():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.find().sort('balance', -1)
    for result in results:
        print(result)
    closeConnection(conn)

# Get overdue shares
# ADD PRINT
def readOverdueShares():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.find({ 'status': 'overdue' })
    for result in results:
        print(result)
    closeConnection(conn)

# Get next (unpaid) dividend payments
# ADD PRINT
def readUnpaidDividends():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.find({ 'status': 'Unpaid' })
    for result in results:
        print(result)
    closeConnection(conn)

def readAggTotalDiv():
    conn = openConnection()
    db = conn['Bank112']
    collection_totalDividends = db['totalDividends']
    results_tD = collection_totalDividends.find()
    for result in results_tD:
        print(' ')
        Issuer_ID = result['Issuer_ID']
        total = result['total']
        print(f'Issuer ID: { Issuer_ID }')
        print(f'Total: { total }')
    closeConnection(conn)


# AGGREGATION PIPELINES

# TESTED Get total dividends history of a client & total per issuer.
def aggregateTotalDividends(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['transaction']

    pipeline = [
        {
            '$match': {
                'dividend': 1,
                'account_to': account_number
            }
        }, {
            '$group': {
                '_id': '$account_from', 
                'totEal': {
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
                'total': 1,
                'account_to': account_number
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

# Get total dividends history of a client & total per time period. Sorted from most recent to latest. (FOR CHECKING)
def aggregateDividendsByTime(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['transaction']

    pipeline = [
        {
            '$match': {
                'dividend': True,
                'account_to': account_number
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
def aggregateSharesByCustomerDesc(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$match': {
                'account_number': account_number
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
def aggregateSharesByCustomerAsc(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$match': {
                'account_number': account_number
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
                'localField': 'issuer_id',
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
        }, {
            '$out': 'sharesByCity'
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
        }, {
            '$out': 'totalBalanceByCurrency'
        }
    ]

    results = collection.aggregate(pipeline)
    
    for result in results:
        print(result)

    closeConnection(conn)



