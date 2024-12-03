from utils import *

# READ

from pymongo import *
from datetime import datetime


def openConnection():
   url = 'mongodb+srv://test_user1:123@csci112-group4.mu6gp.mongodb.net/'
   conn = MongoClient(url)
   return conn


def closeConnection(conn):
   conn.close()


conn = openConnection()
db = conn['location']



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

        # print(result)


    closeConnection(conn)

# readCustomers()

# TESTED: Get all dividend-earning accounts (client accounts) ; TESTED
def readAllClients():
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
        # print(result)

    closeConnection(conn)

# readAllClients()

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

# readCustomerAccs(4)
# readCustomerAccs(979)

# TESTED: Get all dividends paid within October 2024
def readDividendsInMonthYear(month, year):
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['transaction']

    results = collection.find({ 'transaction_date': {'$gte': datetime(int(year), int(month), 1), '$lte': datetime(int(year), int(month), 30)} })

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

# readDividendsInMonthYear(9, 2024)


# TESTED: Get certain account
def readOneAcc(account_no):
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
        # print(result)

# readOneAcc("0-04022023-0")

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


# Get shares owned by all customers. Grouped by city.
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


