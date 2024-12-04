from utils import *
from pymongo import *
from datetime import datetime
import random as r

# Get certain account
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
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Balance: { balance }')
        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')

# Retrieve one customer
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

# Retrieve one issuer
def readIssuer(issuer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['issuer']
    result = collection.find_one(
        {'issuer_id': int(issuer_id)}
    )
    return result

# Read shares
def readShares(account_number, issuer_id):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']  
    if account_number == "": 
        result = collection.find_one(
            {
                'issuer_id': int(issuer_id)
            }
        )
    else: 
        result = collection.find_one(
            {
                'account_number': account_number,
                'issuer_id': int(issuer_id)
            }
        )
    return result

# Retrieve all customers
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

# Get all dividend-earning accounts (client accounts)
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
        orders = result['orders']
        selling = result['selling']
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Balance: { balance }')
        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')
    closeConnection(conn)

# Get all accounts of a customer
def readCustomerAccs(customer_id):
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['account']
    results = collection.find({ 'customer_id': int(customer_id) })

    for result in results:
        print(' ')
        customer_id = result['customer_id']
        account_number = result['account_number']
        account_type = result['account_type']
        balance = result['balance']
        date_created = result['date_created']
        address = result['address']
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Balance: { balance }')

        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')

    closeConnection(conn)

# Get all dividends paid within October 2024
def readDividendHistory(account_number, start_year, start_month, start_day, end_year, end_month, end_day):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['transaction']
    results = collection.find(
        {   'account_to': account_number,
            'dividend': 1,
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
        print(f'Account from: { account_from }')
        print(f'Account to: { account_to }')
        print(f'Amount: { amount }')
        print(f'Transaction Date: { transaction_date }')
        print(f'Reference No.: { reference_number }')
    closeConnection(conn)

# Get accounts exceeding threshold
def readExceedingAcc(threshold):
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.find({ 'balance': { '$gt': int(threshold) } }) 
    for result in results:
        print(' ')
        customer_id = result['customer_id']
        account_number = result['account_number']
        account_type = result['account_type']
        balance = result['balance']
        date_created = result['date_created']
        address = result['address']
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Balance: { balance }')
        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')
    closeConnection(conn)

# Get accounts sorted by balance
def readAccBalance():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['account']
    results = collection.find().sort('balance', -1)
    for result in results:
        print(' ')
        customer_id = result['customer_id']
        account_number = result['account_number']
        account_type = result['account_type']
        balance = result['balance']
        date_created = result['date_created']
        address = result['address']
        print(f'Customer ID: { customer_id }')
        print(f'Account No: { account_number }')
        print(f'Account Type: { account_type }')
        print(f'Date Created: { date_created }')
        print(f'Address: { address }')
        print(f'Balance: { balance }')
        if result['clientAcc'] == 1:
            orders = result['orders']
            selling = result['selling']
            print(f'Orders: { orders }')
            print(f'Selling: { selling }')
    closeConnection(conn)

# Get overdue shares
# ADD PRINT
def readOverdueShares():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.find({ 'status': 'overdue' })
    for result in results:
        print(f"Account Number: {result['account_number']}")
        print(f"Issuer ID: {result['issuer_id']}")
        print(f"Share ID: {result['share_id']}")
        print(f"Total Owned: {result['total_owned']}")
        print(f"Due date: {result['due_date']}")
        print(f"Status: {result['status']}")
    closeConnection(conn)

# Get next (unpaid) dividend payments
def readUnpaidDividends():
    conn = openConnection()
    db = conn['Bank112']
    collection = db['shares']
    results = collection.find({ 'status': 'unpaid' })
    for result in results:
        print(f"Account Number: {result['account_number']}")
        print(f"Issuer ID: {result['issuer_id']}")
        print(f"Share ID: {result['share_id']}")
        print(f"Total Owned: {result['total_owned']}")
        print(f"Due date: {result['due_date']}")
        print(f"Status: {result['status']}")
    closeConnection(conn)

# AGGREGATION PIPELINES

# Get total dividends history of a client & total per issuer.
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
                'total': 1,
                'account_to': account_number
            }
        }, {
            '$out': 'totalDividends'
        }
    ]

    results = collection.aggregate(pipeline)
    
    collection = db['totalDividends']
    results = collection.find()
    for result in results:
        print(f'Issuer ID:  { result['Issuer_ID'] }')
        print(f'Shares Total: { result['total']}')

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
                'dividend': 1, 
                'account_to': account_number
            }
        }, {
            '$group': {
                '_id': '$transaction_date', 
                'total': {
                    '$sum': '$amount'
                }
            }
        }, {
            '$sort': {
                '_id': -1
            }
        }, {
            '$project': {
                '_id': 1, 
                'total': 1
            }
        }, {
            '$out': 'dividendsByTime'
        }
    ]

    results = collection.aggregate(pipeline)
    
    collection = db['dividendsByTime']
    results = collection.find()
    for result in results:
        print(f'Time:  { result['_id'] }')
        print(f'Sum of Dividends: { result['total'] }')

    closeConnection(conn)


# Get shares owned by a customer. Sort by most to least shares.
def aggregateSharesByCustomer(account_number):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$lookup': {
                'from': 'issuer', 
                'localField': 'issuer_id', 
                'foreignField': 'issuer_id', 
                'as': 'issuer_data'
            }
        }, {
            '$match': {
                'account_number': account_number
            }
        }, {
            '$unwind': '$issuer_data'
        }, {
            '$group': {
                '_id': '$issuer_data.name', 
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
    
    collection = db['sortedSharesofAcc']
    results = collection.find()
    for result in results:
        print(f'Issuer Name:  { result['_id'] }')
        print(f'Shares Total: { result['total']}')
    closeConnection(conn)

# Get shares owned by all customers. Grouped by city - not sure if correct ?
def aggregateSharesByCity():
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
        {
            '$lookup': {
                'from': 'account',
                'localField': 'account_number',
                'foreignField': 'account_number',
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
    
    collection = db['sharesByCity']
    results = collection.find()
    count=0
    for result in results:
        if count > 10:
            break
        print(f'City:  { result['_id'] }')
        print(f'Shares Total: { result['total_shares']}')
        count += 1

    closeConnection(conn)
