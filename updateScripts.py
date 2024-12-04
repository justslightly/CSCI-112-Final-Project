from utils import *
from readScripts import *

# UPDATE (FOR REVIEW)


# Marking shares that are past due dates in dividends. Unmarking those that are not.
def updateSharesOverdue():
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
    closeConnection(conn)

# Update shares for a given issuer and client
def updateSharesCount(account_number, issuer_id, count):
    print("updateSharesCount")
    conn = openConnection()
    db = conn['Bank112']
    issuer = db['issuer']
    shares = db['shares']
    print("Issuer Before: ", readIssuer(issuer_id))

    issuer_result = issuer.update_one(
        {'issuer_id': issuer_id},
        {'$inc':{'sold_shares':count}}
    )
    print("Issuer After : ", readIssuer(issuer_id))
    print("Shares Before: ", readShares(account_number, issuer_id))
    shares_result = shares.update_one(
        {
            'account_number': account_number, 
            'issuer_id': issuer_id
        },{
            '$inc':{'total_owned':count}
        }
    )
    print("Shares After : ", readShares(account_number, issuer_id))
    closeConnection(conn)
    
    
# Updating due date for shares of a certain issuer.
def updatetDueDate():
    conn = openConnection()
    # because not all shares earn dividends ?
    db = conn['Bank112']
    collection = db['shares']
    
    results = collection.update_many(
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
    )
    
    for result in results:
            print(result)

    closeConnection(conn)

