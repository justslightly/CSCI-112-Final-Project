from utils import *
from readScripts import *

# UPDATE (FOR REVIEW)


# Marking shares that are past due dates in dividends. Unmarking those that are not.
def updateSharesOverdue(due_year, due_month, due_day):
    conn = openConnection()

    db = conn['Bank112']
    collection = db['shares']

    pipeline = [
    	# set overdue to overdue ones
        {
            '$match': {
                'due_date': {'$exists': True, '$lt': (int(due_year), int(due_month), int(due_day))}
            }
        }, {
            '$set': {
                'status': 'overdue'
            }
        }, 

        # set status 'today' to dividends due today
        {	
            '$match': {
                'due_date': {'$exists': True, '$eq': (int(due_year), int(due_month), int(due_day))}
            }
        }, {
            '$set': {
                'status': 'today'            
            }
        }, 

        # remove status for dividends not urgent
        {	
            '$match': {
                'due_date': {'$exists': True, '$gt': (int(due_year), int(due_month), int(due_day))}
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

# Update shares for a given issuer and client
def updateSharesCount(account_number, issuer_id, count):
    print("updateSharesCount")
    conn = openConnection()
    db = conn['Bank112']
    issuer = db['issuer']
    shares = db['shares']
    print("Issuer Sold Shares Before: ", readIssuer(int(issuer_id))['sold_shares'])

    issuer_result = issuer.update_one(
        {'issuer_id': int(issuer_id)},
        {'$inc':{'sold_shares':int(count)}}
    )
    print("Issuer Sold Shares After : ", readIssuer(int(issuer_id))['sold_shares'])
    print("Shares Total Owned Before: ", readShares(account_number, int(issuer_id))['total_owned'])
    shares_result = shares.update_one(
        {
            'account_number': account_number, 
            'issuer_id': int(issuer_id)
        },{
            '$inc':{'total_owned':int(count)}
        }
    )
    print("Shares Total Owned After : ", readShares(account_number, int(issuer_id))['total_owned'])
    closeConnection(conn)
    
    
# Updating due date for shares of a certain issuer.
def updateSharesDueDate(issuer_id, due_year, due_month, due_day):
    conn = openConnection()
    # because not all shares earn dividends ?
    db = conn['Bank112']
    collection = db['shares']
    
    results = collection.update_many(
        {
            'issuer_id': int(issuer_id)
        }, {
            '$set': {
                'due_date': datetime(int(due_year), int(due_month), int(due_day))
            }
        }
    )
    closeConnection(conn)

