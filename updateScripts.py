from utils import *

# UPDATE (FOR REVIEW)

# Updating due date for shares of a certain issuer.
def updateSharesDueDate():
    conn = openConnection()

    # because not all shares earn dividends ? (!)

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
