from utils import *

# DELETE (FOR REVIEW)

# Deleting shares documents without any shares owned
def deleteEmptyShares():
    conn = openConnection()
    
    db = conn['Bank112']
    collection = db['shares']

    results = collection.delete_many({ 'total_owned': 0 })

    print(results)

    closeConnection(conn)
