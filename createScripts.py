from pymongo import *
from datetime import *
from utils import *
import random as r

firstName = [
    "John","Mark","Michael","James","Christian","Joseph","Daniel","Joshua","Anthony",
    "Francis","Maria","Angeline","Jasmine","Patricia","Nicole","Grace","Anne",
    "Catherine","Theresa","Rafael","Gabriel","Alexander","Paul","Edward","Andrew",
    "Carlo","Kevin","Ryan","Diana","Samantha"
]
lastName = [
    "Garcia","Reyes","Cruz","Bautista","Villanueva","Santos","Flores","Gonzales",
    "Rivera","Martinez","Dela Cruz","Torres","Ramos","Mendoza","Lopez","Hernandez",
    "Castillo","Morales","Domingo","Aguilar","Navarro","Gutierrez","Pascual","Aquino",
    "Vargas","Velasco","Rosales","Padilla","Luna","Salazar"
]
cities_countries = [ 
    "New York, United States","Los Angeles, United States",
    "Chicago, United States","Houston, United States","Phoenix, United States",
    "London, United Kingdom","Manchester, United Kingdom","Birmingham, United Kingdom",
    "Toronto, Canada","Vancouver, Canada","Montreal, Canada","Sydney, Australia",
    "Melbourne, Australia","Brisbane, Australia","Tokyo, Japan","Osaka, Japan",
    "Nagoya, Japan","Shanghai, China","Beijing, China","Guangzhou, China",
    "Paris, France","Lyon, France","Marseille, France","Berlin, Germany",
    "Munich, Germany","Frankfurt, Germany","Madrid, Spain","Barcelona, Spain",
    "Valencia, Spain","Rome, Italy","Milan, Italy","Naples, Italy",
    "Seoul, South Korea","Busan, South Korea","Incheon, South Korea",
    "Mumbai, India","Delhi, India","Bangalore, India","São Paulo, Brazil",
    "Rio de Janeiro, Brazil","Brasilia, Brazil","Mexico City, Mexico",
    "Guadalajara, Mexico","Monterrey, Mexico","Moscow, Russia",
    "Saint Petersburg, Russia","Novosibirsk, Russia","Dubai, United Arab Emirates",
    "Abu Dhabi, United Arab Emirates","Sharjah, United Arab Emirates","Istanbul, Turkey",
    "Ankara, Turkey","Izmir, Turkey","Johannesburg, South Africa",
    "Cape Town, South Africa","Durban, South Africa","Buenos Aires, Argentina",
    "Córdoba, Argentina","Rosario, Argentina","Cairo, Egypt","Alexandria, Egypt",
    "Giza, Egypt","Nairobi, Kenya","Mombasa, Kenya","Kisumu, Kenya","Bangkok, Thailand",
    "Chiang Mai, Thailand","Pattaya, Thailand","Jakarta, Indonesia",
    "Surabaya, Indonesia","Bandung, Indonesia","Kuala Lumpur, Malaysia",
    "George Town, Malaysia","Ipoh, Malaysia","Singapore, Singapore",
    "Manila, Philippines","Quezon City, Philippines","Cebu City, Philippines",
    "Davao City, Philippines","Hanoi, Vietnam","Ho Chi Minh City, Vietnam",
    "Da Nang, Vietnam","Karachi, Pakistan","Lahore, Pakistan","Islamabad, Pakistan",
    "Riyadh, Saudi Arabia","Jeddah, Saudi Arabia","Mecca, Saudi Arabia","Tehran, Iran",
    "Mashhad, Iran","Isfahan, Iran","Baghdad, Iraq","Basra, Iraq","Erbil, Iraq",
    "Lagos, Nigeria","Abuja, Nigeria","Kano, Nigeria","Addis Ababa, Ethiopia",
    "Dire Dawa, Ethiopia","Mekelle, Ethiopia"
]
type_account = ['Savings', 'Checking']
issuers = ["Nexora Innovations","AetherTech Solutions","Vertex Dynamics",
    "Quantum Synergy Inc.","StellarEdge Systems","EcoVista Enterprises",
    "CloudCrest Technologies","LuminaCore Solutions","BlueNova Ventures",
    "ZenithSphere Global","BrightWave Industries","OmniAxis Corporation",
    "PulsePoint Analytics","UrbanLoom Developments","ElevateWorks Group",
    "Greenstream Partners","InfiniteTrail Logistics","SummitForge Enterprises",
    "VividLink Communications","PrimeHorizon Labs"
]
start_date = datetime(2024, 12, 4)
def randomDue(start_date, num_dates, max_days=30):
    due_dates = []    
    for _ in range(num_dates):
        random_days = r.randint(1, 30)
        due_date = start_date + timedelta(days=random_days)
        due_dates.append(datetime.strptime(due_date.strftime("%Y-%m-%d"),"%Y-%m-%d"))
    return due_dates

due_dates = randomDue(start_date, 20)

def createCustomer():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['customer']
    print(f'Connected to database: {collection.name}')
    print(f'Creating Customers')
    ctr = 0
    while ctr < 1000:
        fnameIndex = r.randint(0,29)
        lnameIndex = r.randint(0,29)
        year = r.randint(2016, 2024)
        month = r.randint(1,11)
        address = r.randint(0,99)        
        if month in [1,3,5,7,8,10,12]:
            day = r.randint(1,31)
        elif month == 2:
            day = r.randint (1,28)
        else:
            day = r.randint(1,30)        
        openDate = datetime(year, month, day)
        byear = r.randint(1945, 2006)
        bmonth = r.randint(1,12)
        if bmonth in [1,3,5,7,8,10,12]:
            bday = r.randint(1,31)
        elif bmonth == 2:
            bday = r.randint (1,28)
        else:
            bday = r.randint(1,30)
        birthdate = datetime(byear, bmonth, bday)
        finalDoc = {
            'customer_id': ctr,
            'first_name': firstName[fnameIndex],
            'last_name': lastName[lnameIndex],
            'date_opened': openDate,
            'address': cities_countries[address],
            'birthdate': birthdate
        }
        collection.insert_one(finalDoc)
        if ctr % 100 == 0:
            print(f"Progress: {ctr}/1000 documents inserted...")
        ctr += 1
    closeConnection(conn)

def createAccount():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['customer']
    acct_col = db['account']
    print(f'Connected to database: {db.name}')
    print(f'Creating accounts')
    ctr = 0
    customers = collection.find()
    for customer in customers:
        acct_type = r.randint(0,1)
        balance = r.randint(1000,10000000)
        curr = r.randint(0,9)
        clientAcc = r.randint(0,1)
        id = customer['date_opened'].strftime('%d%m%Y')
        account_document = {
                "customer_id": customer["customer_id"],  
                "account_number": f'{customer["customer_id"]}-{id}-{clientAcc}',
                "account_type": type_account[acct_type],
                "balance":balance,
                "date_created": customer["date_opened"],
                "address": customer["address"],
                "clientAcc":clientAcc
            }
        acct_col.insert_one(account_document)
        if ctr % 100 == 0:
            print(f"Progress: {ctr}/1000 documents inserted...")
        ctr+=1
    closeConnection(conn)

def createIssuer():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['issuer']
    print(f'Connected to database: {db.name}')
    print('Adding Issuers')
    ctr = 0 
    while ctr < 20:
        dividendFreq = r.randint(0,3)
        totalShares = r.randint(1000000,10000000)
        sold_shares = r.randint(0,int(totalShares/2))
        cost_per_share = r.uniform(0,4)
        finalDoc = {
            'issuer_id': ctr,
            'name': issuers[ctr],
            'total_shares': totalShares,
            'sold_shares': sold_shares,
            'cost_per_share': cost_per_share
        }
        collection.insert_one(finalDoc)
        ctr += 1

    closeConnection(conn)

def addOrders():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    print('Adding Order List')
    pipeline = [
        {
            '$match':{
                'clientAcc':1
            }
        }
    ]
    clientAccounts = collection.aggregate(pipeline)
    for i in clientAccounts:
        ord_ctr = 0
        num_order = r.randint(1, 4)
        issuer_rand = r.sample(range(0, 19), num_order)
        orderList = []  
        while ord_ctr < len(issuer_rand):  
            addOrder = {'issuer_id': issuer_rand[ord_ctr]}
            orderList.append(addOrder)  
            ord_ctr += 1  
        collection.update_one(
            {'customer_id':i['customer_id']},
            {'$set':
                {
                    'orders': orderList
                }
            }
        )

    closeConnection(conn)

def addSelling():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    print("Adding Selling List")
    pipeline = [
        {
            '$match':{
                'clientAcc':1
            }
        }
    ]
    clientAccounts = collection.aggregate(pipeline)
    for i in clientAccounts:
        sell_ctr = 0
        num_sell = r.randint(1, 2)
        issuer_rand = r.sample(range(0, 19), num_sell)
        sellList = []  
        while sell_ctr < len(issuer_rand):  
            addOrder = {'issuer_id': issuer_rand[sell_ctr]}
            sellList.append(addOrder)  
            sell_ctr += 1  
        collection.update_one(
            {'customer_id':i['customer_id']},
            {'$set':
                {
                    'selling': sellList
                }
            }
        )
    closeConnection(conn)

def createShares():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    shares_col = db['shares']
    print('Creating Shares')
    pipeline = [
        {
            "$match": {
                "orders": { "$exists": True, "$ne": [] }
            }   
        },{
            "$unwind": "$orders"
        },{
            "$project": {
                "_id":0,
                "account_number": 1,
                "issuer_id": "$orders.issuer_id",
                }
        },{
            '$out': { 'db': 'Bank112', 'coll': 'shares' }
        }
    ]
    collection.aggregate(pipeline)
    cursor = shares_col.find()
    for document in cursor:
        rtotal = r.randint(100,4000)
        shares_col.update_one(
            {'_id':document['_id']},
            {'$set':
                {'total_owned': rtotal,
                 'share_id': str(r.randint(1000,10000)) + "-" + str(r.randint(1000,10000))},
            }
        )
    #adding due dates for each issuer (same issuer = same date)
    n = 0
    status = "Unpaid"
    while n < 20:
        shares_col.update_many(
            {'issuer_id':n},
            {'$set':
                {'due_date':due_dates[n],
                 'status':"Unpaid"}
            }
        )
        n+=1

    closeConnection(conn)

def createDivTransaction():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['shares']
    transac_col = db['transaction']
    print('Creating Transactions')
    pipeline = [
        {
            "$match": {
                "status": { "$exists": True, "$ne": [] }
            }   
        },{
            '$lookup':{
                "from": "issuer", 
                "localField": "issuer_id", 
                "foreignField": "issuer_id",  
                "as": "divInfo"
            }
        },{
            '$unwind':'$divInfo'
        },{
            '$project':{
                '_id':0,
                'account_from':'$issuer_id',
                'account_to':'$account_number',
                'amount':{'$multiply':['$divInfo.cost_per_share','$total_owned']},
                "transaction_date": {
                    "$dateSubtract": {  
                        "startDate": "$due_date",
                        "unit": "month",
                        "amount": 3
                    }
                },
                "reference_number": {
                    "$concat": [
                        { "$toString":"$account_number"}, "-",
                        { "$toString": "$divInfo.issuer_id" }, "-",
                        { "$dateToString": { 
                            "format": "%Y%m%d", 
                            "date": {"$dateSubtract": 
                                {  
                                "startDate": "$due_date",
                                "unit": "month",
                                "amount": 3
                                }
                            } 
                        } 
                        }
                    ]
                },
                "dividend":{'$literal':1},
                "share_id":"$share_id",
            }
        },{
            '$out': { 'db': 'Bank112', 'coll': 'transaction' }
        }
    ]
    collection.aggregate(pipeline)

    closeConnection(conn)

def createTransaction():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['transaction']
    acct_col = db['account']
    accounts = list(acct_col.find())
    account_number = [item['account_number'] for item in accounts]
    ctr = 0
    while ctr < 500:
        fromRand = r.randint(0,999)
        toRand = r.randint(0,999)
        amountRand = r.randint(1000,100000)
        year = r.randint(2023, 2024)
        month = r.randint(1,11)
        if month in [1,3,5,7,8,10,12]:
            day = r.randint(1,31)
        elif month == 2:
            day = r.randint (1,28)
        else:
            day = r.randint(1,30)
        transactionDate = datetime(year, month, day)
        acctFrom = account_number[fromRand]
        acctTo = account_number[toRand]
        refNo = str(acctFrom) + "-" + str(acctTo) + transactionDate.strftime("%Y%m%d")
        document = {
            "account_from": acctFrom,
            "account_to": acctTo,
            "amount": amountRand,
            "transaction_date": transactionDate,
            "reference_number": refNo,
            "dividend":0
        }
        collection.insert_one(document)
        if ctr % 100 == 0:
            print(f"Progress: {ctr}/500 documents inserted...")
        ctr += 1

def oneCustomer(NameFirst, NameLast, City, BirthYear, BirthMonth, BirthDay):
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['customer']

    print(f'connected to {db.name}')

    lastID = collection.find_one(sort=[("customer_id", -1)])["customer_id"]

    newID = lastID+1

    finalDoc = {
    'customer_id': newID,
    'first_name': NameFirst,
    'last_name': NameLast,
    'date_opened': datetime.now(),
    'address': City,
    'birthdate': datetime(int(BirthYear),int(BirthMonth), int(BirthDay))
    }

    collection.insert_one(finalDoc)

    result = collection.find_one({'customer_id':newID})

    print(f"Customer ID: {result['customer_id']}")
    print(f"First Name: {result['first_name']}")
    print(f"Last Name: {result['last_name']}")
    print(f"Date Opened: {result['date_opened']}")
    print(f"Address: {result['address']}")
    print(f"Birthdate: {result['birthdate']}")

    closeConnection(conn)

def oneAccount(customerID, AcctType, clientAcct, Balance,Address):
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['customer']

    custID = collection.find_one({"customer_id":customerID})["customer_id"]

    final_doc = {
        "customer_id": customerID,
        "account_number": str(custID) + "-" + datetime.now().strftime("%Y%m%d") + "-" + str(clientAcct),
        "account_type": AcctType,
        "balance": Balance,
        "date_created": datetime.now(),
        "address": Address,
        "clientAcc": clientAcct
        }

    collection.insert_one(final_doc)

    result = collection.find_one({'customer_id':customerID})
    print(f"Customer ID: {result['customer_id']}")
    print(f"Account Number: {result['account_number']}")
    print(f"Account Type: {result['account_type']}")
    print(f"Balance: {result['balance']}")
    print(f"Date Created: {result['date_created']}")
    print(f"Address: {result['address']}")
    print(f"Client Account: {result['clientAcc']}")
    closeConnection(conn)
    
def oneOrder(customerID,issuerID,numShares, AcctNumber, DueDate):
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    shares_col = db['shares']

    collection.update_one(
        {"customer_id":customerID},
        {"$set":
         {
             'orders': issuerID
         }
        }
    )

    final_doc = {
        "account_number": AcctNumber,
        "issuer_id": issuerID,
        "total_owned": numShares,
        "due_date": DueDate,
        "status":"Unpaid"

    }

    shares_col.insert_one(final_doc)

def oneSelling(customerID,issuerID):
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    shares_col = db['shares']

    collection.update_one(
        {"customer_id":customerID},
        {"$set":
         {
             'selling': issuerID
         }
        }
    )

def oneTransaction(acctFrom,acctTo,amount,div):
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['transaction']

    transactiondate = datetime.now()

    final_doc = {
    "account_from":acctFrom,
    "account_to":acctTo,
    "transaction_date": transactiondate,
    "reference_number": str(acctFrom) +  "-" + str(acctTo) + "-" + transactiondate.strftime("%Y%m%d"),
    "amount": amount,
    "dividend": div,
    "share_id": str(acctTo) + "-" + transactiondate.strftime("%Y%m%d"),

    }

    collection.insert_one(final_doc)
