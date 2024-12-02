from pymongo import *
from datetime import *
from utilization import *
from insertData import *
import random as r

def createCustomer():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']

    print(f'Connected to database: {db.name}')

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
            '_id': ctr,
            'first_name': firstName[fnameIndex],
            'last_name': lastName[lnameIndex],
            'date_opened': openDate,
            'address': cities_countries[address],
            'birthdate': birthdate
        }

        collection.insert_one(finalDoc)

        ctr += 1

def createAccount():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['customer']
    acct_col = db['account']

    print(f'Connected to database: {db.name}')

    pipeline = [
    {
        "$lookup": {
            "from": "customer",             
            "localField": "customer_id",    
            "foreignField": "_id",         
            "as": "customer_data"           
        }
    },
    {
        "$unwind": "$customer_data"  
    },
    {
        "$addFields": {
            "account_number": {
                "$concat": [
                    { "$toString": "$customer_id" },  
                    "-",
                    { "$dateToString": { 
                        "format": "%Y%m%d", 
                        "date": "$customer_data.date_opened"  
                    }}
                ]
            }
        }
    },
    {
        "$project": {
            "address": "$customer_data.address",      
            "date_opened": "$customer_data.date_opened",
            "account_number": 1                   
        }
    }
]

    results = acct_col.aggregate(pipeline)

    for result in results:
        acct_col.update_one(
            {"_id": result["_id"]}, 
            {"$set": {
                "address": result["address"],              
                "date_opened": result["date_opened"],     
                "account_number": result["account_number"] 
            }}
        )

def editAccount():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']

    print(f'Connected to database: {db.name}')

    ctr = 0

    while ctr < 1000:

        acct_type = r.randint(0,1)
        balance = r.randint(1000,10000000)
        curr = r.randint(0,9)
        clientAcc = r.randint(0,1)        

        collection.update_one(
            {"customer_id":ctr},
            {"$set": {"account_type":type_account[acct_type],
                      "balance":balance,
                      "currency":currencies[curr],
                      "clientAcc": clientAcc
                      }}
                              
                              
        )

        ctr += 1

def createIssuer():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['issuer']

    print(f'Connected to database: {db.name}')
    
    ctr = 0 

    while ctr < 20:
        dividendFreq = r.randint(0,3)
        totalShares = r.randint(1000000,10000000)
        sold_shares = r.randint(0,int(totalShares/2))
        cost_per_share = r.uniform(0,4)

        finalDoc = {
            '_id': ctr,
            'name': issuers[ctr],
            'dividend': div_freq[dividendFreq],
            'total_shares': totalShares,
            'sold_shares': sold_shares,
            'cost_per_share': cost_per_share
        }

        collection.insert_one(finalDoc)

        ctr += 1

def addOrders():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']

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
        print(orderList)
        print(i['customer_id'])
        collection.update_one(
            {'customer_id':i['customer_id']},
            {'$set':
                {
                    'orders': orderList
                }
            }
        )

def addSelling():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']

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
            addOrder = {'issuer_id': issuer_rand[ord_ctr]}
            sellList.append(addOrder)  
            ord_ctr += 1  
        print(sellList)
        print(i['customer_id'])
        collection.update_one(
            {'customer_id':i['customer_id']},
            {'$set':
                {
                    'selling': sellList
                }
            }
        )

def createShares():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['account']
    
    
    pipeline = [
    {
    "$match": {
        "orders": { "$exists": True, "$ne": [] }
    }   
    },
    {
    "$unwind": "$orders"
    },
    {
    "$project": {
        "account_number": 1,
        "issuer_id": "$orders.issuer_id",
        }
    },
    {
    '$out': { 'db': 'Bank112', 'coll': 'shares' }
    }
    ]

    collection.aggregate(pipeline)

def editShares():
    conn = openConnection()
    db = conn['Bank112'] 
    collection = db['shares']

    cursor = collection.find()

    for document in cursor:
        rtotal = r.randint(100,4000)
        collection.update_one(
            {'_id':document['_id']},
            {'$set':{'total_owned': rtotal}}
        )


firstName = [
    "John",
    "Mark",
    "Michael",
    "James",
    "Christian",
    "Joseph",
    "Daniel",
    "Joshua",
    "Anthony",
    "Francis",
    "Maria",
    "Angeline",
    "Jasmine",
    "Patricia",
    "Nicole",
    "Grace",
    "Anne",
    "Catherine",
    "Theresa",
    "Rafael",
    "Gabriel",
    "Alexander",
    "Paul",
    "Edward",
    "Andrew",
    "Carlo",
    "Kevin",
    "Ryan",
    "Diana",
    "Samantha"
]

lastName = [
    "Garcia",
    "Reyes",
    "Cruz",
    "Bautista",
    "Villanueva",
    "Santos",
    "Flores",
    "Gonzales",
    "Rivera",
    "Martinez",
    "Dela Cruz",
    "Torres",
    "Ramos",
    "Mendoza",
    "Lopez",
    "Hernandez",
    "Castillo",
    "Morales",
    "Domingo",
    "Aguilar",
    "Navarro",
    "Gutierrez",
    "Pascual",
    "Aquino",
    "Vargas",
    "Velasco",
    "Rosales",
    "Padilla",
    "Luna",
    "Salazar"
]

cities_countries = [
"New York, United States",
"Los Angeles, United States",
"Chicago, United States",
"Houston, United States",
"Phoenix, United States",
"London, United Kingdom",
"Manchester, United Kingdom",
"Birmingham, United Kingdom",
"Toronto, Canada",
"Vancouver, Canada",
"Montreal, Canada",
"Sydney, Australia",
"Melbourne, Australia",
"Brisbane, Australia",
"Tokyo, Japan",
"Osaka, Japan",
"Nagoya, Japan",
"Shanghai, China",
"Beijing, China",
"Guangzhou, China",
"Paris, France",
"Lyon, France",
"Marseille, France",
"Berlin, Germany",
"Munich, Germany",
"Frankfurt, Germany",
"Madrid, Spain",
"Barcelona, Spain",
"Valencia, Spain",
"Rome, Italy",
"Milan, Italy",
"Naples, Italy",
"Seoul, South Korea",
"Busan, South Korea",
"Incheon, South Korea",
"Mumbai, India",
"Delhi, India",
"Bangalore, India",
"São Paulo, Brazil",
"Rio de Janeiro, Brazil",
"Brasilia, Brazil",
"Mexico City, Mexico",
"Guadalajara, Mexico",
"Monterrey, Mexico",
"Moscow, Russia",
"Saint Petersburg, Russia",
"Novosibirsk, Russia",
"Dubai, United Arab Emirates",
"Abu Dhabi, United Arab Emirates",
"Sharjah, United Arab Emirates",
"Istanbul, Turkey",
"Ankara, Turkey",
"Izmir, Turkey",
"Johannesburg, South Africa",
"Cape Town, South Africa",
"Durban, South Africa",
"Buenos Aires, Argentina",
"Córdoba, Argentina",
"Rosario, Argentina",
"Cairo, Egypt",
"Alexandria, Egypt",
"Giza, Egypt",
"Nairobi, Kenya",
"Mombasa, Kenya",
"Kisumu, Kenya",
"Bangkok, Thailand",
"Chiang Mai, Thailand",
"Pattaya, Thailand",
"Jakarta, Indonesia",
"Surabaya, Indonesia",
"Bandung, Indonesia",
"Kuala Lumpur, Malaysia",
"George Town, Malaysia",
"Ipoh, Malaysia",
"Singapore, Singapore",
"Manila, Philippines",
"Quezon City, Philippines",
"Cebu City, Philippines",
"Davao City, Philippines",
"Hanoi, Vietnam",
"Ho Chi Minh City, Vietnam",
"Da Nang, Vietnam",
"Karachi, Pakistan",
"Lahore, Pakistan",
"Islamabad, Pakistan",
"Riyadh, Saudi Arabia",
"Jeddah, Saudi Arabia",
"Mecca, Saudi Arabia",
"Tehran, Iran",
"Mashhad, Iran",
"Isfahan, Iran",
"Baghdad, Iraq",
"Basra, Iraq",
"Erbil, Iraq",
"Lagos, Nigeria",
"Abuja, Nigeria",
"Kano, Nigeria",
"Addis Ababa, Ethiopia",
"Dire Dawa, Ethiopia",
"Mekelle, Ethiopia"
]

type_account = ['Savings', 'Checking']

currencies = [
    "USD",  # United States Dollar
    "EUR",  # Euro
    "JPY",  # Japanese Yen
    "GBP",  # British Pound Sterling
    "AUD",  # Australian Dollar
    "CAD",  # Canadian Dollar
    "CHF",  # Swiss Franc
    "CNY",  # Chinese Yuan
    "HKD",  # Hong Kong Dollar
    "PHP"   # Philippine Peso
]


issuers = [
    "Nexora Innovations",
    "AetherTech Solutions",
    "Vertex Dynamics",
    "Quantum Synergy Inc.",
    "StellarEdge Systems",
    "EcoVista Enterprises",
    "CloudCrest Technologies",
    "LuminaCore Solutions",
    "BlueNova Ventures",
    "ZenithSphere Global",
    "BrightWave Industries",
    "OmniAxis Corporation",
    "PulsePoint Analytics",
    "UrbanLoom Developments",
    "ElevateWorks Group",
    "Greenstream Partners",
    "InfiniteTrail Logistics",
    "SummitForge Enterprises",
    "VividLink Communications",
    "PrimeHorizon Labs"
]

div_freq = [12,6,3,1]


