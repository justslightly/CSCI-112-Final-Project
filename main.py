from createScripts import *
from readScripts   import *
from updateScripts import *
from deleteScripts import *

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

info = "E) End Program\n###CREATE###\nC1)  Create random customers.\nC2)  Create random accounts.\nC3)  Create random issuers.\nC4)  Create random shares.\nC5)  Create transactions.\nC6)  Create dividend transactions.\nC7)  Add random orders.\nC8)  Add random selling.\nC9)  Add one customer.\nC10) Add one account.\nC11) Add one order.\nC12) Add one selling.\nC13) Add one transaction.\n###READ###\nR1)  Read selected account.\nR2)  Read selected customer.\nR3)  Read selected issuer.\nR4)  Read selected shares.\nR5)  Read all customers.\nR6)  Read all client accounts.\nR7)  Read all customer's accounts.\nR8)  Read all dividends paid within a time period.\nR9)  Read all inactive accounts.\nR10) Read exceeding accounts.\nR11) Read account balance.\nR12) Read overdue shares.\nR13) Read unpaid dividends.\nR14) Read total dividends aggregate.\n###UPDATE###\nU1)  Update due date for shares of a certain issuer.\nU2)  Update shares that are overdue.\nU3)  Update shares count for a selected issuer and client.\n###AGGREGATE###\nA1)  Aggregate total dividends history of a client per issuer.\nA2)  Aggregate total dividends history of a client per time period (recent to latest).\nA3)  Aggregate shares owend by a customer (most to least).\nA4)  Aggregate shares owned by a customer (least to most).\nA5)  Aggregate shares by city. \nA6)  Aggregate total balance by currency.\n###DELETE###\nD1)  Deleting all customers.\nD2)  Deleting all accounts.\nD3)  Deleting all issuers.\nD4)  Deleting all shares.\nD5)  Deleting all transactions.\nD6)  Deleting all aggregates.\nD7)  Deleting all empty shares.\n"

print(info)
user_input = input("Which method do you want to execute? ")
while user_input!="E":
    if user_input=="C1":
        createCustomer()
    elif user_input=="C2":
        createAccount()
    elif user_input=="C3":
        createIssuer()
    elif user_input=="C4":
        createShares()
    elif user_input=="C5":
        createTransaction()
    elif user_input=="C6":
        createDivTransaction()
    elif user_input=="C7":
        addOrders()
    elif user_input=="C8":
        addSelling()
    elif user_input=="C9":
        NameFirst = input("First Name: ")
        NameLast = input("Last Name: ")
        City = input("City: ")
        BirthYear = input("Birth year (YYYY): ")
        BirthMonth = input("Birth month (MM): ")
        BirthDay = input("Birth day (DD): ")
        oneCustomer(NameFirst, NameLast, City, BirthYear, BirthMonth, BirthDay)
    elif user_input=="C10":
        customerID = input("Customer ID: ")
        while True: 
            AcctType = input("Account Type (Savings or Checking): ")
            if AcctType in ["Savings", "Checking"]:
                break
        # Check if real??
        clientAcct = input("Client Account: ")
        Balance = int(input("Balance: "))
        while True:
            Address = input("Address: ")
            if Address in cities_countries:
                break
        oneAccount(customerID, AcctType, clientAcct, Balance, Address)
    elif user_input=="C11":
        customerID = input("Customer ID: ")
        issuerID = input("Issuer ID: ")
        numShares = int(input("Number of Shares: "))
        AcctNumber = input("Account Number: ")
        DueDate = input("Due Date: ")
        oneOrder(customerID, issuerID, numShares, AcctNumber, DueDate)
    elif user_input=="C12":
        customerID = input("Customer ID: ")
        issuerID = input("Issuer ID: ")
        oneSelling(customerID, issuerID)
    elif user_input=="C13":
        acctFrom = input("Account from: ")
        acctTo = input("Account to: ")
        amount = input("Amount: ")
        while True: 
            div = input("Dividend (0 or 1): ")
            if div in ["0", "1"]:
                break
        oneTransaction(acctFrom, acctTo, amount, div)
    elif user_input=="R1":
        account_no = input("Account Number: ")
        readAccount(account_no)
    elif user_input=="R2":
        customer_id = input("Customer ID: ")
        readCustomer(customer_id)
    elif user_input=="R3":
        issuer_id = input("Issuer ID: ")
        readIssuer(issuer_id)
    elif user_input=="R4":
        account_number = input("Account Number: ")
        issuer_id = input("Issuer ID: ")
        readShares(account_number, issuer_id)
    elif user_input=="R5":
        readCustomers()
    elif user_input=="R6":
        readClientAccs()
    elif user_input=="R7":
        customer_id = input("Customer ID: ")
        readCustomerAccs(customer_id)
    elif user_input=="R8":
        account_number = input("Account Number: ")
        start_year = input("Start year (YYYY): ")
        start_month = input("Start month (MM): ")
        start_day = input("Start day (DD): ")
        end_year = input("End year (YYYY): ")
        end_month = input("End month (MM): ")
        end_day = input("End day (DD): ")
        readDividendHistory(account_number, start_year, start_month, start_day, end_year, end_month, end_day)
    elif user_input=="R9":
        readInactiveAcc()
    elif user_input=="R10":
        threshold = input("Threshold: ")
        readExceedingAcc(threshold)
    elif user_input=="R11":
        readAccBalance()
    elif user_input=="R12":
        readOverdueShares()
    elif user_input=="R13":
        readUnpaidDividends()
    elif user_input=="R14":
        readAggTotalDiv()
    elif user_input=="U1":
        issuer_id = input("Issuer ID: ")
        due_year = input("Due year (YYYY): ")
        due_month = input("Due month (MM): ")
        due_day = input("Due day (DD): ")
        updateSharesDueDate(issuer_id, due_year, due_month, due_day)
    elif user_input=="U2":
        due_year = input("Due year (YYYY): ")
        due_month = input("Due month (MM): ")
        due_day = input("Due day (DD): ")
        updateSharesOverdue()
    elif user_input=="U3":
        account_number = input("Account Number: ")
        issuer_id = input("Issuer ID: ")
        count = input("Shares Count: ")
        updateSharesCount(account_number, issuer_id, count)
    elif user_input=="A1":
        account_number = input("Account Number: ")
        aggregateTotalDividends(account_number)
        readAggTotalDiv()
    elif user_input=="A2":
        account_number = input("Account Number: ")
        aggregateDividendsByTime(account_number)
    elif user_input=="A3":
        account_number = input("Account Number: ")
        aggregateSharesByCustomerDesc(account_number)
    elif user_input=="A4":
        account_number = input("Account Number: ")
        aggregateSharesByCustomerAsc(account_number)
    elif user_input=="A5":
        aggregateSharesByCity()
    elif user_input=="A6":
        aggregateTotalBalanceByCurrency()
    elif user_input=="D1":
        deleteCustomer()
    elif user_input=="D2":
        deleteAccount()
    elif user_input=="D3":
        deleteIssuer()
    elif user_input=="D4":
        deleteShares()
    elif user_input=="D5":
        deleteTransaction()
    elif user_input=="D6":
        deleteAggregates()
    elif user_input=="D7":
        deleteEmptyShares()
    else:
        print("Incorrect User Input")
    user_input = input("Which method do you want to execute? ")