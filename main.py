from createScripts import *
from readScripts   import *
from updateScripts import *
from deleteScripts import *


info = "###CREATE###\n 1) Create a customer.\n 2) Create an account.\n 3) Create an issuer.\n 4) Add an order.\n 5) Add a selling.\n 6) Create shares.\n 7) Create a transaction.\n 8) Create a dividend transaction.\n###READ###\n 9) Read selected account.\n10) Read selected customer.\n11) Read selected issuer.\n12) Read selected shares.\n13) Read all customers.\n14) Read all client accounts.\n15) Read all customer's accounts.\n16) Read all dividends paid within a time period.\n17) Read all inactive accounts.\n18) Read exceeding accounts.\n19) Read account balance.\n20) Read overdue shares.\n21) Read unpaid dividends.\n22) Read total dividends aggregate.\n###UPDATE###\n23) Update due date for shares of a certain issuer.\n24) Update shares that are overdue.\n25) Update shares count for a selected issuer and client.\n###AGGREGATE###\n26) Aggregate total dividends history of a client per issuer (most to least).\n27) Aggregate total dividends hisotry of a client per issuer (least to most).\n28) Aggregate total dividends history of a client per time period (recent to latest).\n29) Aggregate shares owend by a customer (most to least).\n30) Aggregate shares owned by a customer (least to most).\n31) Aggregate shares by city. \n32) Aggregate total balance by currency.\n###DELETE###\n33) Deleting all customers.\n34) Deleting all accounts.\n35) Deleting all issuers.\n36) Deleting all shares.\n37) Deleting all transactions.\n38) Deleting all empty shares.\n"

input = input("Which method do you want to execute? ")