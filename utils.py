from pymongo import *
from datetime import *
from random import *

def openConnection():
    url = 'mongodb+srv://group4:2PFTvj2srxCpOfED@csci112-group4.mu6gp.mongodb.net/'

    conn = MongoClient(url)

    return conn

def closeConnection(conn):
    conn.close()
