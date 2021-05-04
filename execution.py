# author: Jigar Makwana B00842568
from queryParser.queryParser import parseQuery
from queryExecutor.queryExecutor import qExecuteQuery, q_set_db_name
from queryExecutor.transactionMngr import executeQuery, setUserDBName
from dataDictonary.createMetaData import createDBUserMap
from userManagement import functions
from userManagement import user_class
import os
import csv
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', filename='logs/eventlogs.log')
import json

db_path = "database/"
db_name = ''

class Execution:
    def set_db_name(dbname):
        global db_name
        db_name = dbname

    def ExecutionMenu(username):
        while (True):
            print('User in session: ' + username)
            logging.warning('User in session: ' + username)
            userInput = functions.display_CRUD_options()
            if (userInput == "1"):
                dbname = input("Enter a new Database Name: ")
                directory = dbname
                newDBPath = os.path.join(db_path, directory)  
                try:
                    os.mkdir(newDBPath)
                    createDBUserMap(dbname, username)
                    print(f"\nDatabase {dbname} sucessfully created!")
                    logging.info(f"\nDatabase {dbname} sucessfully created!")
                except OSError as error:  
                    print(f"\nDatabase {dbname} already exist!")
                    logging.warning(f"\nDatabase {dbname} already exist!")
            elif (userInput == "2"):
                dbname = input("Enter an existing Database Name: ")
                q_set_db_name(dbname)
                Execution.set_db_name(dbname)
                setUserDBName(dbname + "/" , username)
                print(dbname + ' database selected successfully')
                logging.info(dbname + ' database selected successfully')
            elif (userInput == "3"):
                # createTableQuery = input("Enter Create Table Query: ")
                # dbname = input("Enter a new Database Name: ")
                # Execution.set_db_name(dbname)
                # Execution.createMetaData(db_name, parseQuery(createTableQuery, db_name, username))
                qExecuteQuery(username)
            elif (userInput == "3"):
                qExecuteQuery(username)
            elif (userInput == "4"):
                executeQuery()
            elif (userInput == "5"):
                isLoggedIn = user_class.User.logOut()
                break
            else:
                print("Please enter a valid option...")
            print("\n")

