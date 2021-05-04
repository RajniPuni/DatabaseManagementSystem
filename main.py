# author: Jigar Makwana B00842568
import sys
from userManagement.user_class import User
from userManagement.functions import display_options
from dbmsMain import DBMSMain
import logging

User.loadDatabase()
logging.basicConfig(format='%(asctime)s - %(filename) - %(message)s', filename='logs/eventlogs.log')
# user_class.User.generateKey() Needs to be run only once

global isLoggedIn
global username

if __name__ == '__main__':
    while (True):
        isLoggedIn = False
        userInput = display_options()
        if (userInput == "1"):
            username = input("Please enter a Username: ")
            password = input("Please enter a Password: ")
            isLoggedIn = User.signIn(username, password)
            if (isLoggedIn):
                print("\nSuccessfully Signed into CSCI5408 DBMS")
                logging.warning("\nSuccessfully Signed into CSCI5408 DBMS")
                DBMSMain.DBMSMainMenu(username)
            else:
                print("\nPlease try again..")
        elif (userInput == "2"):
            username = input("Please enter a Username: ")
            password = input("Please enter a Password: ")
            User.addUser(username, password)
        elif (userInput == "3"):
            User.displayUsers()
        elif (userInput == "4"):
            print("Quitting CSCI5408 DBMS....")
            logging.warning("Quitting CSCI5408 DBMS....")
            sys.exit(0)
        else:
            print("Please enter a valid option...")
        print("\n")
