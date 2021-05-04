#author: Jigar Makwana B00842568
from userManagement import functions
from userManagement import user_class
from execution import Execution
import logging
from dbERD.generateERD import generateERD
from sqlDump.sqlDump import createDump
logging.basicConfig(format='%(asctime)s - %(message)s', filename='logs/eventlogs.log')

dd_path = "dataDictonary/"
db_log = "logs/eventlogs.log"

class DBMSMain:
    def DBMSMainMenu(username):
        while( True ):
            print('User in session: ' + username)
            logging.warning('User in session: ' + username)
            userInput = functions.display_DBMS_options()
            if(userInput == "1"):
                Execution.ExecutionMenu(username)
            elif(userInput == "2"):
                with open(db_log, 'r') as f:
                    for line in f:
                        print(line)
            elif(userInput == "3"):
                db_name = input("Enter a DB Name: ")
                with open(dd_path + db_name + '.json', 'r') as f:
                    for line in f:
                        print(line)
            elif(userInput == "4"):
                dbname = input("Enter a new Database Name: ")
                createDump(dbname)
            elif(userInput == "5"):
                dbname = input("Enter a new Database Name: ")
                generateERD(dbname)
            elif(userInput == "6"):
                isLoggedIn = user_class.User.logOut()
                break
            else:
                print("Please enter a valid option...")
            print("\n")

