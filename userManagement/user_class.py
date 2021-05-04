# author: Jigar Makwana B00842568
from cryptography.fernet import Fernet
import random
import csv
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', filename='logs/eventlogs.log')


class User:
    database = "userManagement//UserDatabase.csv"
    maxNoUsers = 1500
    dbUsersList = []

    def __init__(self, id_num, name, password):
        self.id_num = int(id_num)
        self.name = name
        self.password = password
        self.dbUsersList.append(self)
        self.key = ""

    def displayUsers():
        myFile = open(User.database, 'r', newline='')
        with myFile:
            reader = csv.reader(myFile, delimiter=',')
            for row in reader:
                print("\n")
                print(row[0])
                print(row[1])
                print(row[2])

    def saveUserstoDB(id_num, name, password):
        userData = []
        # for i in range(0, len(User.dbUsersList)):
        encryptedPassword = User.encryptPassword(password)
        data = [id_num, name, encryptedPassword]
        userData.append(data)
        myFile = open(User.database, 'a', newline='')
        with myFile:
            writer = csv.writer(myFile)
            writer.writerows(userData)
        print("\nNew user is saved in DBMS Database!")
        logging.warning("\nNew user is saved in DBMS Database!")

    def addUser(username, password):
        newUser = User((User.getUniqueNumber()), username, password)
        User.saveUserstoDB(newUser.id_num, newUser.name, newUser.password)
        print("\nSucessfully added a New user.\n")
        logging.warning("\nSucessfully added a New user.\n")

    def loadUsers():
        myFile = open(User.database, 'r')
        with myFile:
            reader = csv.reader(myFile)
            next(reader, None)
            data = list(reader)
            for entry in data:
                newuser = User(entry[0], entry[1], entry[2])

    def signIn(username, password):
        myFile = open(User.database, 'r', newline='')
        with myFile:
            reader = csv.reader(myFile, delimiter=',')
            for row in reader:
                if (row[1] == username):
                    decryptedPassword = User.decryptPassword(row[2])
                    # print(f"\ndecryptedPassword: {decryptedPassword}")
                    if (decryptedPassword == password):
                        return True
                    else:
                        print("\nInvalid password")
                        logging.warning("\nInvalid password")
                        return False
            print("\nUser not found")
            logging.warning("\nUser not found")
            return False

    def logOut():
        return False

    def generateKey():
        # print('Entered to the generate key')
        key = Fernet.generate_key()
        # print(f'Generated key {key}')
        with open('userManagement//keys.csv', 'w') as key_in:
            key_in.write(key.decode())

    def loadKey():
        return open('userManagement//keys.csv', "r").read()

    def encryptPassword(password):
        key = User.loadKey()
        # print(f'\nretrived key: {key}')
        f = Fernet(key.encode())
        encryptedPassword = f.encrypt(password.encode()).decode()
        # print(f'encrypted_message {encryptedPassword}')
        return encryptedPassword

    def decryptPassword(password):
        # print(f'In decrypt {password}')
        key = User.loadKey()
        # print(f'\nretrived key: {key}')
        f = Fernet(key)
        decryptedPassword = f.decrypt(password.encode()).decode()
        return decryptedPassword

    def checkId_num(newNumber):
        for k in range(0, len(User.dbUsersList)):
            if (User.dbUsersList[k].id_num == newNumber):
                return False
        return True

    def getUniqueNumber():
        unique = False
        while (not unique):
            num = random.randrange(1, User.maxNoUsers)
            if (User.checkId_num(num) == True):
                unique = True
        return num

    def loadDatabase():
        User.loadUsers()

    def save_database():
        print("\nSaving a new User to Database....")
        logging.warning("\nSaving a new User to Database....")
        User.saveUserstoDB()
        return 1
