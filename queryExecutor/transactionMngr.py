#author: Jigar Makwana B00842568
from os import path, listdir
import logging
import time
import csv
import os
import queryParser.queryParser as qp

db_path = "database/"
logging.basicConfig(format='%(asctime)s - %(message)s', filename='logs/eventlogs')

db = ''
userName = ''
needToSetLock = False
filePath = ''
filePathTemp = ''

def setUserDBName(dbname, username):
    global db, userName
    db = dbname
    userName = username

def executeQuery():
    global filePath
    global filePathTemp
    db_main = db_path + db
    if path.exists(db_main):
        # database: validation
        print("Into the database: ", db)
        logging.info('User entered valid database: ' + db_main)
        qp.tableNames = listdir(db_path + db)

        # query: validation and parsing
        while True:
            query = input(">> ")
            if(query == "q"):
                break
            elif(query == "start transaction"):
                needToSetLock = True
                continue
            elif(query == "commit"):
                commit()
            else:
                try:
                    parser = qp.parseQuery(query, db, userName)
                    logging.warning('Query is parsed successfully.')
                    logging.warning(parser)
                    # print(parser)

                    # query: select
                    if parser['Type'] == 'select':
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == -1):
                            logging.warning('Wainting for other transaction to release the lock...')
                            print('Wainting for other transaction to release the lock')
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            isLockedFlag = isLocked(parser['Table'][0].lower())
                            if(isLockedFlag == -1):
                                logging.warning('Exclusive Lock is not released by other transaction')
                                print('Exclusive Lock is not released by other transaction')
                                print('Cannot perform read')
                                rollback()
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == 0 or isLockedFlag == 1):
                            rows = []
                            with open(db_main + parser['Table'][0].lower() + '.csv') as table:
                                table_data = table.read().splitlines()
                                table_cols = table_data[0].split(',')

                                if parser['InsertUpdateData'][0] != '*':
                                    indexes = []
                                    for col in parser['InsertUpdateData']:
                                        indexes.append(table_cols.index(col))
                                    for row in table_data:
                                        cols = row.split(',')
                                        row_data = ''
                                        for index in indexes:
                                            row_data += cols[index] + ','
                                        rows.append(row_data[:-1])
                                else:
                                    for row in table_data:
                                        rows.append(row)

                                if len(parser['WhereFields']) == 0:
                                    for element in rows:
                                        print(element)
                                    logging.warning('Select query executed successfully.')
                                else:
                                    indexes = [table_cols.index(col) for col in parser['WhereFields']]
                                    conditions_indexes = [0]
                                    for row_idx in range(1, len(table_data)):
                                        table_row = table_data[row_idx].split(',')
                                        for idx in range(len(indexes)):
                                            if table_row[indexes[idx]] == parser['WhereValues'][idx]:
                                                conditions_indexes.append(row_idx)

                                    filtered_rows = []
                                    for idx in range(len(rows)):
                                        if idx in conditions_indexes:
                                            filtered_rows.append(rows[idx])

                                    for element in filtered_rows:
                                        print(element)
                                    logging.warning('Select query executed successfully.')

                    # query: insert
                    elif parser['Type'] == 'insert':
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == -1):
                            logging.warning('Wainting for other transaction to release the lock...')
                            print('Wainting for other transaction to release the lock')
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            isLockedFlag = isLocked(parser['Table'][0].lower())
                            if(isLockedFlag == -1):
                                logging.warning('Exclusive Lock is not released by other transaction')
                                print('Exclusive Lock is not released by other transaction')
                                rollback()
                        if(isLockedFlag == 0):
                            if(needToSetLock):
                                setLock(parser['Table'][0].lower())
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == 1):
                            filePath = db_main + '/' + parser['Table'][0].lower() +'.csv'
                            filePathTemp = db_main + '/' + parser['Table'][0].lower() +'temp.csv'
                            if os.path.exists(filePathTemp):
                                with open(db_main + filePathTemp, 'a') as table:
                                    row = ','.join(col for col in parser['InsertUpdateData'])
                                    table.write(row + "\n")
                                    logging.warning('Insert query executed successfully.')
                                    print("Successfully entered")
                            else:
                                os.system(f'{filePath} {filePathTemp}')
                                if os.path.exists(filePathTemp):
                                    with open(db_main + filePathTemp, 'a') as table:
                                        row = ','.join(col for col in parser['InsertUpdateData'])
                                        table.write(row + "\n")
                                        logging.warning('Insert query executed successfully.')
                                        print("Successfully entered")

                    # query: update
                    elif parser['Type'] == 'update':
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == -1):
                            logging.warning('Wainting for other transaction to release the lock...')
                            print('Wainting for other transaction to release the lock')
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            print('...')
                            time.sleep(1)
                            isLockedFlag = isLocked(parser['Table'][0].lower())
                            if(isLockedFlag == -1):
                                logging.warning('Exclusive Lock is not released by other transaction')
                                print('Exclusive Lock is not released by other transaction')
                                rollback()
                        if(isLockedFlag == 0):
                            if(needToSetLock):
                                setLock(parser['Table'][0].lower())
                        isLockedFlag = isLocked(parser['Table'][0].lower())
                        if(isLockedFlag == 1):
                            rows = []
                            table_data_rows = []
                            filePath = db_main + '/' + parser['Table'][0].lower() +'.csv'
                            filePathTemp = db_main + '/' + parser['Table'][0].lower() +'temp.csv'
                            # filePath = 'database/datadb/sample'
                            table = open(filePath, 'r') 
                            with table:
                                col = parser['WhereFields'][0]
                                update_col = parser['UpdateFields'][0]
                                table_data_rows = table.readlines()
                                rows.append(table_data_rows[0])
                                header = table_data_rows[0].split(',')
                                for rows_idx in range(1, len(table_data_rows)):
                                    row = table_data_rows[rows_idx]
                                    row_elements = row.split(',')
                                    if row_elements[header.index(col)] != parser['WhereValues'][0]:
                                        rows.append(row)
                                    else:
                                        row = row.replace(row_elements[header.index(update_col)], parser['InsertUpdateData'][0])
                                        rows.append(row)
                            with open(filePathTemp, 'w') as table:
                                for row in rows:
                                    table.write(row)
                            logging.warning('Update query executed successfully.')
                            print("Update query executed successfully.")

                    # query: delete
                    elif parser['Type'] == 'delete':
                        rows = []
                        table_data_rows = []
                        
                        with open(filePath, 'r') as table:
                            col = parser['WhereFields'][0]
                            table_data_rows = table.readlines()
                            rows.append(table_data_rows[0])
                            header = table_data_rows[0].split(',')
                            for rows_idx in range(1, len(table_data_rows)):
                                row = table_data_rows[rows_idx]
                                row_elements = row.split(',')
                                if row_elements[header.index(col)] != parser['WhereValues'][0]:
                                    rows.append(row)
                        with open(db_main + parser['Table'][0].lower(), 'w') as table:
                            for row in rows:
                                table.write(row)
                        logging.warning('Delete query executed successfully.')
                        print("Deleted Successfully")

                    # query: create
                    elif parser['Type'] == 'create':
                        with open(db_main + parser['Table'][0].lower(), 'w') as table:
                            print(table)

                except Exception as e:
                    print(e)
                    # logging.warning('Exclusive Lock is not released by other transaction')
                    # print('Exclusive Lock is not released by other transaction')
                    rollback()

    else:
        logging.info('User entered invalid database name.')
        print("Invalid database name")

def isLocked(tableName):
    isLockedFlag = False
    filePath = "dataDictonary/locktracker.csv"
    myFile = open(filePath, 'r')        
    with myFile:
        reader = csv.reader(myFile)
        # next(reader, None)
        for row in reader:
            if len(row) != 0:
                if(row[2] == tableName and row[3] == 'yes'):
                    isLockedFlag = True
                    if(row[0] == userName and row[1] == db):
                        return 1
                    elif(row[0] != userName and row[1] == db):
                        return -1
                if(isLockedFlag == False):
                    return 0
        return 0

def setLock(tableName):   
    lockEntry = []
    data = [userName, db, tableName, 'yes']
    lockEntry.append(data)
    filePath = "dataDictonary/locktracker.csv"
    myFile = open(filePath, 'w')     
    with myFile:
        writer = csv.writer(myFile)
        writer.writerows(lockEntry)
    logging.warning("\nA new lock is acquired on table " + tableName + " by user " + userName)
    print("\nA new lock is acquired on table " + tableName + " by user " + userName)

def releaseLock():
    needToSetLock = False   
    filePath = "dataDictonary/locktracker.csv"
    myFile = open(filePath, 'w')   
    logging.warning('Lock is released')
    print('Exclusive Lock is released')

def rollback():
    print('Transaction is rolled back.')

def commit():
    fileName = filePath
    if os.path.exists(filePath):
        os.remove(filePath)
    else:
        print("The file does not exist")   
    if os.path.exists(filePathTemp):
        os.rename(filePathTemp, fileName)
    else:
        print("The file does not exist")    
    releaseLock()
    print('\nTransaction is Commited')