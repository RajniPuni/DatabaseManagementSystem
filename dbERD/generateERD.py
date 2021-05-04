#author: Jigar Makwana B00842568
import csv
import json

def extractColumn(dbname, table, key):
    array = []
    index = 0
    filepath = 'database/'+ dbname + '/' + table +'.csv'
    with open(filepath) as table:
        rows = table.read().splitlines()
        headerColumns = rows[0].split(',')
    for i in range(len(headerColumns)):
        if headerColumns[i] == key:
            index = i
    for i in range(len(rows)):
        columns = rows[i].split(',')
        array.append(columns[index])
    return array

def calculateCardinality(dbname, table1, table2, key):
    array1 = extractColumn(dbname, table1, key)
    if(len(array1) == len(set(array1))):
        return '1TO1'
    else:
        return '1TON'

def generateERD(dbname):
    filepath = 'dataDictonary/' + dbname +'MetaData.csv'
    erdList = []
    with open(filepath) as table:
        entities = table.read().splitlines()

        erdList.append('\nDatabase Name is :' + dbname)
        erdList.append(f'Database has total {len(entities)} entities')

        for index in range(len(entities)):
            tablePath = 'dataDictonary/' + entities[index] +'MetaData.json'

            with open(tablePath) as json_file:
                parseData = json.load(json_file)

            erdList.append('\nTable Name is :' + parseData["Table"][0])

            erdList.append(parseData["Table"][0]+ "'s details is as follow:")
            for index in range(len(parseData["Columns"])):
                for key in parseData["Columns"][index]:
                    if(key == "Name"):
                        erdList.append('Column Name is :' + parseData["Columns"][index]["Name"])
                    elif(key == "DataType"):
                        erdList.append('Column Data Type is :' + parseData["Columns"][index]["DataType"])
                    elif(key == "isPrimaryKey"):
                        if(parseData["Columns"][index]["isPrimaryKey"] == True):
                            erdList.append(parseData["Columns"][index]["Name"] + ' is PrimaryKey')
                    elif(key == "isForeignKey"):
                        if(parseData["Columns"][index]["isForeignKey"] == True):
                            erdList.append(parseData["Columns"][index]["Name"] + ' is ForeignKey')
                    elif(key == "referencesTable"):
                        if(parseData["Columns"][index]["referencesTable"] != ""):
                            erdList.append("Key: " + parseData["Columns"][index]["Name"] + ' references to table: ' + parseData["Columns"][index]["referencesTable"])
                            erdList.append("Reference key is: " + parseData["Columns"][index]["referencesKey"])
                            card = calculateCardinality(dbname, parseData["Table"][0], parseData["Columns"][index]["referencesTable"], parseData["Columns"][index]["Name"])
                            if(card == '1TO1'):
                                erdList.append("Cradinality from " + parseData["Columns"][index]["referencesTable"] + " to " + parseData["Table"][0] + " is 1:1")

                            elif(card == '1TON'):
                                erdList.append("Cradinality from " + parseData["Columns"][index]["referencesTable"] + " to " + parseData["Table"][0] + " is 1:N")


    filepath = 'dbERD/' + dbname + 'ERD.txt'
    MyFile=open(filepath,'w')
    for element in erdList:
        MyFile.write(element)
        MyFile.write('\n')
    MyFile.close()
    print('\nERD sucessfully dumped in file!')


