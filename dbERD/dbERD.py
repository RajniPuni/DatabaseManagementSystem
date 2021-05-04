#author: Jigar Makwana B00842568
import csv
import json

def generateERD(dbname):
    filepath = 'dataDictonary/' + dbname +'.json'
    with open(filepath) as json_file:
        parseData = json.load(json_file)
    erdList = []
    print('\nPrinting ERD...')

    print('\nDatabase Name is :' + parseData["Database"])
    erdList.append('\nDatabase Name is :' + parseData["Database"])

    print('\nTable Name is :' + parseData["Table"][0])
    erdList.append('\nTable Name is :' + parseData["Table"][0])

    erdList.append('\nColumn details are as follow: \n')
    print('\nColumn details are as follow: \n')
    for index in range(len(parseData["Columns"])):
        for key in parseData["Columns"][index]:
            if(key == "Name"):
                print('Column Name is :' + parseData["Columns"][index]["Name"])
                erdList.append('Column Name is :' + parseData["Columns"][index]["Name"])
            elif(key == "DataType"):
                print('Column Data Type is :' + parseData["Columns"][index]["DataType"])
                erdList.append('Column Data Type is :' + parseData["Columns"][index]["DataType"])
            elif(key == "isUnique"):
                if(parseData["Columns"][index]["isUnique"] == "True"):
                    print(parseData["Columns"][index]["Name"] + ' is Unique')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is Unique')
                else:
                    print(parseData["Columns"][index]["Name"] + ' is not Unique')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is not Unique')
            elif(key == "isNotNull"):
                if(parseData["Columns"][index]["isNotNull"] == "True"):
                    print(parseData["Columns"][index]["Name"] + ' cannot be Not Null')
                    erdList.append(parseData["Columns"][index]["Name"] + ' cannot be Not Null')
                else:
                    print(parseData["Columns"][index]["Name"] + ' can be Null')
                    erdList.append(parseData["Columns"][index]["Name"] + ' cannot be Null')
            elif(key == "isAutoIncrement"):
                if(parseData["Columns"][index]["isAutoIncrement"] == "True"):
                    print(parseData["Columns"][index]["Name"] + ' is Auto Increment')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is Auto Increment')
                else:
                    print(parseData["Columns"][index]["Name"] + ' is not Auto Increment')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is not Auto Increment')
            elif(key == "isPrimaryKey"):
                if(parseData["Columns"][index]["isPrimaryKey"] == "True"):
                    print(parseData["Columns"][index]["Name"] + ' is PrimaryKey')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is PrimaryKey')
                else:
                    print(parseData["Columns"][index]["Name"] + ' is not PrimaryKey')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is not PrimaryKey')
            elif(key == "isForeignKey"):
                if(parseData["Columns"][index]["isForeignKey"] == "True"):
                    print(parseData["Columns"][index]["Name"] + ' is ForeignKey')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is ForeignKey')
                else:
                    print(parseData["Columns"][index]["Name"] + ' is not ForeignKey')
                    erdList.append(parseData["Columns"][index]["Name"] + ' is not ForeignKey')
            elif(key == "references"):
                if(parseData["Columns"][index]["references"] != ""):
                    print(parseData["Columns"][index]["Name"] + ' references ' + parseData["Columns"][index]["references"])
                    erdList.append(parseData["Columns"][index]["Name"] + ' references ' + parseData["Columns"][index]["references"])
            elif(key == "default"):
                if(parseData["Columns"][index]["default"] != ""):
                    print(parseData["Columns"][index]["Name"] + "'s default value is " + parseData["Columns"][index]["default"])
                    erdList.append(parseData["Columns"][index]["Name"] + "'s default value is " + parseData["Columns"][index]["default"])
                    print("\n")
                    erdList.append("\n")
                else:
                    print(parseData["Columns"][index]["Name"] + ' does not have any default value')
                    erdList.append(parseData["Columns"][index]["Name"] + ' does not have any default value')
                    print("\n")
                    erdList.append("\n")

    filepath = 'dbERD/' + dbname + 'ERD.txt'
    MyFile=open(filepath,'w')
    for element in erdList:
        MyFile.write(element)
        MyFile.write('\n')
    MyFile.close()
    print('\nERD sucessfully dumped in file!')
