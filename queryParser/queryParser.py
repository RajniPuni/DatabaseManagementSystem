import re;
import os;
import logging

dirname1 = os.path.dirname
path1 = os.path.join(dirname1(dirname1(__file__)))
logging.basicConfig(format='%(asctime)s - %(message)s', filename= path1 + '/logs/eventlogs.log')
qType = ""
qTableName = []
qInserts = []
qFields = []
qUpdates = []
qWhereFields = []
qWhereValues = []
qDatabaseName = []
colList = []

reserveWords = ["(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "select", "insert", "values", "update", "delete", "where", "from", "set"]
tableNames = ["employee", "department"]
listQuery = []
returnValue = ""


def parseQuery(query, DB_Name, UserName):

    resetResult()
    oldQuery = query.strip().split(" ")

    for val in oldQuery:
        listQuery.append(val.lower())

    returnValue = "False"
    qType = listQuery.pop(0)
    if len(listQuery) > 0:
        if qType == "select":
            if checkSelectStep():
                returnValue = "True"
        elif qType == "insert":
            if checkInsertStep(query, DB_Name):
                returnValue = "True"
        elif qType == "update":
            if checkUpdateStep():
                returnValue = "True"
        elif qType == "delete":
            if checkDeleteStep():
                returnValue = "True"
        elif qType == "create":
            if checkCreateStep(query, DB_Name) == "table":
                returnValue = "CreateTable"
    else:
        print("Invalid query")
        logging.warning("Invalid query")
        returnValue = "False"

    if returnValue == "True":
        parsedData = {
            "Type": qType,
            "Database": qDatabaseName,
            "Table": qTableName,
            "InsertFields": qInserts,
            "UpdateFields": qUpdates,
            "InsertUpdateData": qFields,
            "WhereFields": qWhereFields,
            "WhereValues": qWhereValues,
            "Error": ""
        }

        return parsedData
    elif returnValue == "CreateTable":
        parsedData = {
            "Type": qType,
            "Database": DB_Name,
            "UserName": UserName,
            "Table": qTableName,
            "Columns":colList
        }
    else:
        parsedData = {
            "Error": "Invalid query"
        }
    print(parsedData)
    return parsedData


def resetResult():
    global qType, qTableName, qInserts, qFields, qUpdates, qWhereFields, qWhereValues, colList, listQuery
    qType = ""
    qTableName = []
    qInserts = []
    qFields = []
    qUpdates = []
    qWhereFields = []
    qWhereValues = []
    colList = []
    listQuery = []


def checkCreateStep(query, DB_Name):
    fromVal = listQuery.pop(0)

    dirname = os.path.dirname
    path = os.path.join(dirname(dirname(__file__)))

    if fromVal == "table":
        tableName = listQuery.pop(0)
        qTableName.append(tableName)
        directoryPath = path + "/sqlDump/" + DB_Name

        #fullPath = directoryPath + "/data"
        #dumpPath = "sqlDump/" + DB_Name
        with open(directoryPath, "a") as file1:
            toFile = query + "\n"
            file1.write(toFile)
        if len(listQuery) > 0:
            createColumnList(query)

    elif fromVal == "database":
        databaseName = listQuery.pop(0)
        # os.mkdir(path + "/database/" + databaseName)
        directoryPath = path + "/sqlDump/" + DB_Name
        #
        # fullPath = directoryPath + "/data"
        #dumpPath = "sqlDump/" + DB_Name
        with open(directoryPath, "a") as file1:
            toFile = "\n"
            file1.write(toFile)
    return fromVal


def createColumnList(query):
    queryWithoutCreate = query.lower().replace("create table " + qTableName[-1] + " ( ", "")
    listColQuery = queryWithoutCreate.strip().split(",")
    for val in listColQuery:
        columnData = val.strip().split(" ")
        columnName = columnData.pop(0)
        datatype = columnData.pop(0)
        isNotNullVal = False
        isUnique = False
        isAutoIncrement = False
        isPrimaryKey = False
        isForeignKey = False
        referencesTable = ""
        referencesKey = ""
        default = ""

        if(len(columnData) > 0):
            isNot = columnData.pop(0)

            if isNot.lower() == "not":
                isnull = columnData.pop(0)
                isNotNullVal = True

                isUniqueVal = columnData.pop(0)
                if isUniqueVal.lower() == "unique":
                    isUnique = True

                    isAutoIncrementVal = columnData.pop(0)
                    if isAutoIncrementVal.lower() == "auto_increment":
                        isAutoIncrement = True

                        isPrimaryKeyVal = columnData.pop(0)
                        if isPrimaryKeyVal.lower() == "primary":
                            isPrimaryKeyVal = columnData.pop(0)
                            isPrimaryKey = True
                        elif isPrimaryKeyVal.lower() == "foreign":
                            isPrimaryKeyVal = columnData.pop(0)
                            isForeignKey = True
                            refrencesVal = columnData.pop(0)
                            referencesTable = columnData.pop(0)
                            removeval = columnData.pop(0)
                            referencesKey = columnData.pop(0)

                        if len(columnData) > 0:
                            defaultVal = columnData.pop(0)
                            if defaultVal.lower() == "default":
                                default = columnData.pop(0)

                if isUniqueVal.lower() == "default":
                    default = columnData.pop(0)

            elif isNot.lower() == "unique":
                isUnique = True

                defaultVal = columnData.pop(0)
                if defaultVal.lower() == "default":
                    default = columnData.pop(0)

            elif isNot.lower() == "foreign":
                isForeignKey = True
                isForeignKeyVal = columnData.pop(0)
                referencesVal = columnData.pop(0)
                referencesTable = columnData.pop(0)
                removeval = columnData.pop(0)
                referencesKey = columnData.pop(0)

        column = {"Name": columnName, "DataType": datatype, "isNotNull": isNotNullVal, "isUnique": isUnique,
                  "isAutoIncrement": isAutoIncrement, "isPrimaryKey": isPrimaryKey, "isForeignKey": isForeignKey,
                  "referencesTable": referencesTable, "referencesKey":referencesKey, "default":default}
        colList.append(column)
   # print(colList)


def checkDeleteStep():
    fromVal = listQuery.pop(0)

    if checkIfFrom(fromVal) and len(listQuery) > 0:
        tableValue = listQuery.pop(0)
        if checkIfTable(tableValue):
            if len(listQuery) > 0:
                whereValue = listQuery.pop(0)
                if whereValue == "where":
                    flag = 0
                    error = 0
                    lastValue = ""
                    list3 = []
                    missing = 0
                    if len(listQuery) != 0:
                        for abc in listQuery:
                            missing = 0
                            if abc != "and":
                                if flag == 0:
                                    list3.append(abc)
                                    if checkIfIdentifier(abc):
                                        qWhereFields.append(abc)
                                        flag = 1
                                        missing = 1
                                    else:
                                        error = 1
                                        break;
                                elif flag == 1:
                                    list3.append(abc)
                                    if checkIfEqualOperator(abc):
                                        flag = 2
                                        missing = 1
                                    else:
                                        error = 2
                                        break;
                                elif flag == 2:
                                    list3.append(abc)
                                    if checkIfStringOrNumber(abc) or checkIfIdentifier(abc):
                                        qWhereValues.append(abc)
                                        flag = 0
                                    else:
                                        lastValue = abc
                                        break;
                            else:
                                missing = 1
                                list3.append(abc)
                        if error == 1:
                            print("Invalid query: identifier expected in where condition")
                            logging.warning("Invalid query: identifier expected in where condition")
                            return False
                        elif error == 2:
                            print("Invalid query: operator expected in where condition")
                            logging.warning("Invalid query: operator expected in where condition")
                            return False
                        elif missing == 1:
                            print("Invalid query: expected condition in where clause")
                            logging.warning("Invalid query: expected condition in where clause")
                            return False
                        else:
                            # #print("Successfully parsed query")
                            logging.warning("Successfully parsed query")
                            return True
                    else:
                        print("Invalid query: condition expected after where clause")
                        logging.warning("Invalid query: condition expected after where clause")
                        return False
                elif len(whereValue) > 0:
                    print("Invalid query")
                    logging.warning("Invalid query")
                    return False
                else:
                    #print("Successfully parsed query")
                    logging.warning("Successfully parsed query")
                    return True
            else:
                print("Query Parsed Successfully")
                logging.warning("Query Parsed Successfully")
                return True
        else:
            print("Invalid query: Expected keyword FROM and table name")
            logging.warning("Invalid query: Expected keyword FROM and table name")
            return False
    else:
        print("Invalid query: Expected keyword FROM and table name")
        logging.warning("Invalid query: Expected keyword FROM and table name")
        return False


def checkUpdateStep():
    tableVal = listQuery.pop(0)
    if checkIfTable(tableVal):
        setVal = listQuery.pop(0)
        if setVal == "set":
            flag = 0
            error = 0
            lastValue = ""
            list3 = []
            missing = 0
            for abc in listQuery:
                if abc != "where":
                    if abc != ",":
                        missing = 0
                        if flag == 0:
                            list3.append(abc)
                            if checkIfIdentifier(abc):
                                qUpdates.append(abc)
                                flag = 1
                                missing = 1
                            else:
                                error = 1
                                break;
                        elif flag == 1:
                            list3.append(abc)
                            if checkIfEqualOperator(abc):
                                flag = 2
                                missing = 1
                            else:
                                error = 2
                                break;
                        elif flag == 2:
                            list3.append(abc)
                            if checkIfStringOrNumber(abc) or checkIfIdentifier(abc):
                                qFields.append(abc)
                                flag = 0
                            else:
                                lastValue = abc
                                break;
                    else:
                        list3.append(abc)
                        missing = 1
                else:
                    list3.append(abc)
                    lastValue = abc
                    for v in list3:
                        listQuery.remove(v)
                    break;

            if error == 1:
                print("Invalid query: Expected identifier")
                logging.warning("Invalid query: Expected identifier")
                return False
            elif error == 2:
                print('Invalid query: Expected '"="' operator after identifier')
                logging.warning('Invalid query: Expected '"="' operator after identifier')
                return False
            elif missing == 1:
                print("Invalid query: expected values to update")
                logging.warning("Invalid query: expected values to update")
                return False

            if lastValue == "where":
                flag = 0
                error = 0
                lastValue = ""
                list3 = []
                missing = 0
                if len(listQuery) != 0:
                    for abc in listQuery:
                        missing = 0
                        if abc != "and":
                            if flag == 0:
                                list3.append(abc)
                                if checkIfIdentifier(abc):
                                    qWhereFields.append(abc)
                                    flag = 1
                                    missing = 1
                                else:
                                    error = 1
                                    break;
                            elif flag == 1:
                                list3.append(abc)
                                if checkIfEqualOperator(abc):
                                    flag = 2
                                    missing = 1
                                else:
                                    error = 2
                                    break;
                            elif flag == 2:
                                list3.append(abc)
                                if checkIfStringOrNumber(abc) or checkIfIdentifier(abc):
                                    qWhereValues.append(abc)
                                    flag = 0
                                else:
                                    lastValue = abc
                                    break;
                        else:
                            missing = 1
                            list3.append(abc)
                    if error == 1:
                        print("Invalid query: identifier expected in where condition")
                        logging.warning("Invalid query: identifier expected in where condition")
                        return False
                    elif error == 2:
                        print("Invalid query: operator expected in where condition")
                        logging.warning("Invalid query: operator expected in where condition")
                        return False
                    elif missing == 1:
                        print("Invalid query: expected condition in where clause")
                        logging.warning("Invalid query: expected condition in where clause")
                        return False
                    else:
                        #print("Successfully parsed query")
                        logging.warning("Successfully parsed query")
                        return True
                else:
                    print("Invalid query: condition expected after where clause")
                    logging.warning("Invalid query: condition expected after where clause")
                    return False
            else:
                #print("Successfully parsed query")
                logging.warning("Successfully parsed query")
                return True
        else:
            print("Invalid query: Expected set keyword")
            logging.warning("Invalid query: Expected set keyword")
            return False
    else:
        print("Invalid query: Expected table name")
        logging.warning("Invalid query: Expected table name")
        return False


def checkInsertStep(query, DB_Name):
    dirname = os.path.dirname
    path = os.path.join(dirname(dirname(__file__)))

    directoryPath = path + "/sqlDump/" + DB_Name
    #fullPath = directoryPath + "/data"
    #dumpPath = "sqlDump/" + DB_Name
    with open(directoryPath, "a") as file1:
        toFile = query + "\n"
        file1.write(toFile)

    intoVal = listQuery.pop(0)
    if intoVal == "into":
        tableVal = listQuery.pop(0)
        if checkIfTable(tableVal):
            openRoundB = listQuery.pop(0)
            if openRoundB == "(":
                flag = 1
                error = 0
                lastValue = ""
                list3 = []

                for abc in listQuery:
                    if abc != ")":
                        if flag == 1:
                            list3.append(abc)
                            if checkIfIdentifier(abc):
                                flag = 0
                            else:
                                error = 1
                                break;
                        elif flag == 0:
                            list3.append(abc)
                            if checkIfcomma(abc):
                                flag = 1
                            else:
                                lastValue = abc
                                break;
                    elif len(list3) == 0:
                        error = 1
                        break;
                    else:
                        print(list3)
                        list3.append(abc)
                        lastValue = abc
                        for v in list3:
                            if v != "," and v != ")":
                                qInserts.append(v)
                            listQuery.remove(v)
                        break;

                if error == 1:
                    print("Invalid query: expected identifier")
                    logging.warning("Invalid query: expected identifier")
                    return False

                if lastValue == ")":
                    valuesVal = listQuery.pop(0)
                    if valuesVal == "values":
                        openRoundBVal = listQuery.pop(0)
                        if openRoundBVal == "(":
                            flag = 1
                            error = 0
                            lastValue = ""
                            list3 = []
                            for abc in listQuery:
                                if abc != ")":
                                    if flag == 1:
                                        if checkIfStringOrNumber(abc) or checkIfIdentifier(abc):
                                            list3.append(abc)
                                            flag = 0
                                        else:
                                            error = 1
                                            break;
                                    elif flag == 0:
                                        list3.append(abc)
                                        if checkIfcomma(abc):
                                            flag = 1
                                        else:
                                            lastValue = abc
                                            break;
                                elif len(list3) == 0:
                                    error = 1
                                    break;
                                else:
                                    list3.append(abc)
                                    lastValue = abc
                                    for v in list3:
                                        if v != "," and v != ")":
                                            qFields.append(v)
                                        listQuery.remove(v)
                                    break;

                            if error == 1:
                                print("Invalid query: expected values to insert")
                                logging.warning("Invalid query: expected values to insert")
                                return False
                            if lastValue == ")":
                                #print("Successfully parsed query")
                                logging.warning("Successfully parsed query")
                                return True
                            else:
                                print("Invalid query")
                                logging.warning("Invalid query")
                                return False
                        else:
                            print("Invalid query: Expected round bracket")
                            logging.warning("Invalid query: Expected round bracket")
                            return False
                    else:
                        print("Invalid query: Expected keyword VALUES")
                        logging.warning("Invalid query: Expected keyword VALUES")
                        return False
                else:
                    print("Invalid query: Expected round bracket")
                    logging.warning("Invalid query: Expected round bracket")
                    return False
            else:
                print("Invalid query: Expected round bracket")
                logging.warning("Invalid query: Expected round bracket")
                return False
        else:
            print("Invalid query: Expected table name")
            logging.warning("Invalid query: Expected table name")
            return False
    else:
        print("Invalid insert query: Expected keyword INTO")
        logging.warning("Invalid insert query: Expected keyword INTO")
        return False


def checkSelectStep():
    x = listQuery.pop(0)

    if checkIfIdentifier(x):
        qFields.append(x)
        y = listQuery.pop(0)
        if checkIfcomma(y):
            checkSelectStep()
        elif checkIfFrom(y):
            z = listQuery.pop(0)
            if not checkIfTable(z):
                print("Invalid table name: " + z)
                logging.warning("Invalid table name: " + z)
                return False
        else:
            print("Keyword FROM expected")
            logging.warning("Keyword FROM expected")
            return False
    elif checkIfAsterik(x):
        qFields.append(x)
        y = listQuery.pop(0)
        if checkIfFrom(y):
            z = listQuery.pop(0)
            if not checkIfTable(z):
                print("Invalid table name: " + z)
                logging.warning("Invalid table name: " + z)
                return False
        else:
            print("Keyword FROM expected")
            logging.warning("Keyword FROM expected")
            return False
    else:
        print("Invalid query")
        logging.warning("Invalid query")
        return False

    if len(listQuery) > 0:
        nextValue = listQuery.pop(0)
        if nextValue == "where":
            flag = 0
            error = 0
            lastValue = ""
            list3 = []
            missing = 0
            if len(listQuery) != 0:
                for abc in listQuery:
                    missing = 0
                    if abc != "and":
                        if flag == 0:
                            list3.append(abc)
                            if checkIfIdentifier(abc):
                                qWhereFields.append(abc)
                                flag = 1
                                missing = 1
                            else:
                                error = 1
                                break;
                        elif flag == 1:
                            list3.append(abc)
                            if checkIfEqualOperator(abc):
                                flag = 2
                                missing = 1
                            else:
                                error = 2
                                break;
                        elif flag == 2:
                            list3.append(abc)
                            if checkIfStringOrNumber(abc) or checkIfIdentifier(abc):
                                qWhereValues.append(abc)
                                flag = 0
                            else:
                                lastValue = abc
                                break;
                    else:
                        missing = 1
                        list3.append(abc)
                if error == 1:
                    print("Invalid query: identifier expected in where condition")
                    logging.warning("Invalid query: identifier expected in where condition")
                    return False
                elif error == 2:
                    print("Invalid query: operator expected in where condition")
                    logging.warning("Invalid query: operator expected in where condition")
                    return False
                elif missing == 1:
                    print("Invalid query: expected condition in where clause")
                    logging.warning("Invalid query: expected condition in where clause")
                    return False
                else:
                    #print("Successfully parsed query")
                    logging.warning("Successfully parsed query")
                    return True
            else:
                print("Invalid query: condition expected after where clause")
                logging.warning("Invalid query: condition expected after where clause")
                return False
    else:
        #print("Successfully parsed query")
        logging.warning("Successfully parsed query")
        return True


def checkIfIdentifier(identifier):
    pattern = r'^[a-zA-Z_]\w*$'
    if (re.search(pattern, identifier)):
        for word in reserveWords:
            if word == identifier:
                return False
        for table in tableNames:
            if table == identifier:
                return False
        return True
    else:
        return False


def checkIfcomma(comma):
    if comma == ",":
        return True
    else:
        return False


def checkIfEqualOperator(equal):
    if equal == "=":
        return True
    else:
        return False


def checkIfAsterik(asterik):
    if asterik == "*":
        return True
    else:
        return False


def checkIfFrom(fromVal):
    if fromVal == "from":
        return True
    else:
        return False


def checkIfTable(tableVal):
    qTableName.append(tableVal)
    return True


def checkIfCreateTable(fromVal):
    if fromVal == "table":
        return True
    else:
        return False


def checkIfCreateDatabase(fromVal):
    if fromVal == "database":
        return True
    else:
        return False


def checkIfStringOrNumber(value):
    regnumber = re.compile(r'\d+(?:,\d*)?')
    if regnumber.match(value):
        return True
    else:
        return False

#parseQuery('CREATE TABLE Orders ( OrderName varchar(255) NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,OrderNumber int NOT NULL DEFAULT 1,PersonID int FOREIGN KEY REFERENCES Persons(PersonID));','904','Rajni')
#parseQuery('CREATE TABLE Orders ( OrderName varchar(255) NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,OrderNumber int NOT NULL DEFAULT 1,PersonID int FOREIGN KEY REFERENCES Persons ( PersonID ) );','904','Rajni')
#parseQuery('create table 904','904','Rajni')
#parseQuery('insert into ORDERS ( OrderName , PersonID ) values ( a , b )','904','Rajni');
#parseQuery('update sample set identifier = 5000 where username = '"grey07"'','904','Rajni');

#parseQuery('CREATE TABLE Orders ( OrderName varchar(255) NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,OrderNumber int NOT NULL DEFAULT Sandnes,PersonID int FOREIGN KEY REFERENCES Persons ( PersonID ) );','904','Rajni')

#parseQuery('CREATE TABLE Orders','904','Rajni')


