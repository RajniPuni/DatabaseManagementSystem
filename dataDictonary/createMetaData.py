dd_path = "dataDictonary/"
import os
import csv
import json

def writeToJson(data, filename): 
    with open(filename,'w') as f: 
        json.dump(data, f, indent=4)
    print(f"\n{filename} file updated!") 

def createMetaData(db_name, parseCreateData):
    tableName = parseCreateData['Table'][0].lower()
    dbMetaDataPath = dd_path + db_name + 'MetaData.csv'
    mapEntry = [tableName]
    mapData = []
    mapData.append(mapEntry)
    myFile = open(dbMetaDataPath, 'a', newline='')
    with myFile:
        writer = csv.writer(myFile)
        writer.writerows(mapData)
    print(f"\n{dbMetaDataPath} updated!")

    filePath = dd_path + tableName + 'MetaData.json'
    writeToJson(parseCreateData, filePath)


# def createMetaData(db_name, parseCreateData):
#     filePath = dd_path + db_name + '.json'
#     if not os.path.exists(filePath):
#         erdJsonArry = { "metaData" :[]}
#         writeToJson(erdJsonArry, filePath)
    
#     with open(filePath) as json_file: 
#         data = json.load(json_file) 
#         temp = data['metaData'] 
#         temp.append(parseCreateData)    
#     writeToJson(data) 
#     print("\nmetadata file updated!")


def createDBUserMap(dbName, userName):
    mapEntry = [dbName, userName]
    mapData = []
    mapData.append(mapEntry)
    myFile = open(dd_path + 'dbUserMap.csv', 'a', newline='')
    with myFile:
        writer = csv.writer(myFile)
        writer.writerows(mapData)
    print("\ndbUserMap.csv updated!")