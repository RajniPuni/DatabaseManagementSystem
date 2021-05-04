#author: Jigar Makwana B00842568
import csv
import json

dump_path = "sqlDump/"

def createDump(dbname):
    print("Printing the Sql dump...\n")
    with open(dump_path + dbname, 'r') as f:
        for line in f:
            print(line)