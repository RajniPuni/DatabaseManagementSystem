from os import path, listdir
import logging
import queryParser.queryParser as qp
from dataDictonary.createMetaData import createMetaData

db_path = "database/"

logging.basicConfig(format='%(asctime)s - %(message)s', filename='logs/eventlogs.log')
db = ''


def q_set_db_name(dbname):
    global db
    db = dbname


def qExecuteQuery(username):
    db_main = db_path + db
    if path.exists(db_main):

        # database: validation
        print("Into the database: ", db)
        logging.warning('User entered valid database: ' + db_main)
        qp.tableNames = listdir(db_path + db)

        # query: validation and parsing
        while True:
            query = input(">> ")
            try:
                if query == 'q':
                    break

                parser = qp.parseQuery(query, db, username)
                # print(parser)

                # query: select
                if parser['Type'] == 'select':
                    rows = []
                    with open(db_main + parser['Table'][0].lower()) as table:
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
                    with open(db_main + '/' + parser['Table'][0].lower() + '.csv', 'a', newline='') as table:
                        row = ','.join(col for col in parser['InsertUpdateData'])
                        table.write(row + "\n")
                        logging.warning('Insert query executed successfully.')
                        print("Successfully inserted the data")

                # query: update
                elif parser['Type'] == 'update':
                    rows = []
                    table_data_rows = []
                    with open(db_main + parser['Table'][0].lower()) as table:
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
                    with open(db_main + parser['Table'][0].lower(), 'w') as table:
                        for row in rows:
                            table.write(row)
                    logging.warning('Update query executed successfully.')
                    print("Updated successfully")

                # query: delete
                elif parser['Type'] == 'delete':
                    rows = []
                    table_data_rows = []
                    with open(db_main + parser['Table'][0].lower()) as table:
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
                    createMetaData(db, parser)
                    header = ''
                    for index in range(len(parser["Columns"])):
                        for key in parser["Columns"][index]:
                            if(key == "Name"):
                                if(index == 0):
                                    header = (parser["Columns"][index]["Name"])
                                else:
                                    header = header + ',' + (parser["Columns"][index]["Name"])
                    with open(db_main + '/' + parser['Table'][0].lower() + '.csv', 'w') as table:
                        table.write(header + "\n")
                        # print(table)

            except Exception as e:
                print(e)
                logging.error('Query execution failed on database: ' + db)
                print("Operation failed..")

    else:
        logging.warning('User entered invalid database name.')
        print("Invalid database name")
