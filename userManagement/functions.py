# author: Jigar Makwana B00842568
def display_options():
    print("\nHello! Welcome to the user CSCI5408 DBMS!")
    userSelction = input("""Please select the option
        1 - Sign In
        2 - Create a new User
        3 - Show Users
        4 - Quit\n""")
    return userSelction


def display_DBMS_options():
    print("\nHello! You are now Logged into CSCI5408 DBMS!")
    userSelction = input("""Please select the option
        1 - Execute Database Queries
        2 - Check Logs
        3 - Check Data Dictonary
        4 - Create SQL Dump
        5 - Create ERD
        6 - Logout\n""")
    return userSelction


def display_CRUD_options():
    userSelction = input("""Please select the option
        1 - Create a new Database
        2 - Use Existing Database
        3 - Perform CRUD operations
        4 - Perform Transaction
        5 - Go Back\n""")
    return userSelction
