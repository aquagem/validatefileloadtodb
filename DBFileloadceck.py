#!/usr/bin/env python3
##########################################################################################################################
# Author: Gautham Maroli
# Pre-requisite: Copy all the files (same type) under one directory
# Description: Script is used to verify that all the records in a delimited file is loaded successfully into the DB
#               Reads the contents of every file line by line
#               Generates a SQL : select query : Depends of the Table used
#               And returns success or failure
#               In case of Failure- It returns the File Name - Line number in the file 
#                   and the generated SQL query which can be used for further investigation
##########################################################################################################################
import os
import logging
import csv
import pyodbc

logger = logging.getLogger("DBFileloadCheckinDB")
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('validatedbload.log', 'w')  # overwrite the log file
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


def processfile(fpath, filedelimilter, header):
    server = 'server-name'
    database = 'db-name'
    username = 'db-useruname'
    password = 'db-password'
    driver = '{ODBC Driver 13 for SQL Server}'
    
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()

    # verify if the directory exits
    try:
        os.chdir(fpath)
        logger.info("File path is valid: {}".format(fpath))
        # Get the File name:
        for f in os.listdir(fpath):
            with open(f, 'r') as filetoprocess:
                delimitedfile = csv.reader(filetoprocess, delimiter=filedelimilter)
                if header:  # skip the header
                    next(delimitedfile)
                for i, row in enumerate(delimitedfile):
                    i = i + 1
                    if "'" in row[5]:
                        row[5] = row[5].replace("'", "''")  # if there is a '(single quote) in the string replace it so that query does not fail
                    query = "select * from Customers " \
                        "where" \
                        "[Id] = '" + row[0].strip() + "' and " \
                        "[FirstName]='" + row[1].strip() + "' and " \
                        "[LastName]='" + row[2].strip() + "' and " \
                        "[Email]='" + row[3].strip() + "' and " \
                        "[Gender]='" + row[4].strip() + "' and " \
                        "[Address]='" + row[5].strip() + "' and " \
                        "[PhoneNum]='" + row[6].strip()
                    logger.error("Unprocessed Record LineNum: {}, FileName: {} -- sql: {}".format(i, f, query))
                    logger.info(query)
                    cursor.execute(query)
                    datarow = cursor.fetchone()
                    if datarow:
                        if i % 10 == 0:  # This value can be changed if the file has more records
                            logger.info("Processed Records: {}".format(i))
                    else:
                        logger.error("Unprocessed Records: {} -- sql: {}".format(i, query))

    except FileNotFoundError:
        logger.error("Invalid file path: {}".format(fpath))


if __name__ == '__main__':
    processfile(input("enter the file location: "), ",", True)
