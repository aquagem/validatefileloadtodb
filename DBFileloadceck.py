import os
import logging
import csv

logger = logging.getLogger("validatecsvfileloadtoDB")
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
                    logger.info(query)

    except FileNotFoundError:
        logger.error("Invalid file path: {}".format(fpath))


if __name__ == '__main__':
    processfile(input("enter the file location: "), ",", True)
