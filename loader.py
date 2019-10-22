import sys
import csv
import time

from conf import postgreSQL_Connection

#this function insert the data via bulk load into the postgre DB
#@file_path = path of the file to get the data
#@table_name = table in postgre where data is gonna be inserted
def pg_load_table(file_path, table_name):
    '''
    This function upload csv to a target table
    '''
    try:
        conn = postgreSQL_Connection()
        print("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r")
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE'\"'".format(table_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(table_name))
        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)



