import sys 
import time
import csv
from datetime import datetime

from conf import mssql_connection, get_data_from_sql, sqlite3_connection

#RETRIEVE DATA FROM SQL IN ORDER TO GENERATE A .CSV FILE
def extractor(table_name):
    try:
        query = 'B65949_spGetAllData'

        #SQL Server Connection
        con_sql = mssql_connection()
        data = get_data_from_sql(query,table_name)

        if(len(data) == 0):
            print('No data retrieved')
            dateTimeObj = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            update_extractor_binnacle(table_name,dateTimeObj,0)
            sys.exit(0)
        else:
            access = "w"
            newline = {"newline": ""}
            # Returns a datetime object containing the local date and time
            dateTimeObj = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            filename = str(dateTimeObj)+"-"+str(table_name)+".csv"
            with open(filename, access, **newline) as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(
                    ["","",""])
                
                for row in data:
                    print(row)
                    writer.writerow(row[:-2])

            #Update the binnacle
            update_extractor_binnacle(filename,dateTimeObj,1) 
    except IOError as e:
        print("Error: {0} Getting data from MSSQL: {1}".format(
            e.errno, e.strerror))
        dateTimeObj = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        update_extractor_binnacle(table_name,dateTimeObj,0) 
    finally:
        con_sql.close()
         

#Update extraxt binnacle in sqlite3
def update_extractor_binnacle(filename,dateToSqlite,extracted):
    try:
        query = "INSERT INTO extractor_tb(filename,date,extracted)VALUES(?,?,?)"
        conn = sqlite3_connection()
        cur = conn.cursor()
        task = (filename,dateToSqlite,1)
        cur.execute(query,task)
        conn.commit()
    except IOError as e:
        print("Error: {0} Sqlite3: {1}".format(
            e.errno, e.strerror))
    finally:
        cur.close()
        conn.close()


# Execution Example
table_name = 'B65949_CLIENT_EMAIL'
extractor(table_name)