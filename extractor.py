import sys 
import time
import csv
from datetime import datetime

from conf import mssql_connection, get_data_from_sql
from loader import pg_load_table

#RETRIEVE DATA FROM SQL IN ORDER TO GENERATE A .CSV FILE
def extractor(table_name):
    try:
        query = 'B65949_spGetAllData'

        #SQL Server Connection
        con_sql = mssql_connection()
        data = get_data_from_sql(query,table_name)

        if(len(data) == 0):
            print('No data retrieved')
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
                    ["","","",""])
                
                for row in data:
                    print(row)
                    writer.writerow(row[:-2])
    except IOError as e:
        print("Error: {0} Getting data from MSSQL: {1}".format(
            e.errno, e.strerror))
    finally:
        con_sql.close()
        return filename        




# Execution Example
table_name = 'B65949_CLIENT_ADDRESS'

delimiter = chr(92)
file_path = r"C:\Users\Jose David\Documents\My Projects\PROYECTO 3 APLICADA"
filenameToLoad = str(extractor(table_name))
#Bulk load data to postgre DB
path = file_path+delimiter+filenameToLoad
pg_load_table(path,table_name.lower())

