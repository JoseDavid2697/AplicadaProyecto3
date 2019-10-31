import sys
import csv
import time
from datetime import datetime

from conf import postgreSQL_Connection
from conf import mssql_connection,delete_sql_purchase_orders,delete_sql_clients,sqlite3_connection

#this function insert the data via bulk load into the postgre DB

def upload_csv_file(filename,table_name):
    try:
        con_postgre = postgreSQL_Connection()
        cur = con_postgre.cursor()
        with open(filename,'r') as f:
            #Salto de linea
            next(f)
            cur.copy_from(f,table_name, sep=',')
            con_postgre.commit()

            #update binnacle
            dateTimeObj = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            update_load_binnacle(filename,str(dateTimeObj),1)
            #Delete process
            delete_purchase_orders_sql()
            delete_clients_sql()
    except IOError as e:
        print("Error: {0} uploading data to PostgreSQL: {1}".format(e.errno, e.strerror))
        #update binnacle
        update_load_binnacle(filename,str(dateTimeObj),0)
    finally:
        con_postgre.close()

#these functions call a stored procedure to delete clients and purchase
#orders that has been uploaded to postgre
def delete_purchase_orders_sql():
    try:
        query = 'B65949_spDeletePurchaseData'

        #SQL Server Connection
        con_sql = mssql_connection()
        procedure = delete_sql_purchase_orders(query)
        if(len(procedure)==0):
            sys.exit(0)
        else:
            print("Delete of purchase orders completed")

    except IOError as e:
        print("Error: {0} Deleting orders from SQL: {1}".format(
            e.errno, e.strerror))
    finally:
        con_sql.close()

def delete_clients_sql():
    try:
        query = 'B65949_spDeleteClientsData'

        #SQL Server Connection
        con_sql = mssql_connection()
        procedure = delete_sql_clients(query)
        if(len(procedure)==0):
            sys.exit(0)
        else:
            print("Delete of clients data completed")

    except IOError as e:
        print("Error: {0} Deleting clients from SQL: {1}".format(
            e.errno, e.strerror))
    finally:
        con_sql.close()


#Update load binnacle in sqlite3
def update_load_binnacle(filename,dateToSqlite,loaded):
    try:
        query = "INSERT INTO loader_tb(filename,date,loaded)VALUES(?,?,?)"
        conn = sqlite3_connection()
        cur = conn.cursor()
        task = (filename,dateToSqlite,loaded)
        cur.execute(query,task)
        conn.commit()
    except IOError as e:
        print("Error: {0} Sqlite3: {1}".format(
            e.errno, e.strerror))
    finally:
        cur.close()
        conn.close()

#Execution
filename='2019-10-30-224022-B65949_CLIENT_EMAIL.csv'
table_name='b65949_client_email'
upload_csv_file(filename,table_name)
