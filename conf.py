import pymssql
import psycopg2
import sqlite3

#ConnectionToSQL
#Local SQL Connection
_sql_server = "localhost"
_sql_database = "B65949_DB_XYZ"
_sql_server_port = 1433
_sql_user = "jose"
_sql_password = "1234"

#ConnectionToPostGreSQL
_postgre_server = "localhost"
_postgre_database = "B65949_DB_XYZ"
_postgre_server_port = 65291
_postgre_user = "postgres"
_postgre_password = "123"

#SQL Connection Prod
def mssql_connection():
    try:
        cnx = pymssql.connect(server=_sql_server, port=_sql_server_port,
                            user=_sql_user, password=_sql_password,database=_sql_database)
        return cnx
    except:
        print('Error: MSSQL Connection')

# Postgre Connection
def postgreSQL_Connection():
    try:
        cnx = psycopg2.connect("host="+_postgre_server+" dbname="+_postgre_database+" user="+_postgre_user+" password="+_postgre_password)
        
        return cnx
    except:
        print('Error: PostgreSQL Connection')

#Extracting data 
def get_data_from_sql(sp,table_name):
    try:
        con = mssql_connection()
        cur = con.cursor()
        cur.execute("execute {} @from_date = '2019-10-30', @to_date = '2019-10-31', @table_name = {}".format(sp,table_name))
        data_return = cur.fetchall()
        con.commit()

        return data_return
    except IOError as e:
        print("Error {0} Getting data from SQL Server: {1}".format(
            e.errno, e.strerror))

#Deleting SQL Purchase Orders
def delete_sql_purchase_orders(sp):
    try:
        con = mssql_connection()
        cur = con.cursor()
        cur.execute("execute {}".format(sp))
        data_return = cur.fetchall()
        con.commit()

        return data_return
    except IOError as e:
        print("Error {0} Deleting data from sql: {1}".format(
            e.errno, e.strerror))

#Deleting SQL Clients
def delete_sql_clients(sp):
    try:
        con = mssql_connection()
        cur = con.cursor()
        cur.execute("execute {}".format(sp))
        data_return = cur.fetchall()
        con.commit()

        return data_return
    except IOError as e:
        print("Error {0} Deleting data from sql: {1}".format(
            e.errno, e.strerror))

def sqlite3_connection():
    try:
        conn = sqlite3.connect('B65949_Binnacle.db')
        return conn
    except:
        print('Error connecting to sqlite db')
