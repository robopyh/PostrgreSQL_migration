import json
import pyodbc
import psycopg2

# connect to ms sql server
tsql_conn = pyodbc.connect(r'DRIVER={SQL Server};'
                      r'server=DESKTOP-JPE4CJJ;'
                      r'database=AuthenticationData;'
                      r'Trusted_Connection=yes;',
                      autocommit=True)
tsql_cur = tsql_conn.cursor()

# connect to postgresql
psql_conn = psycopg2.connect("dbname='AuthenticationData' user='postgres' host='localhost' password='toor'")
psql_cur = psql_conn.cursor()

# get db structure
json_data = json.load(open('export_db.json'))
db_name = list(json_data.keys())[0]
json_data = json_data[db_name]

for table in json_data.keys():
    psql_cur.execute('select * from ' + table)
    for row in psql_cur:
        data = [data for data in row[1:]]
        tsql_cur.execute('insert into ' + table + '(' + ','.join(col for col in json_data[table]['columns'] if col not in json_data[table]['pk']) + ') values (' + ','.join('?' for i in range(len(row[1:]))) + ')', data)
