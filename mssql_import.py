import json
import pyodbc as pyodbc


def connect():
    conn = pyodbc.connect(r'DRIVER={SQL Server};'
                          r'server=DESKTOP-JPE4CJJ;'
                          r'database=AuthenticationData;'
                          r'Trusted_Connection=yes;',
                          autocommit=True)
    return conn.cursor()


def create_database():
    connection = pyodbc.connect(r'DRIVER={SQL Server};'
                                r'server=DESKTOP-JPE4CJJ;'
                                r'database=master;'
                                r'Trusted_Connection=yes;',
                                autocommit=True)
    cursor = connection.cursor()
    cursor.execute('create database ' + db_name)
    cursor.close()
    connection.close()


types = {'bigint': 'bigint', 'bytea': 'binary', 'boolean': 'bit', 'char': 'char', 'character': 'character',
         'date': 'date', 'timestamp': 'datetime', 'double precision': 'double precision', 'int': 'int',
         'integer': 'integer', 'numeric': 'numeric', 'varchar': 'varchar', 'character varying': 'varchar',
         'real': 'real', 'text': 'text', 'time': 'time'}


json_data = json.load(open('export_db.json'))
db_name = list(json_data.keys())[0]
json_data = json_data[db_name]

try:
    cur = connect()
except:
    create_database()
    cur = connect()


for table in json_data.keys():
    # make 'create table' query
    create_str = 'create table ' + table + '(\n'
    for col in json_data[table]['columns']:
        col_data = json_data[table]['columns'][col]
        create_str += col + ' ' + types[col_data['data_type']]
        if col_data['character_maximum_length'] is not None:
            create_str += '(' + str(col_data['character_maximum_length']) + ')'
        # check primary keys
        if col in json_data[table]['pk']:
            create_str += ' primary key identity(1,1)'
        if col_data['is_nullable'] == 'NO':
            create_str += ' NOT NULL'
        create_str += ',\n'
    create_str = create_str[:-2] + ');'
    cur.execute(create_str)
    print(create_str)

    # create foreign keys
    for fk in json_data[table]['fk']:
        fk_str = 'alter table ' + table + ' add constraint ' + fk + \
                 ' foreign key (' + json_data[table]['fk'][fk]['column_name'] + ') references ' + \
                 json_data[table]['fk'][fk]['foreign_table_name'] + '(' + \
                 json_data[table]['fk'][fk]['foreign_column_name'] + ');' # + \
                 #' on delete cascade;'
        cur.execute(fk_str)
        print(fk_str)

cur.close()
