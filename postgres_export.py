import json

import psycopg2
import sys


class Table:
    def __init__(self, name):
        self.name = name

    def get_columns(self):
        json_data = {}
        cur.execute("""SELECT 
            column_name, 
            column_default, 
            is_nullable, 
            data_type, 
            character_maximum_length, 
            numeric_precision 
            FROM information_schema.columns WHERE table_name=%s""", self.name)

        for column in cur.fetchall():
            json_data.update(
                {
                    column[0]: {
                        'column_default': column[1],
                        'is_nullable': column[2],
                        'data_type': column[3],
                        'character_maximum_length': column[4],
                        'numeric_precision': column[5]
                    }
                }
            )
        return json_data

    # def get_constraints():
    #     constraints = {}
    #     cur.execute("""select
    #         constraint_name,
    #         table_name,
    #         constraint_type
    #         from information_schema.table_constraints
    #         where table_schema = 'public' and constraint_type not in ('FOREIGN KEY', 'PRIMARY KEY')""")

    def get_pk(self):
        cur.execute("""SELECT 
            kc.column_name 
            FROM  
            information_schema.table_constraints tc,  
            information_schema.key_column_usage kc  
            WHERE 
            tc.constraint_type = 'PRIMARY KEY' 
            AND kc.table_name = tc.table_name AND kc.table_schema = tc.table_schema
            AND kc.constraint_name = tc.constraint_name AND tc.table_name=%s""", self.name)
        json_data = []
        for pk in cur.fetchall():
            json_data.append(pk[0])
        return json_data

    def get_fk(self):
        cur.execute("""SELECT
            tc.constraint_name, kcu.column_name, 
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
            FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=%s""", self.name)
        json_data = {}
        for fk in cur.fetchall():
            json_data.update(
                {
                    fk[0]: {
                        'column_name': fk[1],
                        'foreign_table_name': fk[2],
                        'foreign_column_name': fk[3]
                    }
                }
            )
        return json_data


try:
    conn = psycopg2.connect("dbname='AuthenticationData' user='postgres' host='localhost' password='toor'")
except:
    print("Unable to connect to the database")
    sys.exit()

cur = conn.cursor()

# get list of user tables
cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
tables = []
for table_name in cur.fetchall():
    tables.append(table_name)

# save tables information into json-file
tables_data = {}
for table_name in tables:
    table = Table(table_name)
    tables_data.update(
        {
            table_name[0]: {
                'columns': table.get_columns(),
                'pk': table.get_pk(),
                'fk': table.get_fk()
            }
        }
    )

with open('export_db.json', 'w') as outfile:
    json.dump({'AuthenticationData': tables_data}, outfile)

cur.close()
conn.close()
