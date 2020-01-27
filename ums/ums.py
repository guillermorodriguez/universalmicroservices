
from db import *

def GetSchema(url, schema, username, password):
    endpoints = { schema: {} }
    columns = ['ORDINAL_POSITION', 'COLUMN_KEY', 'EXTRA', 'COLUMN_NAME', 'COLUMN_TYPE', 'COLUMN_DEFAULT',
               'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'DATETIME_PRECISION']
    
    object_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "'";
    
    table_query = "SELECT " + ','.join(columns)
    table_query += " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_NAME = '[TARGET_TABLE]' ORDER BY ORDINAL_POSITION ASC;"
    
    try:
        mysql = db(url, schema, username, password)
        mysql.connect()

        object_list = mysql.query(object_query)
        for entry in object_list:
            for item in entry:
                endpoints[schema][item] = {}

                # Query individual table elements
                table_schema = mysql.query(table_query.replace('[TARGET_TABLE]', item))
                for entry in table_schema:
                    for index in range(0, len(columns)):
                        endpoints[schema][item][columns[index]] = entry[index]

        mysql.close()

    except Exception as ex:
        print(ex)

    return endpoints

if __name__ == "__main__":
    print("Starting ...")
    
    schema = GetSchema('127.0.0.1', 'world', 'ums', 'blablah')

    # Flask initialized
        

    print("Terminated ...")