from flask import Flask, request
from db import *
import json

"""
    @Author:    Guillermo Rodriguez
    @Created:   01.25.2020
    @Inputs:    url         -> The server name
                schema      -> The database schema to connect to 
                username    -> Account user name
                password    -> Account password
    @Outputs    Dictionary of dictionary objects containing the mapping between schema to tables to table attribute mapping
    @Purpose:   Function to retrieve database relationships for some desired schema
"""
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
                endpoints[schema][item] = []

                # Query individual table elements
                table_schema = mysql.query(table_query.replace('[TARGET_TABLE]', item))
                for entry in table_schema:
                    row = {}
                    for index in range(0, len(columns)):
                        row[columns[index]] = entry[index]

                    endpoints[schema][item].append(row)

        mysql.close()

    except Exception as ex:
        print(ex)

    return endpoints

def FormatInput(target, value, attributes):
    result = target + ' = '

    for entry in attributes:
        if entry['COLUMN_NAME'].lower() == target.lower():
            if entry['DATA_TYPE'].lower() in ['binary', 'blob', 'char', 'date', 'datetime', 'enum', 'set', 'text', 'time', 'timestamp', 'varbinary', 'varchar', 'year']:
                result += "'" + value + "'"
            else:
                result += str(value)

            break

    return result

def GetData(url, schema, username, password, query):
    result = None

    try:
        mysql = db(url, schema, username, password)
        mysql.connect()

        result = mysql.query(query)

        mysql.close()

    except Exception as ex:
        print(ex)

    return result 
  
if __name__ == "__main__":
    print("Starting ...")

    SERVER = '127.0.0.1'
    REPOSITORY = 'world'
    USERNAME = 'ums'
    PASSWORD = 'blahBLAH001'

    schema = GetSchema(SERVER, REPOSITORY, USERNAME, PASSWORD)

    # Initialize flask 
    application = Flask(__name__)
    @application.route("/api/" + REPOSITORY + '/<name>', methods=['DELETE', 'GET', 'POST', 'PUT'])
    def index(name):
        if request.method == 'DELETE':
            # Remove entry
            pass
        elif request.method == 'GET':
            # Retrieve entry
            query = "SELECT * FROM %s.%s" % (REPOSITORY, name)
            values = {}
            for entry in request.args:
                if not "WHERE" in query:
                    query += " WHERE "
                else:
                    query += " AND "

                query += FormatInput(entry, request.args[entry], schema[REPOSITORY][name])

            query += ";"
    
            return json.dumps( GetData(SERVER, REPOSITORY, USERNAME, PASSWORD, query) )

        elif request.method == 'POST':
            # Create entry
            pass

        elif request.method == 'PUT':
            # Replace designated value
            pass
        

    # Start web server
    application.run()
    

    print("Terminated ...")