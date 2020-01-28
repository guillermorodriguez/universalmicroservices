
from db import *

"""
    @Author:    Guillermo Rodriguez
    @Created:   01.25.2020
    @Purpose:   Helper class to house functions to facilitate some body of work
"""
class helper(object):
    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
    """
    def __init__(self):
        pass

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    
        @Outputs    
        @Purpose:   
    """
    def FormatInput(self, target, value, attributes):
        result = target + ' = '

        for entry in attributes:
            if entry['COLUMN_NAME'].lower() == target.lower():
                if entry['DATA_TYPE'].lower() in ['binary', 'blob', 'char', 'date', 'datetime', 'enum', 'set', 'text', 'time', 'timestamp', 'varbinary', 'varchar', 'year']:
                    result += "'" + value + "'"
                else:
                    result += str(value)

                break

        return result

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    
        @Outputs    
        @Purpose:   
    """
    def FormatOutput(self, columns, data):
        result = []

        try:
            for entry in data:
                row = {}
                for index in range(0, len(entry)):
                    row[columns[index]] = entry[index]

                result.append(row)

        except Exception as ex:
            print(ex)

        return result

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    url         -> The server name
                    schema      -> The database schema to connect to 
                    username    -> Account user name
                    password    -> Account password
                    table       -> Table name to filter column values on
        @Outputs    Listing of column names that a specific table has
        @Purpose:   To retrieve the names of all the columns a specific table contains as a list
    """
    def GetColumnNames(self, url, schema, username, password, table):

        data = []

        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + schema + "' " 
        query += "and TABLE_NAME = '" + table + "' ORDER BY ORDINAL_POSITION ASC;"

        try:
            mysql = db(url, schema, username, password)
            mysql.connect()

            data = [c[0] for c in mysql.query(query)]

            mysql.close()

        except Exception as ex:
            print(ex)

        return data

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
    def GetSchema(self, url, schema, username, password):
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


