
import sys
import dbSql as objSQL

"""
    @Author:    Guillermo Rodriguez
    @Created:   01.25.2020
    @Purpose:   Helper class to house functions to facilitate some body of work
"""
class Helper():
    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    None
        @Outputs:   None
        @Purpose:   Constructor
    """
    def __init__(self):
        self.like_string = ['binary', 'blob', 'char', 'date', 'datetime', 'enum', 'set', 'text', 'time', 'timestamp', 'varbinary', 'varchar', 'year']

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.30.2020
        @Inputs:    target      -> Column name to search
                    value       -> Individual element to format
                    attributes  -> Dictionary of table attributes to search upon
        @Outputs:   None
        @Purpose:   Constructor
    """
    def FormatField(self, target, value, attributes):
        result = ""

        for entry in attributes:
            if entry['COLUMN_NAME'].lower() == target.lower():
                if entry['DATA_TYPE'].lower() in self.like_string:
                    result += "'" + value + "'"
                else:
                    result += str(value)

                break

        return result

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    target      -> Name of column to operate on
                    value       -> Value to apply to desired column
                    attributes  -> List of table detail
        @Outputs    Formated equality constraint as a string
        @Purpose:   To format a given input field and its corresponding value to a proper SQL format to apply to a query
    """
    def FormatInput(self, target, value, attributes):
        result = target + ' = '

        for entry in attributes:
            if entry['COLUMN_NAME'].lower() == target.lower():
                if entry['DATA_TYPE'].lower() in self.like_string:
                    result += "'" + value + "'"
                else:
                    result += str(value)

                break

        return result

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    columns         -> Name of columns in data set
                    data            -> Data sequence to serialize
        @Outputs    Array of hash values representing the data rows as hash elements
        @Purpose:   The purpose of this function is to take a series of columns and map those columns to the data
                    retrieved so that the data is assigned to its corresponding column name in a dictionary mapping
    """
    def FormatOutput(self, columns, data):
        result = []

        try:
            for entry in data:
                row = {}
                for index in range(0, len(entry)):
                    row[columns[index]] = entry[index]

                result.append(row)

        except:
            print(sys.exc_info()[0])

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
            mysql = objSQL.dbSql(url, schema, username, password)
            mysql.Connect()

            data = [c[0] for c in mysql.Query(query)]

            mysql.Close()

        except:
            print(sys.exc_info()[0])

        return data

    """
        @Author:    Guillermo Rodriguez
        @Created:   02.11.2020
        @Inputs:    schema      -> Table schema to search for primary key 
        @Outputs    The name of the column that is the primary key of the table schema definition
        @Purpose:   The purpose of this function is to obtain the primary key for some table defintion so that the unique
                    column for the specific table can be identified.
    """
    def GetColumnsUniqueId(self, schema):
        for entry in schema:
            if entry['COLUMN_KEY'] == 'PRI':
                return entry['COLUMN_NAME']

        return None

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
            mysql = objSQL.dbSql(url, schema, username, password)
            mysql.Connect()

            object_list = mysql.Query(object_query)
            for entry in object_list:
                for item in entry:
                    endpoints[schema][item] = []

                    # Query individual table elements
                    table_schema = mysql.Query(table_query.replace('[TARGET_TABLE]', item))
                    for entry in table_schema:
                        row = {}
                        for index in range(0, len(columns)):
                            row[columns[index]] = entry[index]

                        endpoints[schema][item].append(row)

            mysql.Close()

        except:
            print(sys.exc_info()[0])

        return endpoints


