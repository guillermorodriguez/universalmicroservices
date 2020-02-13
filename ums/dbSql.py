
import sys
import helper as objHelp

"""
    @Author:    Guillermo Rodriguez
    @Created:   01.25.2020
    @Purpose:   General database object to manage database connections
"""
class dbSql():
    
    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    server          -> IP address of server
                    database        -> Name of database to connect to 
                    username        -> Account user name to authenticate through
                    password        -> Account password
                    database_type   -> The database engine type, currently only configured for MySQL
        @Outputs:   None
        @Purpose:   Constructor instance
    """
    def __init__(self, server, database, username, password, database_type = 'MySQL'):       
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.database_type = database_type

        self.connection = None

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    None
        @Outputs    None
        @Purpose:   Closes database connection
    """
    def Close(self):
        try:
            self.connection.close()
        except:
            return sys.exc_info()[0]

        return True

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    None
        @Ouputs:    True    -> Connection was successfully established
                    Error   -> Connection error encountered, description of error is returned
        @Purpose:   Create database connection to database server
    """
    def Connect(self):
        try:
            if self.database_type == 'MySQL':
                import mysql.connector

                self.connection = self.connection = mysql.connector.connect(user=self.username,
                                                                            password = self.password,
                                                                            host=self.server,
                                                                            database=self.database)
        except:
            return sys.exc_info()[0]

        return True

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.29.2020
        @Inputs:    database            -> Name of database to operate on
                    table               -> Name of table
                    columnsAndValues    -> Dictionary of column to value mapping to insert
                    schema              -> Table schema definition
        @Outputs    The result of the insert operation from the database engine
        @Purpose:   To execute an insert statement on a table given the inputs provided
    """
    def Create(self, database, table, columnsAndValues, schema):
        data = []
        cursor = None
        query = ''

        facilitate = objHelp.Helper()

        try:
            query = 'INSERT INTO %s.%s ' % (database, table)

            columns = ''
            values = ''
            for key, value in columnsAndValues.items():
                if len(columns) > 0:
                    columns += ', '
                else:
                    columns += '('

                if len(values) > 0:
                    values += ', '
                else:
                    values += '('

                columns += key 
                values += facilitate.FormatField(key, value, schema)

            if len(columns) > 0:
                columns += ')'

            if len(values) > 0:
                values = ' VALUES ' + values + ');'

            query += columns + values

            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()

            data.append({"rows": cursor.rowcount})

        except:
            print(sys.exc_info()[0])

        return data

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.29.2020
        @Inputs:    table       -> Name of table to delete from
                    condition   -> Conditions upon which to delete
                    schema      -> Table schema to operate on
        @Outputs    The result of the delete statement being executed 
        @Purpose:   To delete from a given table by providing the table name, the conditions to delete upon and the name
                    of the schema to operate on
    """
    def Delete(self, database, table, condition, schema):
        data = []
        cursor = None
        query = ''

        facilitate = objHelp.Helper()

        try:
            query = 'DELETE FROM %s.%s' % (database, table)
            clause = ''
            for key, value in condition.items():
                clause += facilitate.FormatInput(key, value, schema)

            if len(clause) > 0:
                clause = ' WHERE ' + clause 

            cursor = self.connection.cursor()
            cursor.execute(query + clause + ';')
            self.connection.commit()

            data.append({'rows': cursor.rowcount})

        except:
            print(sys.exc_info()[0])

        return data

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    sql     -> Query to process through database engine
        @Outputs    The data set by row from the query executed
        @Purpose:   Create database connection to database server
    """
    def Query(self, sql):
        data = []
        cursor = None

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            for entry in cursor:
                data.append(entry)

        except:
            return sys.exc_info()[0]
        finally:
            if not cursor is None:
                cursor.close()

        return data
    
    """
        @Author:    Guillermo Rodriguez
        @Created:   01.29.2020
                    table           -> Name of table to perform SELECT operation on
        @Inputs:    columns         -> List of column names to select from 
        @Outputs    Field values returned by the database engine
        @Purpose:   To execute a select statement by the database engine
    """
    def Read(self, table, columns):
        data = []
        cursor = None
        query = ''

        try:
            query = 'SELECT ' + ', '.join(columns) + ' FROM ' + table + ';'

            cursor = self.connection.cursor()
            cursor.execute(query)

            for entry in cursor:
                data.append(entry)

        except:
            print(sys.exc_info()[0])

        return data

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.29.2020
        @Inputs:    table               -> Name of table to operate on
                    columnsAndValues    -> Series of column and corresponding values to assign on update
                    conditions          -> Dictionary of filter condition to apply to on update
                    schema              -> Table schema to operate on
        @Outputs    
        @Purpose:   
    """
    def Update(self, database, table, columnsAndValues, conditions, schema):
        data = []
        cursor = None
        query = ''

        facilitate = objHelp.Helper()

        try:

            query = 'UPDATE  %s.%s SET ' % (database, table) 
            
            update = ''
            for key, value in columnsAndValues.items():
                if len(update) > 0:
                    update += ', '

                update += facilitate.FormatInput(key, value, schema)

            condition = ''
            for key, value in conditions.items():
                if len(condition) > 0:
                    condition += ' AND '
                else:
                    condition = ' WHERE '

                condition += facilitate.FormatInput(key, value, schema)

            cursor = self.connection.cursor()
            cursor.execute(query + update + condition + ';')
            self.connection.commit()

            data.append({'rows': cursor.rowcount})

        except:
            print(sys.exc_info()[0])

        return data
