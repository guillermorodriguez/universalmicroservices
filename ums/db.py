
"""
    @Author:    Guillermo Rodriguez
    @Created:   01.25.2020
    @Purpose:   General database object to manage database connections
"""
class db(object):
    
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
        print("Database object initialized ...")
        
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
        @Ouputs:    True    -> Connection was successfully established
                    Error   -> Connection error encountered, description of error is returned
        @Purpose:   Create database connection to database server
    """
    def connect(self):
        try:
            if self.database_type == 'MySQL':
                import mysql.connector

                self.connection = self.connection = mysql.connector.connect(user=self.username,
                                                                            password = self.password,
                                                                            host=self.server,
                                                                            database=self.database)
        except Exception as ex:
            return ex

        return True

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    None
        @Outputs    None
        @Purpose:   Closes database connection
    """
    def close(self):
        try:
            self.connection.close()
        except Exception as ex:
            return ex

        return True

    """
        @Author:    Guillermo Rodriguez
        @Created:   01.25.2020
        @Inputs:    sql     -> Query to process through database engine
        @Outputs    cursor object containing object reference to result set
        @Purpose:   Create database connection to database server
    """
    def query(self, sql):
        data = []
        cursor = None

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            for entry in cursor:
                data.append(entry)

        except Exception as ex:
            return ex
        finally:
            if not cursor is None:
                cursor.close()

        return data