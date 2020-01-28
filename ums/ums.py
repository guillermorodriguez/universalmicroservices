from flask import Flask, request
from db import *
from helper import *
import json

if __name__ == "__main__":
    print("Starting ...")

    SERVER = '127.0.0.1'
    REPOSITORY = 'world'
    USERNAME = 'ums'
    PASSWORD = 'blahBLAH001'

    facilitate = helper()
    schema = facilitate.GetSchema(SERVER, REPOSITORY, USERNAME, PASSWORD)

    # Initialize flask
    response = {
                "status": 200,
                "data": [] 
                }
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

                query += facilitate.FormatInput(entry, request.args[entry], schema[REPOSITORY][name])

            query += ";"
            
            try:
                mysql = db(SERVER, REPOSITORY, USERNAME, PASSWORD)
                mysql.connect()

                response['data'] = mysql.query(query)

                mysql.close()

            except Exception as ex:
                print(ex)

            return response 

        elif request.method == 'POST':
            # Create entry
            pass

        elif request.method == 'PUT':
            # Replace designated value
            pass
    

    @application.errorhandler(400)
    def error_400(error):
        response['status'] = 400

        return response
    
    @application.errorhandler(404)
    def error_404(error):
        response['status'] = 404

        return response 
  

    # Start web server
    application.run()
    

    print("Terminated ...")