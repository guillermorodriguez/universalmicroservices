import os
import sys
from flask import Flask, request
from helper import *
from dbSql import *
import json

if __name__ == "__main__":
    print("Starting ...")

    SERVER = '127.0.0.1'
    REPOSITORY = 'world'
    USERNAME = 'ums'
    PASSWORD = 'blahBLAH001'

    facilitate = Helper()
    schema = facilitate.GetSchema(SERVER, REPOSITORY, USERNAME, PASSWORD)

    # Initialize flask
    application = Flask(__name__)
    @application.route("/api/" + REPOSITORY + '/<name>', methods=['DELETE', 'GET', 'POST', 'PUT'])
    def index(name):
        response = {
                    "status": 200,
                    "data": []
                    }

        if request.method == 'DELETE':
            # Remove entry
            try:
                data = {}
                for key, value in request.form.to_dict(flat=False).items():
                    data[key] = value[0]

                mysql = dbSql(SERVER, REPOSITORY, USERNAME, PASSWORD)
                mysql.Connect()

                response['data'] =  mysql.Delete(REPOSITORY, name, data, schema[REPOSITORY][name])
                
                mysql.Close()
            except:
                response['error'] = sys.exc_info()[0]
                response['status'] = 500

            return response
  
        elif request.method == 'GET':
            # Retrieve entry
            try:
                data = {}
                for key, value in request.args.items():
                    data[key] = value

                mysql = dbSql(SERVER, REPOSITORY, USERNAME, PASSWORD)
                mysql.Connect()

                response['data'] = mysql.Read(REPOSITORY, name, ['*'], data, schema[REPOSITORY][name])

                mysql.Close()

            except:
                print(sys.exc_info()[0])

            response['data'] = facilitate.FormatOutput(facilitate.GetColumnNames(SERVER, REPOSITORY, USERNAME, PASSWORD, name), response['data'])

            return response 
        
        elif request.method == 'POST':
            # Create entry            
            try:
                data = {}
                for key, value in request.form.to_dict(flat=False).items():
                    data[key] = value[0]

                mysql = dbSql(SERVER, REPOSITORY, USERNAME, PASSWORD)
                mysql.Connect()


                response['data'] =  mysql.Create(REPOSITORY, name, data, schema[REPOSITORY][name])
                
                mysql.Close()
            except:
                response['error'] = sys.exc_info()[0]
                response['status'] = 500

            return response

        elif request.method == 'PUT':
            # Replace designated value
            try:
                data = {}
                for key, value in request.form.to_dict(flat=False).items():
                    data[key] = value[0]

                mysql = dbSql(SERVER, REPOSITORY, USERNAME, PASSWORD)
                mysql.Connect()

                condition = {}
                id = facilitate.GetColumnsUniqueId(schema[REPOSITORY][name])
                if id is not None:
                    condition[id] = data[id]
                    del data[id]

                if len(condition) > 0:
                    # Perform Update
                    response['data'] =  mysql.Update(REPOSITORY, name, data, condition, schema[REPOSITORY][name])
                
                mysql.Close()
            except:
                response['error'] = sys.exc_info()[0]
                response['status'] = 500

            return response
    

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
