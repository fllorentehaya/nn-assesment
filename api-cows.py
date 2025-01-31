from flask import Flask, request, jsonify
from api_cows_utils import ApiPersistence
from logging_utils import logging_enrich
import api_cows_utils
import requests
import logging



app = Flask(__name__)
'''
Root of the API
'''

@app.route("/")
def hello():
    return "<p>Hi, everyone</p>"
'''
Method that return the detail of cow data by id of cow
'''
@app.route('/cows/<string:id>', methods=['GET'])
def query_cow(id):
    api_util=ApiPersistence()

    return api_util.get_cow(id=id)
'''
Method to ingest related-milk measures

'''

@app.route('/measure/sensor/milk/<string:id>', methods=['POST'])
def create_measure_milk(id):
    try:

        data=request.json
        api_util=ApiPersistence()
        api_util.create_measure_milk(sensor_id=id,cow_id=data["cow_id"],value=data["value"],date=data["date"])
        return jsonify("{'oper':'successfull measure addition'}") 
    except:
        return jsonify("{'oper':'not successfull addition'}") 

'''
Method to ingest related-weight measures

'''
@app.route('/measure/sensor/weight/<string:id>', methods=['POST'])
def create_measure_weight(id):
    try:

        data=request.json
        api_util=ApiPersistence()
        api_util.create_measure_weight(sensor_id=id,cow_id=data["cow_id"],value=data["value"],date=data["date"])
        return jsonify("{'oper':'successfull measure addition'}") 
    except:
        return jsonify("{'oper':'not successfull addition'}")     

'''
Method to create a cow register

'''
@app.route('/cows/<string:id>', methods=['POST'])
def create_cow(id):
    try:

        data = request.json 
        api_util=ApiPersistence()
        api_util.create_cow(id=id,name=data["name"],birthday=data["birthday"])
        return jsonify("{'oper':'successfull addition'}") 
    except:

         return jsonify("{'oper':'not successfull addition'}") 
    api_util=ApiPersistence()
    return api_util.test()
    #return id