
# TransactionGenerator.py
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
import sys
import os
import socket
import logging
import string
import datetime
import random
import uuid
import csv
import json
import math
import time
from random import randint
from random import uniform

# Add some data = Amounts and Cities.
AMOUNTS = [20, 50, 100, 200, 300, 400,500]
CITIES = [                                                                                                                                                                                                                                                     
    {"lat": 48.8534, "lon": 2.3488, "city": "Paris"},                                                                                                                                                                                                    
    {"lat": 43.2961743, "lon": 5.3699525, "city": "Marseille"},                                                                                                                                                                                                 
    {"lat": 45.7578137, "lon": 4.8320114, "city": "Lyon"},                                                                                                                                                                                                      
    {"lat": 50.6365654, "lon": 3.0635282, "city": "Lille"},
    {"lat": 44.841225, "lon": -0.5800364, "city": "Bordeaux"}
]   

class TransactionGenerator(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = '''A Python processor that creates credit card transactions for the Fraud Demo.'''

    # Define geo functions
    def create_random_point(self, x0, y0, distance):
        r = distance/111300
        u = random.random()
        v = random.random()
        w = r * math.sqrt(u)
        t = 2 * math.pi * v
        x = w * math.cos(t)
        x1 = x / math.cos(y0)
        y = w * math.sin(t)
        return (x0+x1, y0 +y)

    def create_geopoint(self, lat, lon):
        return self.create_random_point(lat, lon, 50000)

    def get_latlon(self):                                                                    
        geo = random.choice(CITIES)
        return self.create_geopoint(geo['lat'], geo['lon']),geo['city']        

    def create_fintran(self):
     
        latlon,city = self.get_latlon()
        tsbis=(datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S ")
        date = str(datetime.datetime.strptime(tsbis, "%Y-%m-%d %H:%M:%S "))
        fintran = {
          'ts': date,
          'account_id' : str(random.randint(1, 1000)),
          'transaction_id' : str(uuid.uuid1()),
          'amount' : random.randrange(1,2000),  
          'lat' : latlon[0],
          'lon' : latlon[1]
        }    
        return (fintran)

    def create_fraudtran(fintran):
        latlon,city = get_latlon()
        tsbis = str((datetime.datetime.now() - datetime.timedelta(seconds=random.randint(60,600))).strftime("%Y-%m-%d %H:%M:%S "))
        fraudtran = {
          'ts' : tsbis,
          'account_id' : fintran['account_id'],
          'transaction_id' : 'xxx' + str(fintran['transaction_id']),
          'amount' : random.randrange(1,2000),      
          'lat' : latlon[0],
          'lon' : latlon[1]
        }    
        return (fraudtran)

    def __init__(self, **kwargs):
        pass

    def create(self, context):
        fintran = self.create_fintran()   
        fintransaction =  json.dumps(fintran)
        return FlowFileSourceResult(relationship = 'success', attributes = {'NiFi': 'PythonProcessor'}, contents = fintransaction)
