import sys
import os
import json
import pymongo

import pandas as pd
import numpy as np
from network_security.exceptions.exceptions import NetworkSecurityException
from network_security.logging.logger import logging

from dotenv import load_dotenv
import certifi

load_dotenv()

MONGO_DB_URL = os.getenv('MONGO_DB_URL')

ca = certifi.where()

class NetworkDataExtract():
    def __init__(self):

        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def csv_to_json(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collections):
        try:
            self.database = database
            self.collection = collections
            self.records = records

            self.monog_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.monog_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)

            return (len(self.records))
        
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
if __name__ == '__main__':
    FILE_PATH = 'Data\phisingData.csv'
    DATABASE = 'YashML'
    Collection = 'NetworkData'
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json(file_path=FILE_PATH)
    no_of_records = networkobj.insert_data_mongodb(records=records, database=DATABASE, collections=Collection)
    print(no_of_records)
    


