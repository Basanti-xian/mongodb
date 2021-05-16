from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')


class DB:

    def __init__(self, connect_uri, db_name):
        self.connect_uri = connect_uri
        self.db_name = db_name
        self.mydb = self._connect()

    def _connect(self):
        uri = self.connect_uri
        db_name = self.db_name
        logging.info("Connecting to %s with db_name=%s" % (uri, db_name))
        myclient = MongoClient(uri)

        return myclient[db_name]


    def dropdb(self):
        uri = self.connect_uri
        db_name = self.db_name
        logging.info("Dropping Datbase: %s" % (db_name))
        myclient = MongoClient(uri)
        myclient.drop_database(db_name)


    def _check_collection_exist(self, collection):
        collist = self.mydb.list_collection_names()
        if collection not in collist:
            logging.error("The collection not exist")
            

    def insert(self, collection, values):
        '''
        collection: str
        values: list of dict
        '''
        mycol = self.mydb[collection]
        record = mycol.insert_many(values)
        
        return record.inserted_ids

    
    def filter(self, collection, query={}, return_df=True):
        '''
        collection: str
        query: dict ex. { "timestamp": { "$gt": "2021-04" } }
        '''
        self._check_collection_exist(collection)

        mycol = self.mydb[collection]
        cursor = list(mycol.find(query))

        if return_df:
            return pd.DataFrame(cursor)
        else:
            return dumps(cursor) 


    def find(self, collection, fields, return_df=True):
        '''
        collection: str
        fields: list of string ex. ['open', 'high']
        '''      
        self._check_collection_exist(collection)

        mycol = self.mydb[collection]
        fields_dict = dict.fromkeys(fields, 1)
        cursor = list(mycol.find({}, fields_dict))

        if return_df:
            return pd.DataFrame(cursor)
        else:
            return dumps(cursor) 


    def update(self, collection, query, newvalues):
        '''
        collection: str
        query: dict ex. { "timestamp": { "$gt": "2021-04" } }
        newvalues: dict ex. { "$set": { "open": "124" } }
        '''
        self._check_collection_exist(collection)

        mycol = self.mydb[collection]
        # update all documents that meets the criteria of the query
        record = mycol.update_many(query, newvalues)

        return record.modified_count


    def delete(self, collection, query):
        '''
        collection: str
        query: dict ex. { "timestamp": { "$gt": "2021-04" } }
        '''
        self._check_collection_exist(collection)

        mycol = self.mydb[collection]
        # delete all documents that meets the criteria of the query
        # if query={}, then delete all
        record = mycol.delete_many(query)

        return record.deleted_count




if __name__ == "__main__":

    mydb = DB(connect_uri='mongodb://localhost:27017/', 
              db_name='stocks')

    print(mydb.filter(collection='ABT', query={ "timestamp": { "$gt": "2021-04" } }))
    print(mydb.find(collection='CTRN', fields=['open', 'time']))

    # values = [{'timestamp': '2021-04-16', 'open': 124.39, 'high': 124.6074, 'low': 122.95, 'close': 124.35, 'adjusted_close': 124.35, 'volume': 6243068, 'dividend_amount': 0.0, 'split_coefficient': 1.0}, 
    #         {'timestamp': '2021-04-15', 'open': 122.4, 'high': 124.68, 'low': 121.9, 'close': 123.94, 'adjusted_close': 123.94, 'volume': 4510894, 'dividend_amount': 0.0, 'split_coefficient': 1.0}, 
    #         {'timestamp': '2021-04-14', 'open': 122.55, 'high': 122.83, 'low': 121.285, 'close': 121.5, 'adjusted_close': 121.5, 'volume': 4726734, 'dividend_amount': 0.45, 'split_coefficient': 1.0}, 
    #         {'timestamp': '2021-04-13', 'open': 121.05, 'high': 123.64, 'low': 120.91, 'close': 123.01, 'adjusted_close': 122.55608856100001, 'volume': 4944569, 'dividend_amount': 0.0, 'split_coefficient': 1.0}, 
    #         {'timestamp': '2021-04-12', 'open': 120.56, 'high': 121.369, 'low': 120.48, 'close': 121.04, 'adjusted_close': 120.593357934, 'volume': 2915975, 'dividend_amount': 0.0, 'split_coefficient': 1.0}]
    # print(mydb.insert(collection='ABT', values=values))

    # print(mydb.insert(collection='Test', values=values))
    