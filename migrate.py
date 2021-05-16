import pandas as pd
from db import DB
import glob
import os

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')





class Migration:

    def __init__(self, connect_uri, data_dir, db_name):
        self.connect_uri = connect_uri
        self.data_dir = data_dir
        self.db_name = db_name

    def process_stockdata(self, data):
        data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
        return data.to_dict('records')


    def initialize_collections(self):
        '''
        migrate CSV files under given data_dir to database
        each file would be a collection, filename is the collection name
        '''

        db = DB(self.connect_uri, self.db_name)
        # drop db before migrate new data in
        db.dropdb()
        
        try:
            fnames = glob.glob("{}/*.CSV".format(self.data_dir))
        except IndexError:
            logging.error("No such dir:{}".foramt(self.data_dir))
            raise
        
        for f in fnames:
            collection = os.path.basename(f).split('.')[0]
            logging.info("Migrating Stock: {}".format(collection))

            data = pd.read_csv(f)
            db.insert(collection, values=self.process_stockdata(data))


    def update_db(self, collection, data):
        '''
        collection: str
        data: dataframe

        add new data into given collection
        '''
        db = DB(self.connect_uri, self.db_name)

        values = self.process_stockdata(data)
        db.insert(collection, values)



if __name__ == "__main__":

    test = Migration(connect_uri='mongodb://localhost:27017/', 
                    data_dir = './stocks_data',
                    db_name='stocks')

    test.initialize_collections()