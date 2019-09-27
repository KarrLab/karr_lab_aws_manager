from datanator_query_python.query import query_protein
from datanator_query_python.config import config as config_mongo
from karr_lab_aws_manager.config import config
import os
import json
import math
import requests
from requests_aws4auth import AWS4Auth


class MongoToES:

    def __init__(self, profile_name='karrlab-zl', credential_path='.wc/third_party/aws_credentials',
                config_path='.wc/third_party/aws_config', elastic_path='.wc/third_party/elasticsearch.ini',
                cache_dir=None, service_name='es', index='protein', max_entries=float('inf'), verbose=False):
        ''' 
            Args:
                profile_name (:obj: `str`): AWS profile to use for authentication
                credential_path (:obj: `str`): directory for aws credentials file
                config_path (:obj: `str`): directory for aws config file
                elastic_path (:obj: `str`): directory for file containing aws elasticsearch service variables
                cache_dir (:obj: `str`): temp directory to store json for bulk upload
                service_name (:obj: `str`): aws service to be used
        '''
        session = config.establishES(config_path=config_path, profile_name=profile_name,
                                    elastic_path=elastic_path, service_name=service_name)
        self.verbose = verbose
        self.max_entries = max_entries
        self.cache_dir = cache_dir
        self.client = session.client
        self.es_endpoint = session.es_endpoint
        self.awsauth = AWS4Auth(session.access_key, session.secret_key,
                           session.region, service_name)
        self.index = index


    def data_from_mongo_protein(self, server, db, username, password, verbose=False,
                                readPreference='nearest', authSource='admin', projection={'_id': 0},
                                query={}):
        ''' Acquire documents from protein collection in datanator
            Args:
                server (:obj: `str`): mongodb ip address
                db (:obj: `str`): database name
                username (:obj: `str`): username for mongodb login
                password (:obj: `str`): password for mongodb login
                verbose (:obj: `bool`): display verbose messages
                readPreference (:obj: `str`): mongodb readpreference
                authSource (:obj: `str`): database login info is authenticating against
                projection (:obj: `str`): mongodb query projection
                query (:obj: `str`): mongodb query filter
            Return:
                docs (:obj: `pymongo.Cursor`): pymongo cursor object that points to all documents in protein collection
                count (:obj: `int`): number of documents returned
        '''
        protein_manager = query_protein.QueryProtein(server=server, database=db,
                 verbose=verbose, username=username, authSource=authSource,
                 password=password, readPreference=readPreference)
        docs = protein_manager.collection.find(filter=query, projection=projection)
        count = protein_manager.collection.count_documents(query)
        return (count, docs)

    def make_action_and_metadata(self, _id):
        ''' Make action_and_metadata obj for bulk loading
            e.g. { "index": { "_index" : "index", "_id" : "id" } }
            Args:
                index (:obj: `str`): name of index on ES
                _id (:obj: `int`):  unique id for document
            Returns:
                action_and_metadata (:obj: `dict`): metadata that conforms to ES bulk load requirement
        '''
        action_and_metadata = {'index': { "_index" : self.index, "_id" : _id }}
        return action_and_metadata
    
    def data_to_es_bulk(self, count, cursor, bulk_size=100,
                        headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service
            Args:
                count (:obj: `int`): cursor size
                cursor (:obj: `pymongo.Cursor` or :obj: `iter`): documents to be PUT to es
                action_and_metadata (:obj: `dict`): elasticsearch action_and_metadata information for bulk operations
                                    e.g. {"index": { "_index": "test", "_type" : "_doc"}}
                bulk_size (:obj: `int`): number of documents in one PUT
            Return:
                status_code (:obj: `set`): set of status codes
        '''
        url = self.es_endpoint + '/_bulk'
        status_code = {201}
        bulk_file = ''
        tot_rounds = math.ceil(count/bulk_size)
        def gen_bulk_file(i, bulk_file):
            action_and_metadata = self.make_action_and_metadata(i)
            bulk_file += json.dumps(action_and_metadata) + '\n'
            bulk_file += json.dumps(doc) + '\n'  
            return bulk_file          

        for i, doc in enumerate(cursor):
            if i == self.max_entries:
                break
            if self.verbose:
                print("Processing bulk {} out of {} ...".format(math.floor(i/bulk_size)+1, tot_rounds))
               
            if i == count - 1:  # last entry
                bulk_file = gen_bulk_file(i, bulk_file)
                # print('commit last entry')
                # print(bulk_file)
                r = requests.post(url, auth=self.awsauth, data=bulk_file, headers=headers)
                status_code.add(r.status_code)
                return status_code
            elif i % bulk_size != 0 or i == 0: #  bulk_size*(n-1) + 1 --> bulk_size*n - 1
                bulk_file = gen_bulk_file(i, bulk_file)
                # print('building in between')
                # print(bulk_file)
            else:               # bulk_size * n
                # print('commit endpoint')
                # print(bulk_file)
                r = requests.post(url, auth=self.awsauth, data=bulk_file, headers=headers)
                status_code.add(r.status_code)
                bulk_file = gen_bulk_file(i, '') # reset bulk_file
                
    def data_to_es_single(self, count, cursor, headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service
            Args:
                count (:obj: `int`): cursor size
                cursor (:obj: `pymongo.Cursor` or :obj: `iter`): documents to be PUT to es
                es_endpoint (:obj: `str`): elasticsearch endpoint
                headers (:obj: `dict`): http header information
            Return:
                status_code (:obj: `set`): set of status codes
        '''
        url_root = self.es_endpoint + '/' + self.index + '/_doc/'
        status_code = {201}
        for i, doc in enumerate(cursor):
            if i == self.max_entries:
                break
            if i % 20 == 0 and self.verbose:
                print("Processing doc {} out of {}...".format(i, min(count, self.max_entries)))
            url = url_root + str(i)
            r = requests.post(url, auth=self.awsauth, json=doc, headers=headers)
            status_code.add(r.status_code)
        return status_code


def main():
    conf = config_mongo.Config()
    username = conf.USERNAME
    password = conf.PASSWORD
    server = conf.SERVER
    authDB = conf.AUTHDB
    db = 'datanator'
    manager = MongoToES()
    
    # data from "protein" collection
    count, docs = manager.data_from_mongo_protein(server, db, username, password, authSource=authDB)
    status_code = manager.data_to_es_bulk(count, docs) 
    
    print(status_code)   

if __name__ == "__main__":
    main()