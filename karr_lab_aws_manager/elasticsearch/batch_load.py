from datanator_query_python.query import query_protein
from datanator_query_python.config import config as config_mongo
from karr_lab_aws_manager.config import config
import os
import json
import requests
from requests_aws4auth import AWS4Auth


class MongoToES:

    def __init__(self, profile_name='karrlab-zl', credential_path='.wc/third_party/aws_credentials',
                config_path='.wc/third_party/aws_config', elastic_path='.wc/third_party/elasticsearch.ini',
                cache_dir=None, service_name='es'):
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
        self.cache_dir = cache_dir
        self.client = session.client
        self.es_endpoint = session.es_endpoint
        self.awsauth = AWS4Auth(session.access_key, session.secret_key,
                           session.region, service_name)


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
        '''
        protein_manager = query_protein.QueryProtein(server=server, database=db,
                 verbose=verbose, username = username, authSource=authSource,
                 password = password, readPreference=readPreference)

        docs = protein_manager.collection.find(filter=query, projection=projection)
        return docs

    
    def data_to_es_bulk(self, cursor, action_and_metadata, bulk_size=100,
                   file_name='es_temp.json', headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service
            Args:
                cursor (:obj: `pymongo.Cursor` or :obj: `iter`): documents to be PUT to es
                action_and_metadata (:obj: `dict`): elasticsearch action_and_metadata information for bulk operations
                                    e.g. {"index": { "_index": "test", "_type" : "_doc"}}
                bulk_size (:obj: `int`): number of documents in one PUT
        '''
        file_dir = os.path.join(self.cache_dir, file_name)
        url = self.es_endpoint + '/_bulk'
        bulk_file = ''
        for i, doc in enumerate(cursor):
            action_and_metadata['index']['_id'] = i
            if i % bulk_size != 0 or i == 0:
                bulk_file += json.dumps(action_and_metadata) + '\n'
                bulk_file += json.dumps(doc) + '\n'
            else:
                bulk_file += json.dumps(action_and_metadata) + '\n'
                bulk_file += json.dumps(doc) + '\n'
                r = requests.post(url, json=bulk_file, headers=headers)
                
    def data_to_es_single(self, cursor, index='proteins',
                          headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service
            Args:
                cursor (:obj: `pymongo.Cursor` or :obj: `iter`): documents to be PUT to es
                es_endpoint (:obj: `str`): elasticsearch endpoint
                index (:obj: `str`): index collection
                headers (:obj: `dict`): http header information
        '''
        url_root = self.es_endpoint + '/' + index + '/_doc/'
        for i, doc in enumerate(cursor):
            url = url_root + str(i)
            r = requests.post(url, auth=self.awsauth, json=doc, headers=headers)
