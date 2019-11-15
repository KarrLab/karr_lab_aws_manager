from elasticsearch import Elasticsearch, RequestsHttpConnection
from karr_lab_aws_manager.config import config
import requests
import json
import math
from pathlib import Path
from requests_aws4auth import AWS4Auth
import re
import datetime


class EsUtil(config.establishES):

    def __init__(self, profile_name=None, credential_path=None,
                config_path=None, elastic_path=None,
                cache_dir=None, service_name='es', max_entries=float('inf'), verbose=False):
        ''' 
            Args:
                profile_name (:obj:`str`): AWS profile to use for authentication
                credential_path (:obj:`str`): directory for aws credentials file
                config_path (:obj:`str`): directory for aws config file
                elastic_path (:obj:`str`): directory for file containing aws elasticsearch service variables
                cache_dir (:obj:`str`): temp directory to store json for bulk upload
                service_name (:obj:`str`): aws service to be used
        '''
        super().__init__(config_path=config_path, profile_name=profile_name, credential_path=credential_path,
                        elastic_path=elastic_path, service_name=service_name)
        self.verbose = verbose
        self.max_entries = max_entries
        self.cache_dir = cache_dir

    def unassigned_reason(self):
        """sends http request to get why a shard is unassigned

        Returns:
            (HTTPResponse): http response    
        """
        uri = self.es_endpoint + '/' + '_cat/shards?h=index,shard,prirep,state,unassigned.reason'
        r = requests.get(uri, auth=self.awsauth)
        return r
    
    def index_settings(self, index, number_of_replicas, number_of_shards=1, 
                      headers={ "Content-Type": "application/json" }):
        """Setting index's shard and replica number in es cluster
        
        Args:
            index (str): name of index to be set
            number_of_replicas (int): number of replica shards to be used for the index
            number_of_shards (int): number of primary shards contained in the es cluster
            headers (dict): http request content header description

        Returns:
            (HTTPResponse): http response
        """
        url = self.es_endpoint + '/' + index + '/_settings'
        if number_of_shards == 1:
            body = {"index": {"number_of_replicas": number_of_replicas}}
        else:
            body = {"index": {"number_of_replicas": number_of_replicas,
                            "number_of_shards": number_of_shards}}
        r = requests.put(url, auth=self.awsauth, data=json.dumps(body), headers=headers)
        return r

    def create_index(self, index, setting={"settings": {"number_of_shards": 1}}):
        """Create index
            (https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html)

        Args:
            index (:obj:`str`): name of index
            setting (:obj:`dict`, optional): index settings. Defaults to {"settings": {"number_of_shards": 1}}.
            headers (:obj:`dict`): http request content header description. Defaults to { "Content-Type": "application/json" }.
        """
        url = self.es_endpoint + '/' + index
        r = requests.put(url, auth=self.awsauth, json=setting)
        return r

    def _build_es(self, suffix=None):
        ''' build es query object

            Args:
                suffix (:obj:`str`): string trailing es endpoint

            Returns:
                (:obj:`Elasticsearch`): Elasticsearch object
        '''
        if suffix is None:
            uri = self.es_endpoint.split('https://')[1]
        else:
            uri = self.es_endpoint.split('https://')[1] + suffix 
        
        es = Elasticsearch(
            hosts = [{'host': uri, 'port': 443}],
            http_auth = self.awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )
        return es

    def add_field_to_index(self, index, field, value):
        """Add a field of value to all documents in index
        
        Args:
            index (:obj:`str`): name of index.
            field (:obj:`str`): name of field.
            value (:obj:`Obj`): value of field.

        Return:
            (:obj:`HTTPResponse`): elasticsearch update status description.
        """
        url = self.es_endpoint + '/' + index + '/' + '_update_by_query'
        script = "ctx._source.{} = {}".format(field, value)
        body = {
                "query": {
                    "match_all": {}
                },
                "script": { "source": script, "lang": "painless" }
                }
        r = requests.post(url, auth=self.awsauth, json=body)
        return r
    
    def make_action_and_metadata(self, index, _id):
        ''' Make action_and_metadata obj for bulk loading
            e.g. { "index": { "_index" : "index", "_id" : "id" } }

            Args:
                index (:obj:`str`): name of index on ES
                _id (:obj:`str`):  unique id for document

            Returns:
                (:obj:`dict`): metadata that conforms to ES bulk load requirement
        '''
        action_and_metadata = {'index': { "_index" : index, "_id" : _id }}
        return action_and_metadata

    def delete_index(self, index, _id=None):
        ''' Delete elasticsearch index

            Args:
                index (:obj:`str`): name of index in es
                _id (:obj:`int`): id of the doc in index (optional)
        '''
        if _id is None:
            url = self.es_endpoint + '/' + index
        else:
            url = self.es_endpoint + '/' + index + '/_doc/' + _id
        r = requests.delete(url, auth=self.awsauth)
        return r.status_code

    def data_to_es_bulk(self, cursor, index='test', count=None, bulk_size=100, _id='uniprot_id',
                        headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service

            Args:
                count (:obj:`int`): cursor size
                cursor (:obj:`pymongo.Cursor` or :obj:`iter`): documents to be PUT/POST to es
                index (:obj:`str`): name of unique key to be used as index for es
                bulk_size (:obj:`int`): number of documents in one PUT
                headers (:obj:`dict`): http header
                _id (:obj:`str`): key in mogno collection for identification

            Returns:
                (:obj:`set`): set of status codes
        '''
        def date_converter(d):
            if isinstance(d, datetime.datetime):
                return d.__str__()

        def gen_bulk_file(_iid, bulk_file):
            action_and_metadata = self.make_action_and_metadata(index, _iid)
            bulk_file += json.dumps(action_and_metadata) + '\n'
            bulk_file += json.dumps(doc, default=date_converter) + '\n'  
            return bulk_file

        def mod_cursor(cursor):
            pathlist = Path(cursor).expanduser().glob('**/*.json')
            for path in pathlist:
                with path.open() as f:
                    yield json.load(f)     

        if isinstance(cursor, str):
            count = len(list(Path(cursor).expanduser().glob('**/*.json'))) 
            cursor = mod_cursor(cursor)            
        elif isinstance(cursor, list):
            count = len(cursor) 
        else:
            cursor = cursor       

        url = self.es_endpoint + '/_bulk'
        status_code = {201}
        bulk_file = ''
        tot_rounds = math.ceil(count/bulk_size)        

        for i, doc in enumerate(cursor):
            if i == self.max_entries:
                break
            if self.verbose and i % bulk_size == 0:
                print("Processing bulk {} out of {} ...".format(math.floor(i/bulk_size)+1, tot_rounds))
               
            if i == count - 1:  # last entry
                bulk_file = gen_bulk_file(doc[_id], bulk_file)
                r = requests.post(url, auth=self.awsauth, data=bulk_file, headers=headers)
                status_code.add(r.status_code)
                return status_code
            elif i % bulk_size != 0 or i == 0: #  bulk_size*(n-1) + 1 --> bulk_size*n - 1
                bulk_file = gen_bulk_file(doc[_id], bulk_file)
            else:               # bulk_size * n
                r = requests.post(url, auth=self.awsauth, data=bulk_file, headers=headers)
                status_code.add(r.status_code)
                bulk_file = gen_bulk_file(doc[_id], '') # reset bulk_file
                
    def data_to_es_single(self, count, cursor, index, _id='uniprot_id',
                          headers={ "Content-Type": "application/json" }):
        ''' Load data into elasticsearch service

            Args:
                count (:obj:`int`): cursor size
                cursor (:obj:`pymongo.Cursor` or :obj:`iter`): documents to be PUT to es
                index (:obj:`str`): name of unique key to be used as index for es
                es_endpoint (:obj:`str`): elasticsearch endpoint
                headers (:obj:`dict`): http header information
                _id (:obj:`str`): key in mongo collection for identification
                
            Returns:
                (:obj:`set`): set of status codes
        '''
        url_root = self.es_endpoint + '/' + index + '/_doc/'
        status_code = {201}
        for i, doc in enumerate(cursor):
            if i == self.max_entries:
                break
            if i % 20 == 0 and self.verbose:
                print("Processing doc {} out of {}...".format(i, min(count, self.max_entries)))
            url = url_root + doc[_id]
            r = requests.post(url, auth=self.awsauth, json=doc, headers=headers)
            status_code.add(r.status_code)
        return status_code

# def main():
#     manager = EsUtil()
#     manager.delete_index('protein')

# if __name__ == "__main__":
#     main()