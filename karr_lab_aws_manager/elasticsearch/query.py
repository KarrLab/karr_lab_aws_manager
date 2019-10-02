from elasticsearch import Elasticsearch, RequestsHttpConnection
from karr_lab_aws_manager.elasticsearch import util
import requests


class Query(util.EsUtil):

    def __init__(self, profile_name=None, credential_path=None,
                config_path=None, elastic_path=None,
                cache_dir=None, service_name='es', max_entries=float('inf'), verbose=False):
        ''' 
            Args:
                profile_name (:obj: `str`): AWS profile to use for authentication
                credential_path (:obj: `str`): directory for aws credentials file
                config_path (:obj: `str`): directory for aws config file
                elastic_path (:obj: `str`): directory for file containing aws elasticsearch service variables
                cache_dir (:obj: `str`): temp directory to store json for bulk upload
                service_name (:obj: `str`): aws service to be used
                max_entries (:obj: `int`): maximum number of operations
                verbose (:obj: `bool`): verbose messages
        '''
        super().__init__(profile_name=profile_name, credential_path=credential_path,
                config_path=config_path, elastic_path=elastic_path,
                cache_dir=cache_dir, service_name=service_name, max_entries=max_entries, verbose=verbose)
        self.es = Elasticsearch(
            hosts = [{'host': self.es_endpoint, 'port': 443}],
            http_auth = self.awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )

    def simple_query_string(self, string, **kwargs):
        ''' perform "simple_query_string" 
            (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html)
            Args:
                string (:obj: `str`): string or search pattern
            
        '''
        pass