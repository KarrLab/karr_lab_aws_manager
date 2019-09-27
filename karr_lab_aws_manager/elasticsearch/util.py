from datanator_query_python.config import config as config_mongo
from karr_lab_aws_manager.config import config
import requests
from requests_aws4auth import AWS4Auth


class EsUtil:

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

    def delete_index(self, index, _id=None):
        ''' Delete elasticsearch index
            Args:
                index (:obj: `str`): name of index in es
                _id (:obj: `int`): id of the doc in index (optional)
        '''
        if _id is None:
            url = self.es_endpoint + '/' + index
        else:
            url = self.es_endpoint + '/' + index + '/_doc/' + _id
        requests.delete(url, auth=self.awsauth)

def main():
    manager = EsUtil()
    manager.delete_index('protein')

if __name__ == "__main__":
    main()