import datanator_query_python
from karr_lab_aws_manager.config import config


class MongoToES:

    def __init__(self, profile_name='karrlab-zl', credential_path='.wc/third_party/aws_credentials',
                config_path='.wc/third_party/aws_config', elastic_path='.wc/third_party/elasticsearch.ini'):
        ''' 
            Args:
                profile_name (:obj: `str`): AWS profile to use for authentication
                credential_path (:obj: `str`): directory for aws credentials file
                config_path (:obj: `str`): directory for aws config file
                elastic_path (:obj: `str`): directory for file containing aws elasticsearch service variables
        '''
        session = config.establishES(config_path=config_path, profile_name=profile_name,
                                    elastic_path=elastic_path)
        self.client = session.client
        self.es_endpoint = session.es_endpoint
        
    def data_from_mongo(self):
        ''' Acquire documents from MongoDB

        '''
        pass

    def data_to_es(self):
        ''' Load data into elasticsearch service
        '''
        pass