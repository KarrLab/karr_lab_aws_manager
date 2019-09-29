from dotenv import load_dotenv
from pathlib import Path, PurePath
import boto3
import os
from configparser import ConfigParser

class credentialsFile:
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', config_path='.wc/third_party/aws_config'):
        ''' Establish environment variables' paths
        '''
        self.credential_path = credential_path
        self.config_path = config_path
        self.home_path = PurePath(Path.home(), self.credential_path)
        if os.path.exists(self.home_path):
            self.AWS_SHARED_CREDENTIALS_FILE = '~/' + self.credential_path
            self.AWS_CONFIG_FILE = '~/' + self.config_path
        else:
            self.AWS_SHARED_CREDENTIALS_FILE = '/' + self.credential_path
            self.AWS_CONFIG_FILE = '/' + self.config_path


class establishSession(credentialsFile):
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', 
                config_path='.wc/third_party/aws_config', profile_name='test'):
        super().__init__(credential_path=credential_path, config_path=config_path)
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.AWS_SHARED_CREDENTIALS_FILE
        os.environ['AWS_CONFIG_FILE'] = self.AWS_CONFIG_FILE
        self.session = boto3.Session(profile_name=profile_name)
        self.access_key = self.session.get_credentials().access_key
        self.secret_key = self.session.get_credentials().secret_key


class establishES(establishSession):

    def __init__(self, credential_path='.wc/third_party/aws_credentials', 
                config_path='.wc/third_party/aws_config', profile_name='test',
                elastic_path='.wc/third_party/elasticsearch.ini', service_name='es'):
        super().__init__(credential_path=credential_path, config_path=config_path,
                        profile_name=profile_name)
        self.client = self.session.client(service_name)
        if os.path.exists(self.home_path):
            self.es_config = '~/' + elastic_path
        else:
            self.es_config = '/' + elastic_path
        config = ConfigParser()
        config.read(os.path.expanduser(self.es_config))
        self.es_endpoint = config['elasticsearch-endpoint']['address']
        self.region = self.session.region_name


class establishS3(establishSession):

    def __init__(self, credential_path=None, config_path=None, profile_name=None, service_name='s3'):
        super().__init__(credential_path=credential_path, config_path=config_path, profile_name=profile_name)
        self.client = self.session.client(service_name)
        self.region = self.session.region_name
