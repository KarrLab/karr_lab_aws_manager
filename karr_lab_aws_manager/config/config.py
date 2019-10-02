from pathlib import Path
import boto3
import os
from configparser import ConfigParser


class credentialsFile:
    
    def __init__(self, credential_path=None, config_path=None):
        ''' Establish environment variables' paths
        '''
        self.credential_path = credential_path
        self.config_path = config_path
        self.AWS_SHARED_CREDENTIALS_FILE = str(Path(self.credential_path).expanduser())
        self.AWS_CONFIG_FILE = str(Path(self.config_path).expanduser())


class establishSession(credentialsFile):
    
    def __init__(self, credential_path=None, config_path=None, profile_name=None):
        super().__init__(credential_path=credential_path, config_path=config_path)
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.AWS_SHARED_CREDENTIALS_FILE
        os.environ['AWS_CONFIG_FILE'] = self.AWS_CONFIG_FILE
        self.session = boto3.Session(profile_name=profile_name)
        self.access_key = self.session.get_credentials().access_key
        self.secret_key = self.session.get_credentials().secret_key


class establishES(establishSession):

    def __init__(self, credential_path=None, config_path=None, profile_name=None,
                elastic_path=None, service_name='es'):
        super().__init__(credential_path=credential_path, config_path=config_path,
                        profile_name=profile_name)
        self.client = self.session.client(service_name)
        self.es_config = str(Path(elastic_path).expanduser())
        config = ConfigParser()
        config.read(self.es_config)
        self.es_endpoint = config['elasticsearch-endpoint']['address']
        self.region = self.session.region_name


class establishS3(establishSession):

    def __init__(self, credential_path=None, config_path=None, profile_name=None, service_name='s3'):
        super().__init__(credential_path=credential_path, config_path=config_path, profile_name=profile_name)
        self.resource = self.session.resource(service_name)
        self.client = self.resource.meta.client
        self.region = self.session.region_name